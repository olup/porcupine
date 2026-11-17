package porcupine

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

type sharedRoutingState struct {
	backendPID int
	conn       *sql.Conn
	token      string
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	once       sync.Once
	transport  *http.Transport
}

const sharedRouteHeader = "X-Porcupine-Route"
const sharedRouteSignature = "X-Porcupine-Route-Signature"
const sharedRouteValidity = 30 * time.Second

// Import creation accepts manifests larger than ordinary event/API bodies.
// Forwarding must accommodate that envelope; each destination operation still
// enforces its own smaller body limit after the internal proof is validated.
const sharedRouteMaxBodyBytes = 4 << 20

var errSharedRouteBodyLimit = errors.New("forwarded body exceeds limit")

type sharedRouteClaim struct {
	Version     int    `json:"v"`
	Source      string `json:"source"`
	Target      string `json:"target"`
	Project     string `json:"project"`
	Epoch       int64  `json:"epoch"`
	IssuedAtMS  int64  `json:"issued_at_ms"`
	Nonce       string `json:"nonce"`
	Method      string `json:"method"`
	URI         string `json:"uri"`
	Host        string `json:"host"`
	Peer        string `json:"peer"`
	UserAgent   string `json:"user_agent"`
	ContentType string `json:"content_type"`
	Encoding    string `json:"encoding"`
	AuthHash    string `json:"auth_hash"`
	BodyHash    string `json:"body_hash"`
	MCP         bool   `json:"mcp"`
}

type sharedRouteContextKey struct{}
type sharedReplayContextKey struct{}
type sharedDeletedProjectContextKey struct{}

type sharedReplayBody struct {
	raw           []byte
	authorization string
}

// sharedReplayRequest retains the decoded bytes when an ingestion adapter has
// consumed its body before it knows which durable owner should receive it.
func sharedReplayRequest(r *http.Request, raw []byte) *http.Request {
	authorization := r.Header.Get("Authorization")
	if previous, ok := r.Context().Value(sharedReplayContextKey{}).(sharedReplayBody); ok {
		authorization = previous.authorization
	}
	replay := sharedReplayBody{raw: bytes.Clone(raw), authorization: authorization}
	return r.WithContext(context.WithValue(r.Context(), sharedReplayContextKey{}, replay))
}

func allowSharedDeletedProject(ctx context.Context) context.Context {
	return context.WithValue(ctx, sharedDeletedProjectContextKey{}, true)
}

func validateSharedRouteURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || u.Path != "" && u.Path != "/" || u.RawPath != "" {
		return nil, errors.New("worker route must be an exact HTTPS origin or a loopback HTTP origin")
	}
	if u.Scheme != "https" {
		host := u.Hostname()
		ip := net.ParseIP(host)
		if u.Scheme != "http" || host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, errors.New("worker route must use HTTPS outside loopback")
		}
	}
	u.Path = ""
	return u, nil
}

func (s *Service) initializeSharedRouting(ctx context.Context) error {
	if s.shared == nil {
		return nil
	}
	if len(s.shared.config.RoutingSecret) < 32 {
		return errors.New("shared routing secret must contain at least 32 bytes")
	}
	route, err := validateSharedRouteURL(s.shared.config.RouteURL)
	if err != nil {
		return err
	}
	worker := s.shared.config.WorkerID
	if len(worker) == 0 || len(worker) > 128 || strings.TrimSpace(worker) != worker {
		return errors.New("shared worker ID must contain 1 to 128 bytes without surrounding whitespace")
	}
	volume, err := s.sharedWorkerVolume(ctx, worker)
	if err != nil {
		return err
	}
	conn, err := s.control.Conn(ctx)
	if err != nil {
		return err
	}
	state := &sharedRoutingState{conn: conn, token: newID(), done: make(chan struct{})}
	state.ctx, state.cancel = context.WithCancel(context.Background())
	state.transport = &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 16, MaxIdleConnsPerHost: 4, IdleConnTimeout: 60 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 90 * time.Second}
	fail := func(err error) error {
		state.cancel()
		closeSharedRoutingConnection(state.conn)
		state.transport.CloseIdleConnections()
		return err
	}
	var locked bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended('porcupine-worker:' || $1,0))`, worker).Scan(&locked); err != nil {
		return fail(err)
	}
	if !locked {
		return fail(errors.New("shared worker ID is already active; use one process per durable worker volume"))
	}
	if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&state.backendPID); err != nil {
		return fail(errors.New("shared worker registration identity unavailable"))
	}
	var current string
	err = conn.QueryRowContext(ctx, `INSERT INTO shared_workers(worker_id,volume_id,route_url,registration_token)
VALUES($1,$2,$3,$4) ON CONFLICT(worker_id) DO UPDATE SET route_url=excluded.route_url,
registration_token=excluded.registration_token,last_seen_at=clock_timestamp()
WHERE shared_workers.volume_id=excluded.volume_id RETURNING volume_id`, worker, volume, route.String(), state.token).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return fail(errors.New("shared worker ID is bound to a different durable volume"))
	}
	if err != nil {
		return fail(errors.New("shared worker registration unavailable"))
	}
	s.shared.routing = state
	go s.sharedWorkerHeartbeat(state)
	return nil
}

func (s *Service) sharedWorkerVolume(ctx context.Context, worker string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('shared_worker_id',$1),('shared_worker_volume_id',$2) ON CONFLICT(key) DO NOTHING`, worker, newID()); err != nil {
		return "", err
	}
	var previous, volume string
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT value FROM metadata WHERE key='shared_worker_id'),(SELECT value FROM metadata WHERE key='shared_worker_volume_id')`).Scan(&previous, &volume); err != nil {
		return "", err
	}
	if previous != worker {
		return "", errors.New("this durable volume belongs to another shared worker ID")
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return volume, nil
}

func closeSharedRoutingConnection(conn *sql.Conn) {
	// Discard the physical session so an advisory lock cannot return to the pool.
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}

func (s *Service) sharedWorkerHeartbeat(state *sharedRoutingState) {
	defer close(state.done)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-state.ctx.Done():
			return
		case <-ticker.C:
		}
		ctx, stop := context.WithTimeout(state.ctx, 2*time.Second)
		result, err := state.conn.ExecContext(ctx, `UPDATE shared_workers SET last_seen_at=clock_timestamp() WHERE worker_id=$1 AND registration_token=$2`, s.shared.config.WorkerID, state.token)
		stop()
		if err != nil {
			state.cancel()
			return
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			state.cancel()
			return
		}
	}
}

func (s *Service) closeSharedRouting() {
	if s.shared == nil || s.shared.routing == nil {
		return
	}
	state := s.shared.routing
	state.once.Do(func() {
		state.cancel()
		<-state.done
		closeSharedRoutingConnection(state.conn)
		state.transport.CloseIdleConnections()
	})
}

func (s *Service) assignSharedProjectOwner(ctx context.Context, project string) error {
	if s.shared == nil {
		return nil
	}
	if s.shared.routing == nil || s.shared.routing.ctx.Err() != nil {
		return apiError(503, "worker registration unavailable")
	}
	result, err := s.control.ExecContext(ctx, `INSERT INTO shared_project_owners(project_id,worker_id)
SELECT id,$2 FROM projects WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)
ON CONFLICT(project_id) DO NOTHING`, project, s.shared.config.WorkerID)
	if err != nil {
		return apiError(503, "project owner registration unavailable")
	}
	if count, _ := result.RowsAffected(); count == 0 {
		_, _, err = s.sharedProjectOwner(ctx, project)
	}
	return err
}

func (s *Service) sharedProjectOwner(ctx context.Context, project string) (string, int64, error) {
	if s.shared == nil {
		return "", 0, nil
	}
	if s.shared.routing == nil || s.shared.routing.ctx.Err() != nil {
		return "", 0, apiError(503, "worker registration unavailable")
	}
	var worker string
	var epoch int64
	var deleting bool
	err := s.control.QueryRowContext(ctx, `SELECT o.worker_id,o.epoch,EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=o.project_id)
FROM shared_project_owners o JOIN projects p ON p.id=o.project_id WHERE o.project_id=$1`, project).Scan(&worker, &epoch, &deleting)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, apiError(404, "project owner unavailable")
	}
	if err != nil {
		return "", 0, apiError(503, "project owner lookup unavailable")
	}
	allowDeleted, _ := ctx.Value(sharedDeletedProjectContextKey{}).(bool)
	if deleting && !allowDeleted {
		return "", 0, apiError(404, "project unavailable")
	}
	if worker == s.shared.config.WorkerID {
		var current bool
		if err := s.control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM shared_workers WHERE worker_id=$1 AND registration_token=$2)`, worker, s.shared.routing.token).Scan(&current); err != nil || !current {
			return "", 0, apiError(503, "worker registration unavailable")
		}
	}
	return worker, epoch, nil
}

func (s *Service) requireSharedProjectOwner(ctx context.Context, project string) error {
	if s.shared == nil {
		return nil
	}
	// A deletion retry may reach the owner for cleanup, but it must never grant
	// background projection or destination execution access to that project.
	ctx = context.WithValue(ctx, sharedDeletedProjectContextKey{}, false)
	owner, _, err := s.sharedProjectOwner(ctx, project)
	if err != nil {
		return err
	}
	if owner != s.shared.config.WorkerID {
		return apiError(503, "project belongs to another durable worker")
	}
	return nil
}

func stripSharedRouteHeaders(header http.Header) {
	for name := range header {
		if strings.HasPrefix(strings.ToLower(name), "x-porcupine-route") {
			delete(header, name)
		}
	}
}

func sharedRouteDigest(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func sharedRouteMAC(secret []byte, raw []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(raw)
	return mac.Sum(nil)
}

func readSharedRouteBody(r *http.Request) ([]byte, bool, error) {
	if replay, ok := r.Context().Value(sharedReplayContextKey{}).(sharedReplayBody); ok {
		if len(replay.raw) > sharedRouteMaxBodyBytes {
			return nil, true, errSharedRouteBodyLimit
		}
		return replay.raw, true, nil
	}
	if r.Body == nil {
		return nil, false, nil
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, sharedRouteMaxBodyBytes+1))
	if err != nil {
		return nil, false, err
	}
	if len(raw) > sharedRouteMaxBodyBytes {
		return nil, false, errSharedRouteBodyLimit
	}
	return raw, false, nil
}

func (s *Service) forwardSharedProject(w http.ResponseWriter, r *http.Request, project string) bool {
	if s.shared == nil {
		return false
	}
	owner, epoch, err := s.sharedProjectOwner(r.Context(), project)
	if err != nil {
		writeSharedRouteProblem(w, err)
		return true
	}
	if incoming, ok := r.Context().Value(sharedRouteContextKey{}).(sharedRouteClaim); ok {
		if incoming.Target != s.shared.config.WorkerID || incoming.Project != project || incoming.Epoch != epoch || owner != s.shared.config.WorkerID {
			problem(w, http.StatusConflict, "forwarded project ownership changed; retry the original request")
			return true
		}
		return false
	}
	if owner == s.shared.config.WorkerID {
		return false
	}
	var origin string
	if err := s.control.QueryRowContext(r.Context(), `SELECT route_url FROM shared_workers WHERE worker_id=$1`, owner).Scan(&origin); err != nil {
		problem(w, http.StatusServiceUnavailable, "project owner unavailable; retry the same request")
		return true
	}
	target, err := validateSharedRouteURL(origin)
	if err != nil {
		problem(w, http.StatusServiceUnavailable, "project owner route unavailable")
		return true
	}
	body, decoded, err := readSharedRouteBody(r)
	if err != nil {
		problem(w, http.StatusRequestEntityTooLarge, "request body cannot be forwarded")
		return true
	}
	r = r.Clone(r.Context())
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	if decoded {
		r.Header.Del("Content-Encoding")
		query := r.URL.Query()
		query.Del("compression")
		r.URL.RawQuery = query.Encode()
		// Segment strips its public Basic write key before canonical acceptance.
		// Forward the original wire authentication so the owner can independently
		// validate it against the replayed envelope, including writeKey agreement.
		switch r.URL.Path {
		case "/v1/track", "/v1/page", "/v1/identify", "/v1/batch":
			replay, _ := r.Context().Value(sharedReplayContextKey{}).(sharedReplayBody)
			if r.Header.Get("Authorization") == "" && len(replay.authorization) > 6 && strings.EqualFold(replay.authorization[:6], "Basic ") {
				r.Header.Set("Authorization", replay.authorization)
			}
		}
	}
	r.Header.Del("Content-Length")
	stripSharedRouteHeaders(r.Header)
	// MCP's in-process API request has a relative URL and no Host. The HTTP
	// transport supplies the destination host in that case, so sign that exact
	// wire value rather than the synthetic empty host.
	if r.Host == "" {
		r.Host = target.Host
	}
	peer, err := s.clientIP(r)
	mcp, _ := r.Context().Value(mcpDispatchKey{}).(bool)
	if err != nil && !mcp {
		problem(w, http.StatusBadRequest, "invalid client address")
		return true
	}
	peerText := "0.0.0.0"
	if peer.IsValid() {
		peerText = peer.String()
	}
	claim := sharedRouteClaim{Version: 1, Source: s.shared.config.WorkerID, Target: owner, Project: project,
		Epoch: epoch, IssuedAtMS: time.Now().UnixMilli(), Nonce: newID(), Method: r.Method, URI: r.URL.RequestURI(),
		Host: r.Host, Peer: peerText, UserAgent: r.UserAgent(), ContentType: r.Header.Get("Content-Type"),
		Encoding: r.Header.Get("Content-Encoding"), AuthHash: sharedRouteDigest([]byte(r.Header.Get("Authorization"))),
		BodyHash: sharedRouteDigest(body), MCP: mcp}
	raw, err := json.Marshal(claim)
	if err != nil {
		problem(w, http.StatusBadRequest, "request metadata cannot be forwarded")
		return true
	}
	proof, signature := base64.RawURLEncoding.EncodeToString(raw), base64.RawURLEncoding.EncodeToString(sharedRouteMAC(s.shared.config.RoutingSecret, raw))
	proxy := &httputil.ReverseProxy{Transport: s.shared.routing.transport,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL.Scheme, request.Out.URL.Host = target.Scheme, target.Host
			request.Out.Host = claim.Host
			stripSharedRouteHeaders(request.Out.Header)
			request.Out.Header.Set(sharedRouteHeader, proof)
			request.Out.Header.Set(sharedRouteSignature, signature)
		},
		ErrorHandler: func(response http.ResponseWriter, _ *http.Request, _ error) {
			problem(response, http.StatusServiceUnavailable, "project owner unavailable; retry the same request")
		},
		ModifyResponse: func(response *http.Response) error {
			stripSharedRouteHeaders(response.Header)
			return nil
		}}
	proxy.ServeHTTP(w, r)
	return true
}

func writeSharedRouteProblem(w http.ResponseWriter, err error) {
	var managed *managementError
	if errors.As(err, &managed) {
		write(w, managed.GetStatus(), managed)
		return
	}
	problem(w, http.StatusServiceUnavailable, "project owner unavailable")
}

func (s *Service) sharedRoutingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.shared == nil {
			stripSharedRouteHeaders(r.Header)
			next.ServeHTTP(w, r)
			return
		}
		state := s.shared.routing
		if state == nil || state.ctx.Err() != nil {
			problem(w, http.StatusServiceUnavailable, "worker registration unavailable")
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		stop := context.AfterFunc(state.ctx, cancel)
		defer func() { stop(); cancel() }()
		r = r.WithContext(ctx)
		proofs, signatures := r.Header.Values(sharedRouteHeader), r.Header.Values(sharedRouteSignature)
		if len(proofs) == 0 && len(signatures) == 0 {
			stripSharedRouteHeaders(r.Header)
			next.ServeHTTP(w, r)
			return
		}
		if len(proofs) != 1 || len(signatures) != 1 || len(proofs[0]) > 8192 {
			problem(w, http.StatusUnauthorized, "invalid forwarded request")
			return
		}
		raw, err := base64.RawURLEncoding.DecodeString(proofs[0])
		signature, sigErr := base64.RawURLEncoding.DecodeString(signatures[0])
		if err != nil || sigErr != nil || !hmac.Equal(signature, sharedRouteMAC(s.shared.config.RoutingSecret, raw)) {
			problem(w, http.StatusUnauthorized, "invalid forwarded request")
			return
		}
		var claim sharedRouteClaim
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&claim) != nil || decoder.Decode(new(any)) != io.EOF || !s.validSharedRouteClaim(r, claim) {
			problem(w, http.StatusUnauthorized, "invalid or expired forwarded request")
			return
		}
		body, _, err := readSharedRouteBody(r)
		if errors.Is(err, errSharedRouteBodyLimit) {
			problem(w, http.StatusRequestEntityTooLarge, "forwarded request body exceeds limit")
			return
		}
		if err != nil || sharedRouteDigest(body) != claim.BodyHash {
			problem(w, http.StatusUnauthorized, "invalid forwarded request body")
			return
		}
		if err := s.consumeSharedRouteNonce(r.Context(), claim); err != nil {
			writeSharedRouteProblem(w, err)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		r.RemoteAddr = net.JoinHostPort(claim.Peer, "0")
		r.Header.Del("Forwarded")
		r.Header.Del("X-Forwarded-For")
		r.Header.Del("X-Real-IP")
		stripSharedRouteHeaders(r.Header)
		ctx = context.WithValue(r.Context(), sharedRouteContextKey{}, claim)
		if claim.MCP {
			ctx = context.WithValue(ctx, mcpDispatchKey{}, true)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Service) validSharedRouteClaim(r *http.Request, claim sharedRouteClaim) bool {
	now := time.Now().UnixMilli()
	if claim.Version != 1 || claim.Source == "" || claim.Source == claim.Target || claim.Target != s.shared.config.WorkerID || claim.Project == "" || claim.Epoch < 1 || claim.Nonce == "" || len(claim.Nonce) > 128 || claim.IssuedAtMS < now-sharedRouteValidity.Milliseconds() || claim.IssuedAtMS > now+5000 {
		return false
	}
	if net.ParseIP(claim.Peer) == nil {
		return false
	}
	return claim.Method == r.Method && claim.URI == r.URL.RequestURI() && claim.Host == r.Host &&
		claim.UserAgent == r.UserAgent() && claim.ContentType == r.Header.Get("Content-Type") && claim.Encoding == r.Header.Get("Content-Encoding") &&
		claim.AuthHash == sharedRouteDigest([]byte(r.Header.Get("Authorization")))
}

func (s *Service) consumeSharedRouteNonce(ctx context.Context, claim sharedRouteClaim) error {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return apiError(503, "forwarded request validation unavailable")
	}
	defer tx.Rollback()
	var valid bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM shared_project_owners WHERE project_id=$1 AND worker_id=$2 AND epoch=$3)
AND EXISTS(SELECT 1 FROM shared_workers WHERE worker_id=$4)
AND EXISTS(SELECT 1 FROM shared_workers WHERE worker_id=$2 AND registration_token=$5)`, claim.Project, claim.Target, claim.Epoch, claim.Source, s.shared.routing.token).Scan(&valid); err != nil {
		return apiError(503, "forwarded request validation unavailable")
	}
	if !valid {
		return apiError(409, "forwarded project ownership changed; retry the original request")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM shared_route_nonces WHERE expires_at<clock_timestamp()`); err != nil {
		return apiError(503, "forwarded request validation unavailable")
	}
	var nonce string
	err = tx.QueryRowContext(ctx, `INSERT INTO shared_route_nonces(nonce,expires_at) VALUES($1,clock_timestamp()+interval '35 seconds') ON CONFLICT(nonce) DO NOTHING RETURNING nonce`, claim.Nonce).Scan(&nonce)
	if errors.Is(err, sql.ErrNoRows) {
		return apiError(409, "forwarded request was already consumed; retry the original request")
	}
	if err != nil || tx.Commit() != nil {
		return apiError(503, "forwarded request validation unavailable")
	}
	return nil
}
