package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
	_ "modernc.org/sqlite"
)

type Config struct {
	DBPath             string
	BootstrapToken     string
	QueueMaxEvents     int
	QueueMaxBytes      int64
	AllowedOrigins     []string
	TrustedProxies     []string
	Clock              func() time.Time
	DuckLake           *DuckLakeConfig
	Pipelines          *PipelineConfig
	Exports            *ExportConfig
	OAuth              *OAuthConfig
	NativeBackup       *NativeBackupConfig
	InteractiveQueries *InteractiveQueryConfig
}

type Service struct {
	lakeS3Storage               bool
	lakeS3RequestTimeoutSeconds int
	backups                     *nativeBackupRuntime
	activity                    projectActivity
	catalogMu                   sync.RWMutex
	deletionMu                  contextMutex
	deletionCancel              context.CancelFunc
	deletionDone                chan struct{}
	db                          *sql.DB
	*eventRules
	acceptance            *eventAcceptance
	imports               *importManager
	allowedOrigins        map[string]bool
	trustedProxies        []netip.Prefix
	lake                  lakeStore
	lakeMemoryMB          int
	lakeMaintenanceCancel context.CancelFunc
	lakeMaintenanceDone   chan struct{}
	deliveryCancel        context.CancelFunc
	deliveryDone          chan struct{}
	deliveryMaxEvents     int
	deliveryMaxBytes      int
	querySlots            chan struct{}
	interactiveConfig     InteractiveQueryConfig
	tenantQueries         tenantQueryAdmission
	captureSlots          chan struct{}
	importSlots           chan struct{}
	tokenSlots            chan struct{}
	statusSlots           chan struct{}
	mutationMu            sync.Mutex
	pipelines             *pipelineRuntime
	batchSlot             chan struct{}
	exports               *exportRuntime
	oauth                 *oauthRuntime
	projectionGuard       *projectionGuard
}

func Open(c Config) (*Service, error) {
	interactiveConfig, err := normalizeInteractiveQueries(c.InteractiveQueries)
	if err != nil {
		return nil, err
	}
	if c.DuckLake != nil {
		cleanup := *c.DuckLake
		if err := normalizeDuckLakeCleanup(&cleanup); err != nil {
			return nil, err
		}
	}
	if c.DuckLake != nil && c.DuckLake.MaintenanceInterval < 0 {
		return nil, errors.New("DuckLake maintenance interval must be nonnegative")
	}
	if c.DBPath == "" {
		c.DBPath = "./data/porcupine.db"
	}
	if err := checkNativeRestoreMarker(c.DBPath); err != nil {
		return nil, err
	}
	backups, err := prepareNativeBackup(c.NativeBackup, c.DBPath, c.DuckLake)
	if err != nil {
		return nil, err
	}
	if c.QueueMaxEvents <= 0 {
		c.QueueMaxEvents = 100000
	}
	if c.QueueMaxBytes < 0 {
		return nil, errors.New("queue byte limit must be nonnegative")
	}
	if c.QueueMaxBytes == 0 {
		c.QueueMaxBytes = DefaultQueueMaxBytes
	}
	var oauth *oauthRuntime
	if c.OAuth != nil {
		var err error
		oauth, err = normalizeOAuthConfig(*c.OAuth)
		if err != nil {
			return nil, err
		}
	}
	pipelines, err := newPipelineRuntime(c.Pipelines)
	if err != nil {
		return nil, err
	}
	var exports *exportRuntime
	if c.Exports != nil {
		if c.DuckLake == nil {
			return nil, errors.New("exports require an analytical store")
		}
		ec, err := normalizeExportConfig(*c.Exports)
		if err != nil {
			return nil, err
		}
		store, err := exportstore.New(context.Background(), ec.S3)
		if err != nil {
			return nil, errors.New("invalid S3 export configuration")
		}
		exports = &exportRuntime{config: ec, store: store, wake: make(chan struct{}, 1)}
	}
	if c.DBPath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(c.DBPath), 0700); err != nil {
			return nil, err
		}
	}
	proxies, err := parseTrustedProxies(c.TrustedProxies)
	if err != nil {
		return nil, err
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	origins := map[string]bool{}
	for _, origin := range c.AllowedOrigins {
		origin = strings.TrimSpace(origin)
		u, err := url.Parse(origin)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || strings.Contains(origin, "*") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("allowed origins must be exact http(s) origins without paths or wildcards")
		}
		origins[origin] = true
	}
	// Apply connection-local settings whenever database/sql opens a replacement connection.
	dsn := c.DBPath
	if dsn != ":memory:" {
		absolute, err := filepath.Abs(dsn)
		if err != nil {
			return nil, err
		}
		dsn = (&url.URL{Scheme: "file", Path: absolute}).String()
	}
	pragmas := url.Values{"_pragma": {"journal_mode(WAL)", "synchronous(FULL)", "foreign_keys(ON)", "busy_timeout(5000)"}}
	db, err := sql.Open("sqlite", dsn+"?"+pragmas.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Service{db: db, backups: backups, eventRules: &eventRules{queueMax: c.QueueMaxEvents, queueMaxBytes: c.QueueMaxBytes, clock: c.Clock}, allowedOrigins: origins, trustedProxies: proxies,
		pipelines: pipelines, batchSlot: make(chan struct{}, 1), exports: exports, oauth: oauth, interactiveConfig: interactiveConfig, querySlots: make(chan struct{}, interactiveConfig.MaxConcurrent)}
	s.acceptance = &eventAcceptance{db: db, rules: s.eventRules}
	if pipelines != nil {
		webhooks := &webhookAcceptance{config: &pipelines.config, rules: s.eventRules}
		s.acceptance.enqueueWebhooks = webhooks.Enqueue
	}
	s.imports = &importManager{db: db, rules: s.eventRules, deliveryEnabled: c.DuckLake != nil}
	s.captureSlots = make(chan struct{}, 32)
	s.deliveryMaxEvents = 100
	s.deliveryMaxBytes = 4 << 20
	s.importSlots = make(chan struct{}, 2)
	s.tokenSlots = make(chan struct{}, 8)
	s.statusSlots = make(chan struct{}, 1)
	fail := func(err error) (*Service, error) {
		if s.lake != nil {
			s.lake.Close()
		}
		if exports != nil {
			exports.store.Close()
		}
		db.Close()
		s.projectionGuard.close()
		return nil, err
	}
	s.projectionGuard, err = openProjectionGuard(c.DBPath)
	if err != nil {
		return fail(err)
	}
	if err := s.initializeTenants(context.Background()); err != nil {
		return fail(err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id), name TEXT NOT NULL, created_at TEXT NOT NULL, ingestion_mode TEXT NOT NULL DEFAULT 'mixed' CHECK(ingestion_mode IN ('mixed','server_only')));
CREATE INDEX IF NOT EXISTS projects_tenant ON projects(tenant_id,id);
CREATE TABLE IF NOT EXISTS project_deletions (project_id TEXT PRIMARY KEY, completed INTEGER NOT NULL CHECK(completed IN (0,1)));
CREATE TABLE IF NOT EXISTS access_keys (id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT UNIQUE NOT NULL, permissions TEXT NOT NULL, projects TEXT NOT NULL, revoked_at TEXT, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL, event TEXT NOT NULL, distinct_id TEXT NOT NULL, properties TEXT NOT NULL, timestamp TEXT NOT NULL, received_at TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, UNIQUE(project_id,uuid));
CREATE INDEX IF NOT EXISTS events_project_id ON events(project_id,id);
CREATE INDEX IF NOT EXISTS events_pending ON events(delivered,id);
CREATE TABLE IF NOT EXISTS delivery_ready (event_id INTEGER PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS delivery_quarantine (
event_id INTEGER PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
error_code INTEGER NOT NULL CHECK(error_code IN (321,469)),quarantined_at TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS delivery_ready_insert AFTER INSERT ON events WHEN NEW.delivered=0 BEGIN
INSERT OR IGNORE INTO delivery_ready(event_id) VALUES(NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS delivery_ready_update AFTER UPDATE OF delivered ON events BEGIN
DELETE FROM delivery_ready WHERE event_id=NEW.id;
INSERT OR IGNORE INTO delivery_ready(event_id) SELECT NEW.id WHERE NEW.delivered=0 AND NOT EXISTS(SELECT 1 FROM delivery_quarantine WHERE event_id=NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS delivery_quarantine_insert AFTER INSERT ON delivery_quarantine BEGIN
DELETE FROM delivery_ready WHERE event_id=NEW.event_id;
END;
CREATE TRIGGER IF NOT EXISTS delivery_quarantine_retry AFTER DELETE ON delivery_quarantine BEGIN
INSERT OR IGNORE INTO delivery_ready(event_id) SELECT id FROM events WHERE id=OLD.event_id AND delivered=0;
END;
INSERT OR IGNORE INTO delivery_ready(event_id) SELECT id FROM events WHERE delivered=0 AND NOT EXISTS(SELECT 1 FROM delivery_quarantine WHERE event_id=events.id);
CREATE TABLE IF NOT EXISTS cookieless_keys (day TEXT PRIMARY KEY, secret BLOB NOT NULL CHECK(length(secret)=32));
CREATE TABLE IF NOT EXISTS event_receipts (project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL, PRIMARY KEY(project_id,uuid)) WITHOUT ROWID;`)
	if err != nil {
		return fail(err)
	}
	if err := s.initializeProjectIngestion(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeMutations(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeIngestionMetadata(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeIdentity(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeImports(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeQueueUsage(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeReceipts(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeExports(context.Background()); err != nil {
		return fail(err)
	}
	tx, err := db.Begin()
	if err != nil {
		return fail(err)
	}
	defer tx.Rollback()
	txFail := func(err error) (*Service, error) {
		tx.Rollback()
		return fail(err)
	}
	var lakeBound, foreignBackendBound bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM metadata WHERE key='ducklake_source_id'),EXISTS(SELECT 1 FROM metadata WHERE key IN ('query_master_key','projection_table_uuid'))`).Scan(&lakeBound, &foreignBackendBound); err != nil {
		return txFail(err)
	}
	if lakeBound && c.DuckLake == nil {
		return txFail(errors.New("this SQLite database requires its configured DuckLake store"))
	}
	if foreignBackendBound {
		return txFail(errors.New("this acceptance database belongs to an unsupported analytical backend; automatic migration is not supported"))
	}
	var initialized string
	err = tx.QueryRow(`SELECT value FROM metadata WHERE key='initialized'`).Scan(&initialized)
	if errors.Is(err, sql.ErrNoRows) {
		if len(c.BootstrapToken) < 16 {
			return txFail(errors.New("PORCUPINE_BOOTSTRAP_TOKEN must contain at least 16 characters on first initialization"))
		}
		_, err = tx.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,?,?,?,?,?)`, newID(), "bootstrap admin", hash(c.BootstrapToken), `["instance:admin"]`, `["*"]`, now())
		if err == nil {
			_, err = tx.Exec(`INSERT INTO metadata(key,value) VALUES('initialized','1')`)
		}
	}
	if err != nil {
		return txFail(err)
	}

	if err = tx.Commit(); err != nil {
		return txFail(err)
	}
	if c.DuckLake != nil {
		s.lake, err = newDuckLake(*c.DuckLake)
		if err != nil {
			return fail(err)
		}
		s.lakeS3Storage = c.DuckLake.S3 != nil
		if s.lakeS3Storage {
			s.lakeS3RequestTimeoutSeconds = c.DuckLake.S3.RequestTimeoutSeconds
			if s.lakeS3RequestTimeoutSeconds == 0 {
				s.lakeS3RequestTimeoutSeconds = 5
			}
		}
		s.lakeMemoryMB = c.DuckLake.MemoryLimitMB
		if s.lakeMemoryMB == 0 {
			s.lakeMemoryMB = defaultDuckLakeMemoryMB
		}
		s.deliveryMaxEvents = 1000
	}
	if err := s.initializeOAuthClients(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeServiceKeys(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeOAuth(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeBackupGuard(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializeLakeSource(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.initializePipelines(context.Background()); err != nil {
		return fail(err)
	}
	s.startDeletionRecovery()
	s.startPipelines()
	if s.lake != nil {
		s.startDelivery()
	}
	if s.lake != nil {
		s.startLakeMaintenance(c.DuckLake.MaintenanceInterval)
	}
	if exports != nil {
		s.startExports()
	}
	if err := s.startNativeBackups(); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

func (s *Service) Close() error {
	s.stopNativeBackups()
	if s.lakeMaintenanceCancel != nil {
		s.lakeMaintenanceCancel()
		<-s.lakeMaintenanceDone
	}
	s.deletionCancel()
	<-s.deletionDone
	s.stopPipelines()
	if s.exports != nil {
		s.stopExports()
		if s.exports.store != nil {
			s.exports.store.Close()
		}
	}
	if s.deliveryCancel != nil {
		s.deliveryCancel()
		<-s.deliveryDone
	}
	var lakeErr error
	if s.lake != nil {
		lakeErr = s.lake.Close()
	}
	return errors.Join(lakeErr, s.db.Close(), s.projectionGuard.close())
}
func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func token() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "pc_" + hex.EncodeToString(b)
}
func hash(v string) string { b := sha256.Sum256([]byte(v)); return hex.EncodeToString(b[:]) }

func (s *Service) authenticate(ctx context.Context, raw string) (actor, error) {
	var a actor
	var permissions, projects string
	if raw == "" || strings.HasPrefix(raw, serviceKeyPrefix) {
		return a, errors.New("invalid access key")
	}
	err := s.db.QueryRowContext(ctx, `SELECT id,permissions,projects,source_id FROM access_keys WHERE token_hash=? AND revoked_at IS NULL`, hash(raw)).Scan(&a.ID, &permissions, &projects, &a.SourceID)
	if err != nil {
		return a, err
	}
	if err = json.Unmarshal([]byte(permissions), &a.Permissions); err != nil {
		return a, err
	}
	err = json.Unmarshal([]byte(projects), &a.Projects)
	return a, err
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, message string) {
	write(w, status, map[string]any{"error": message})
}

const maxBodyBytes = 1 << 20

func readBody(w http.ResponseWriter, r *http.Request, ingestion bool) ([]byte, bool) {
	encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	compression := r.URL.Query().Get("compression")
	supportedEncoding := encoding == "" || encoding == "identity" || ingestion && encoding == "gzip"
	if !supportedEncoding || ingestion && compression != "" && compression != "gzip-js" {
		problem(w, 415, "unsupported body encoding")
		return nil, false
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if len(raw) > maxBodyBytes {
		problem(w, 413, "body exceeds 1 MiB")
		return nil, false
	}
	if err != nil {
		problem(w, 400, "invalid body")
		return nil, false
	}
	// posthog-js sends /e/ gzip bytes as text/plain without an encoding marker.
	browserGzip := ingestion && r.URL.Path == "/e/" && encoding == "" &&
		strings.EqualFold(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]), "text/plain") &&
		bytes.HasPrefix(raw, []byte{0x1f, 0x8b})
	if ingestion && (encoding == "gzip" || compression == "gzip-js" || browserGzip) {
		reader, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			problem(w, 400, "invalid gzip body")
			return nil, false
		}
		defer reader.Close()
		raw, err = io.ReadAll(io.LimitReader(reader, maxBodyBytes+1))
		if len(raw) > maxBodyBytes {
			problem(w, 413, "decompressed body exceeds 1 MiB")
			return nil, false
		}
		if err != nil {
			problem(w, 400, "invalid gzip body")
			return nil, false
		}
	}
	return raw, true
}

func (s *Service) ingestionCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nativeRoute := r.URL.Path == "/ingest/v1/events" || r.URL.Path == "/v1/track" || r.URL.Path == "/v1/page" || r.URL.Path == "/v1/identify" || r.URL.Path == "/v1/batch"
		if !nativeRoute && r.URL.Path != "/capture/" && r.URL.Path != "/batch/" && r.URL.Path != "/i/v0/e/" && r.URL.Path != "/e/" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Add("Vary", "Origin")
		origin := r.Header.Get("Origin")
		if origin != "" {
			if !s.allowedOrigins[origin] {
				problem(w, 403, "origin is not allowed")
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		if r.Method == http.MethodOptions {
			w.Header().Add("Vary", "Access-Control-Request-Method")
			w.Header().Add("Vary", "Access-Control-Request-Headers")
			if method := r.Header.Get("Access-Control-Request-Method"); method != "" && method != "POST" {
				problem(w, 403, "preflight method is not allowed")
				return
			}
			for _, header := range strings.Split(r.Header.Get("Access-Control-Request-Headers"), ",") {
				header = strings.ToLower(strings.TrimSpace(header))
				if header != "" && header != "content-type" && header != "content-encoding" && !(nativeRoute && (header == "authorization" || header == "x-porcupine-key")) {
					problem(w, 403, "preflight header is not allowed")
					return
				}
			}
			w.Header().Set("Access-Control-Allow-Methods", "POST")
			allowedHeaders := "Content-Type, Content-Encoding"
			if nativeRoute {
				allowedHeaders += ", Authorization, X-Porcupine-Key"
			}
			w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Service) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	m.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.db.PingContext(r.Context()); err != nil {
			problem(w, 503, "storage unavailable")
			return
		}
		write(w, 200, map[string]string{"status": "ready"})
	})
	s.registerOAuth(m)
	api := s.registerManagement(m)
	m.Handle("/mcp", s.mcpHandler(api.OpenAPI(), m))
	capture := limitConcurrent(s.captureSlots, s.capture)
	m.HandleFunc("POST /capture/", capture)
	m.HandleFunc("POST /batch/", capture)
	m.HandleFunc("POST /i/v0/e/", capture)
	m.HandleFunc("POST /e/", capture)
	m.HandleFunc("POST /ingest/v1/events", limitConcurrent(s.captureSlots, s.captureNative))
	m.HandleFunc("GET /ingest/v1/schema", serveNativeInputSchema)
	for _, path := range []string{"/v1/track", "/v1/page", "/v1/identify", "/v1/batch"} {
		m.HandleFunc("POST "+path, limitConcurrent(s.captureSlots, s.captureSegment))
	}
	return s.ingestionCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/oauth/") {
			s.catalogMu.RLock()
			defer s.catalogMu.RUnlock()
		}
		m.ServeHTTP(w, r)
	}))
}

type eventInput struct {
	APIKey     string          `json:"api_key"`
	Token      string          `json:"token"`
	UUID       string          `json:"uuid"`
	Event      string          `json:"event"`
	DistinctID string          `json:"distinct_id"`
	Properties map[string]any  `json:"properties"`
	Timestamp  string          `json:"timestamp"`
	Set        json.RawMessage `json:"$set,omitempty"`
	SetOnce    json.RawMessage `json:"$set_once,omitempty"`
	ingestion  *ingestionMetadata
}
type ingestInput struct {
	eventInput
	Batch eventBatch `json:"batch"`
}
type Event struct {
	ID               int64           `json:"id"`
	ProjectID        string          `json:"project_id"`
	UUID             string          `json:"uuid"`
	Event            string          `json:"event"`
	DistinctID       string          `json:"distinct_id"`
	Properties       json.RawMessage `json:"properties"`
	Timestamp        string          `json:"timestamp"`
	ReceivedAt       string          `json:"received_at"`
	Delivered        bool            `json:"delivered"`
	Imported         bool            `json:"imported,omitempty"`
	IdentitySnapshot string          `json:"-"`
	SourceID         string          `json:"source_id,omitempty"`
	MessageID        string          `json:"message_id,omitempty"`
	EventType        string          `json:"event_type,omitempty"`
	Context          json.RawMessage `json:"context,omitempty"`
}

func (s *Service) capture(w http.ResponseWriter, r *http.Request) {
	raw, ok := readBody(w, r, true)
	if !ok {
		return
	}
	var input ingestInput
	var batch eventBatch
	raw = bytes.TrimSpace(raw)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if len(raw) > 0 && raw[0] == '[' {
		if err := decoder.Decode(&batch); err != nil {
			problem(w, 400, "invalid event array")
			return
		}
	} else {
		if err := decoder.Decode(&input); err != nil {
			problem(w, 400, "invalid event JSON")
			return
		}
		batch = input.Batch
		if batch == nil {
			if r.URL.Path == "/batch/" {
				problem(w, 400, "batch is required")
				return
			}
			batch = []eventInput{input.eventInput}
		}
	}
	if decoder.Decode(new(json.RawMessage)) != io.EOF {
		problem(w, 400, "unexpected data after event JSON")
		return
	}
	s.captureNormalized(w, r, input.eventInput, batch)
}

// captureNormalized accepts only server-translated event inputs. Its private
// ingestion metadata cannot be populated by the public JSON decoder.
func (s *Service) captureNormalized(w http.ResponseWriter, r *http.Request, root eventInput, batch eventBatch) {
	nativeResponse := len(batch) > 0 && batch[0].ingestion != nil
	for _, e := range batch {
		if (e.ingestion != nil) != nativeResponse {
			problem(w, 400, "mixed ingestion protocols")
			return
		}
	}
	// Every supplied credential must agree, including event-level SDK tokens.
	rawKey := ""
	credentials := append([]eventInput{root}, batch...)
	for _, e := range credentials {
		propertyToken, exists := e.Properties["token"]
		if e.ingestion != nil {
			propertyToken, exists = nil, false
		}
		if exists {
			if _, ok := propertyToken.(string); !ok {
				problem(w, 400, "token must be a string")
				return
			}
		}
		propertyKey, _ := propertyToken.(string)
		for _, candidate := range []string{e.APIKey, e.Token, propertyKey} {
			if candidate != "" {
				if rawKey != "" && rawKey != candidate {
					problem(w, 400, "batch credentials must match")
					return
				}
				rawKey = candidate
			}
		}
	}
	var a actor
	var err error
	authenticatedAPI := false
	if len(r.Header.Values("Authorization")) != 0 {
		var access string
		access, err = bearerCredential(r)
		if err == nil {
			resource := ""
			if s.oauth != nil {
				resource = s.oauth.apiResource
			}
			a, err = s.authenticateResource(r.Context(), access, resource)
			authenticatedAPI = err == nil && s.oauth != nil && (a.serviceKey || strings.HasPrefix(access, "pc_at_"))
		}
		if err == nil && rawKey != "" {
			var selector actor
			selector, err = s.authenticate(r.Context(), rawKey)
			if err == nil && selector.can("events:write") && len(selector.Projects) == 1 && selector.Projects[0] != "*" && authorize(a, "events:write", selector.Projects[0]) && (a.SourceID == "" || selector.SourceID == "" || a.SourceID == selector.SourceID) {
				a.Projects = []string{selector.Projects[0]}
				if selector.SourceID != "" {
					a.SourceID = selector.SourceID
				}
			} else {
				err = errors.New("project key outside token scope")
			}
		}
	} else {
		a, err = s.authenticate(r.Context(), rawKey)
	}
	if err != nil {
		problem(w, 401, "invalid ingestion credential")
		return
	}
	if !a.can("events:write") || len(a.Projects) != 1 || a.Projects[0] == "*" {
		problem(w, 403, "ingestion requires a key scoped to one project with events:write")
		return
	}
	projectID := a.Projects[0]
	ctx, finish, active := s.activity.begin(r.Context(), projectID)
	if !active {
		problem(w, 404, "project unavailable")
		return
	}
	defer finish()
	r = r.WithContext(ctx)
	mode, policyErr := s.projectIngestionMode(r.Context(), projectID)
	if policyErr != nil {
		problem(w, 503, "project ingestion policy unavailable")
		return
	}
	if mode == ingestionServerOnly && !authenticatedAPI {
		problem(w, 401, "server_only ingestion requires an API access token or service key")
		return
	}
	if len(batch) == 0 || len(batch) > maxCaptureEvents {
		problem(w, 400, "batch must contain 1 to 1000 events")
		return
	}
	receiptTime, releaseReceiptTime := s.beginCaptureReceipt(projectID)
	defer releaseReceiptTime()
	prepared, err := prepareLiveBatch(projectID, a.SourceID, batch, receiptTime)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	peer := ingestionPeer{userAgent: r.UserAgent()}
	if prepared.cookieless {
		peer.address, err = s.clientIP(r)
		if err != nil {
			problem(w, 400, "invalid cookieless client address")
			return
		}
	}
	result, err := s.acceptance.Accept(r.Context(), prepared, peer, nativeResponse)
	if err != nil {
		if writeTenantIngestionError(w, r, err, nativeResponse) {
			return
		}
		var failure *acceptanceFailure
		if errors.As(err, &failure) && failure.retry {
			w.Header().Set("Retry-After", "1")
		}
		problem(w, 503, err.Error())
		return
	}
	s.wakeWebhook()
	response := map[string]any{"status": 1, "accepted": result.accepted, "duplicates": result.duplicates, "identity_ignored": result.identityIgnored}
	if nativeResponse {
		response["receipts"] = result.receipts
	}
	write(w, 200, response)
}
