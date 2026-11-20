package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/observability"
	"github.com/olup/porcupine/internal/output"
	"github.com/olup/porcupine/internal/query"
	"github.com/olup/porcupine/internal/syncx"
	"github.com/olup/porcupine/internal/usage"
	_ "modernc.org/sqlite"
)

type Config struct {
	Profile        DeploymentProfile
	DBPath         string
	ControlDBPath  string
	BootstrapToken string
	QueueMaxEvents int
	QueueMaxBytes  int64
	AcceptanceMode AcceptanceMode
	// ReceiptRetention bounds local retry receipts when DuckLake is configured.
	// Zero, the default, keeps every receipt.
	ReceiptRetention    time.Duration
	AllowedOrigins      []string
	TrustedProxies      []string
	GeoIPDatabase       string
	Clock               func() time.Time
	DuckLake            *lakeapi.DuckLakeConfig
	Pipelines           *output.PipelineConfig
	Exports             *output.ExportConfig
	OAuth               *OAuthConfig
	Auth                *AuthConfig
	NativeBackup        *lakeapi.NativeBackupConfig
	InteractiveQueries  *query.InteractiveQueryConfig
	Shared              *SharedConfig
	Usage               *usage.Config
	HostedRecoveryFloor HostedRecoveryFloor
	MetricsEnabled      bool
}

type Service struct {
	metrics                     *operationalMetrics
	hosted                      *hostedRuntime
	control                     *sql.DB
	localAdmission              syncx.ContextMutex
	shared                      *sharedRuntime
	lakeS3Storage               bool
	lakeS3RequestTimeoutSeconds int
	lakeMetadataTimeoutSeconds  int
	backups                     *nativeBackupRuntime
	activity                    projectActivity
	catalogMu                   sync.RWMutex
	deletionMu                  syncx.ContextMutex
	deletionCancel              context.CancelFunc
	deletionDone                chan struct{}
	deletionWake                chan struct{}
	sealHostedProject           func(context.Context, string) error
	db                          *sql.DB
	*eventRules
	acceptance            liveAcceptor
	imports               *importManager
	allowedOrigins        map[string]bool
	trustedProxies        []netip.Prefix
	geoIPCountry          func(netip.Addr) string
	lake                  *lakeapi.DuckLakeStore
	receiptLookup         func(context.Context, string, []string) (map[string]int64, error)
	lakeMemoryMB          int
	lakeMaintenanceCancel context.CancelFunc
	lakeMaintenanceDone   chan struct{}
	deliveryCancel        context.CancelFunc
	deliveryDone          chan struct{}
	deliveryMaxEvents     int
	deliveryMaxBytes      int
	queries               *query.Runtime
	standaloneReader      query.Reader
	hostedReader          query.HostedReader
	captureSlots          chan struct{}
	importSlots           chan struct{}
	tokenSlots            chan struct{}
	statusSlots           chan struct{}
	mutationMu            sync.Mutex
	hostedMutations       *hostedLiveFlusher
	pipelines             *output.PipelineJobs
	pipelineClient        *http.Client
	exports               *output.ExportJobs
	oauth                 *oauthRuntime
	resources             *resourceRuntime
	projectionGuard       *projectionGuard
	usage                 *usageRuntime
}

func newServiceRuntime(c Config, interactiveConfig query.InteractiveQueryConfig) (*Service, error) {
	var err error
	var oauth *oauthRuntime
	var resources *resourceRuntime
	if c.Auth != nil && c.Auth.Mode == "external" {
		resources, err = normalizeExternalRuntime(*c.Auth)
		if err != nil {
			return nil, err
		}
	} else if c.OAuth != nil {
		var err error
		oauth, err = normalizeOAuthConfig(*c.OAuth)
		if err != nil {
			return nil, err
		}
		resources = newResourceRuntime(oauth.issuer)
	}
	proxies, err := parseTrustedProxies(c.TrustedProxies)
	if err != nil {
		return nil, err
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	origins, err := parseAllowedOrigins(c.AllowedOrigins)
	if err != nil {
		return nil, err
	}
	geoIPCountry, err := loadGeoIPCountry(c.GeoIPDatabase)
	if err != nil {
		slog.Warn("GeoIP enrichment disabled", "error", err)
	}
	s := &Service{eventRules: &eventRules{queueMax: c.QueueMaxEvents, queueMaxBytes: c.QueueMaxBytes, clock: c.Clock, receiptRetention: c.ReceiptRetention}, allowedOrigins: origins, trustedProxies: proxies,
		oauth: oauth, resources: resources, geoIPCountry: geoIPCountry}
	var registry *observability.Registry
	if c.MetricsEnabled {
		profile := c.Profile
		if profile == "" {
			profile = ProfileStandalone
		}
		registry = observability.NewRegistry(string(profile))
		s.metrics = &operationalMetrics{Registry: registry}
	}
	s.queries = query.New(interactiveConfig, func() time.Time { return s.clock() }, registry)
	s.captureSlots = make(chan struct{}, 32)
	s.deliveryMaxEvents = 100
	s.deliveryMaxBytes = 4 << 20
	s.importSlots = make(chan struct{}, 2)
	s.tokenSlots = make(chan struct{}, 8)
	s.statusSlots = make(chan struct{}, 1)
	return s, nil
}

func (s *Service) initializeDuckLake(config lakeapi.DuckLakeConfig) error {
	var err error
	s.lake, err = lakeapi.NewDuckLake(config)
	if err != nil {
		return err
	}
	s.lakeS3Storage = config.S3 != nil
	if s.lakeS3Storage {
		s.lakeS3RequestTimeoutSeconds = config.S3.RequestTimeoutSeconds
		if s.lakeS3RequestTimeoutSeconds == 0 {
			s.lakeS3RequestTimeoutSeconds = 5
		}
	}
	s.lakeMemoryMB = config.MemoryLimitMB
	if config.PostgresURL != "" {
		s.lakeMetadataTimeoutSeconds = config.MetadataTimeoutSeconds
		if s.lakeMetadataTimeoutSeconds == 0 {
			s.lakeMetadataTimeoutSeconds = 10
		}
	}
	if s.lakeMemoryMB == 0 {
		s.lakeMemoryMB = lakeapi.DefaultDuckLakeMemoryMB
	}
	s.deliveryMaxEvents = 1000
	return nil
}

func Open(c Config) (*Service, error) {
	return open(c, acceptance.DefaultBufferConfig())
}

func open(c Config, buffer acceptance.BufferConfig) (*Service, error) {
	if err := validateConfig(&c); err != nil {
		return nil, err
	}
	if c.Profile == ProfileHosted {
		return openHostedProfile(c, buffer)
	}
	interactiveConfig, err := query.Normalize(c.InteractiveQueries)
	if err != nil {
		return nil, err
	}
	if err := checkNativeRestoreMarker(c.ControlDBPath); err != nil {
		return nil, err
	}
	if err := checkNativeRestoreMarker(c.DBPath); err != nil {
		return nil, err
	}
	backups, err := prepareNativeBackup(c.NativeBackup, c.DBPath, c.ControlDBPath, c.DuckLake)
	if err != nil {
		return nil, err
	}
	if c.QueueMaxEvents <= 0 {
		c.QueueMaxEvents = 100000
	}
	if c.QueueMaxBytes == 0 {
		c.QueueMaxBytes = DefaultQueueMaxBytes
	}
	if c.DBPath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(c.DBPath), 0700); err != nil {
			return nil, err
		}
	}
	s, err := newServiceRuntime(c, interactiveConfig)
	if err != nil {
		return nil, err
	}
	db, err := openLocalSQLite(c.DBPath)
	if err != nil {
		return nil, err
	}
	control, err := openLocalSQLite(c.ControlDBPath)
	if err != nil {
		db.Close()
		return nil, err
	}
	if err := validateLocalDatabasePaths(c.DBPath, c.ControlDBPath); err != nil {
		control.Close()
		db.Close()
		return nil, err
	}
	s.db, s.control, s.backups = db, control, backups
	s.queries.Bind(control, "", 0)
	s.eventRules.knownEvents = s.knownEventsForAcceptance
	durable := &durableAcceptance{db: db, rules: s.eventRules}
	s.acceptance = durable
	s.imports = &importManager{db: db, control: control, rules: s.eventRules, deliveryEnabled: c.DuckLake != nil}
	fail := func(err error) (*Service, error) {
		if s.lake != nil {
			s.lake.Close()
		}
		if s.exports != nil {
			s.exports.Close()
		}
		db.Close()
		if s.control != nil && s.control != db {
			s.control.Close()
		}
		s.projectionGuard.close()
		return nil, err
	}
	if err := migrateSQLite(context.Background(), db, journalSchema); err != nil {
		return fail(err)
	}
	if err := initializeLocalControl(context.Background(), control, c.BootstrapToken, interactiveConfig, s.externalAuthEnabled()); err != nil {
		return fail(err)
	}
	s.projectionGuard, err = openProjectionGuard(c.DBPath)
	if err != nil {
		return fail(err)
	}
	if err := s.initializeTenants(context.Background()); err != nil {
		return fail(err)
	}

	// Rebuild the delivery index if a previous process stopped between writes.
	_, err = db.Exec(`INSERT OR IGNORE INTO delivery_ready(event_id) SELECT id FROM events WHERE delivered=0 AND NOT EXISTS(SELECT 1 FROM delivery_quarantine WHERE event_id=events.id)`)
	if err != nil {
		return fail(err)
	}
	if err := s.initializeQueueUsage(context.Background()); err != nil {
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
	var lakeBound bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM metadata WHERE key='ducklake_source_id')`).Scan(&lakeBound); err != nil {
		return txFail(err)
	}
	if lakeBound && c.DuckLake == nil {
		return txFail(errors.New("this SQLite database requires its configured DuckLake store"))
	}
	var initialized string
	err = tx.QueryRow(`SELECT value FROM metadata WHERE key='initialized'`).Scan(&initialized)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.Exec(`INSERT INTO metadata(key,value) VALUES('initialized','1'),('journal_layout','separate-control-v1')`)
	} else if err == nil {
		var layout string
		if e := tx.QueryRow(`SELECT value FROM metadata WHERE key='journal_layout'`).Scan(&layout); e != nil || layout != "separate-control-v1" {
			return txFail(errors.New("combined control/journal database is unsupported; start with a fresh data directory"))
		}
	}
	if err != nil {
		return txFail(err)
	}

	if err = tx.Commit(); err != nil {
		return txFail(err)
	}
	if err := s.bindLocalControl(context.Background()); err != nil {
		return fail(err)
	}
	s.eventRules.admission = &s.localAdmission
	s.eventRules.prepareAcceptance = s.prepareLocalAcceptance
	if c.DuckLake != nil {
		if err := s.initializeDuckLake(*c.DuckLake); err != nil {
			return fail(err)
		}
	}
	if err := s.initializeOAuthClients(context.Background()); err != nil {
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
	if err := s.initializeUsage(context.Background(), c.Usage); err != nil {
		return fail(err)
	}
	if err := s.configurePipelines(c.Pipelines); err != nil {
		return fail(err)
	}
	if err := s.pipelines.Initialize(context.Background()); err != nil {
		return fail(err)
	}
	if err := s.configureExports(c.Exports); err != nil {
		return fail(err)
	}
	s.startDeletionRecovery()
	s.pipelines.Start()
	if s.lake != nil {
		s.startDelivery()
		s.startLakeMaintenance(c.DuckLake.MaintenanceInterval, s.lake.Maintain)
	}
	s.exports.Start()
	if err := s.startNativeBackups(); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	s.startUsage()
	s.startQueueSampling()
	return s, nil
}

func (s *Service) Close() error {
	if s.hosted != nil {
		return s.closeHosted()
	}
	if m := s.metrics; m != nil && m.stopSampling != nil {
		m.stopSampling()
		<-m.samplingDone
	}
	s.stopNativeBackups()
	if s.lakeMaintenanceCancel != nil {
		s.lakeMaintenanceCancel()
		<-s.lakeMaintenanceDone
	}
	s.deletionCancel()
	<-s.deletionDone
	s.pipelines.Close()
	if s.exports != nil {
		s.exports.Close()
	}
	if s.deliveryCancel != nil {
		s.deliveryCancel()
		<-s.deliveryDone
	}
	s.stopUsage()
	var lakeErr error
	if s.lake != nil {
		lakeErr = s.lake.Close()
	}
	var controlErr error
	if s.control != s.db {
		controlErr = s.control.Close()
	}
	return errors.Join(lakeErr, controlErr, s.db.Close(), s.projectionGuard.close())
}
func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func token() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "pc_" + hex.EncodeToString(b)
}
func hash(v string) string { b := sha256.Sum256([]byte(v)); return hex.EncodeToString(b[:]) }

func (s *Service) authenticate(ctx context.Context, raw string) (auth.Principal, error) {
	var a auth.Principal
	var permissions, projects string
	var tenantCredential bool
	if raw == "" || strings.HasPrefix(raw, serviceKeyPrefix) {
		return a, errors.New("invalid access key")
	}
	err := s.control.QueryRowContext(ctx, `SELECT k.id,k.permissions,k.projects,k.source_id,t.principal_id IS NOT NULL
FROM access_keys k LEFT JOIN tenant_credentials t ON t.principal_id=k.id
WHERE k.token_hash=$1 AND k.kind='access' AND k.revoked_at IS NULL`, hash(raw)).Scan(&a.ID, &permissions, &projects, &a.SourceID, &tenantCredential)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return a, err
		}
		return a, auth.ErrUnavailable
	}
	if err = json.Unmarshal([]byte(permissions), &a.Permissions); err != nil {
		return a, auth.ErrUnavailable
	}
	err = json.Unmarshal([]byte(projects), &a.Projects)
	if err != nil {
		return a, auth.ErrUnavailable
	}
	if tenantCredential && (len(a.Projects) != 1 || a.Projects[0] == "" || a.Projects[0] == "*") {
		return auth.Principal{}, errors.New("tenant credential requires one explicit project")
	}
	return a, nil
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

func readBody(w http.ResponseWriter, r *http.Request, allowUnmarkedGzip bool) ([]byte, bool) {
	encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	compression := r.URL.Query().Get("compression")
	beacon := compression == "base64"
	supportedEncoding := encoding == "" || encoding == "identity" || encoding == "gzip" && !beacon
	if !supportedEncoding || compression != "" && compression != "gzip-js" && !beacon {
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
	invalidBody, oversizedBody := "invalid gzip body", "decompressed body exceeds 1 MiB"
	// posthog-js sends /e/ gzip bytes as text/plain without an encoding marker.
	browserGzip := allowUnmarkedGzip && encoding == "" &&
		strings.EqualFold(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]), "text/plain") &&
		bytes.HasPrefix(raw, []byte{0x1f, 0x8b})
	if beacon {
		media, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaErr != nil || media != "application/x-www-form-urlencoded" {
			problem(w, 415, "base64 bodies require application/x-www-form-urlencoded")
			return nil, false
		}
		form, formErr := url.ParseQuery(string(raw))
		data := form.Get("data")
		if formErr != nil || data == "" {
			problem(w, 400, "invalid base64 form: expected a nonempty data field")
			return nil, false
		}
		for _, values := range form {
			if len(values) > 1 {
				problem(w, 400, "duplicate base64 form field")
				return nil, false
			}
		}
		codec := base64.RawStdEncoding
		if strings.HasSuffix(data, "=") {
			codec = base64.StdEncoding
		}
		raw, err = codec.DecodeString(data)
		invalidBody, oversizedBody = "invalid base64 body", "decoded body exceeds 1 MiB"
	} else if encoding == "gzip" || compression == "gzip-js" || browserGzip {
		reader, gzipErr := gzip.NewReader(bytes.NewReader(raw))
		if gzipErr != nil {
			problem(w, 400, invalidBody)
			return nil, false
		}
		defer reader.Close()
		raw, err = io.ReadAll(io.LimitReader(reader, maxBodyBytes+1))
	}
	if len(raw) > maxBodyBytes {
		problem(w, 413, oversizedBody)
		return nil, false
	}
	if err != nil {
		problem(w, 400, invalidBody)
		return nil, false
	}
	return raw, true
}

func (s *Service) registerCaptureRoutes(m *http.ServeMux) map[string][]string {
	captureHeaders := make(map[string][]string)
	register := func(path string, headers []string, handler http.HandlerFunc) {
		admit := limitConcurrent(s.captureSlots, handler)
		m.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
			// Exact paths pass the origin check before routing. ServeMux also matches legacy subpaths.
			if r.URL.Path != path && !s.allowCaptureOrigin(w, r) {
				return
			}
			admit(w, r)
		})
		captureHeaders[path] = headers
	}
	postHogHeaders := []string{"Content-Type", "Content-Encoding"}
	producerHeaders := []string{"Content-Type", "Content-Encoding", "Authorization", "X-Porcupine-Key"}

	registerPostHog := func(path string, body postHogBody) {
		register(path, postHogHeaders, func(w http.ResponseWriter, r *http.Request) {
			decoding := body
			if r.URL.Path != path {
				decoding = postHogBody{}
			}
			s.capturePostHog(w, r, decoding)
		})
	}
	registerPostHog("/capture/", postHogBody{})
	registerPostHog("/batch/", postHogBody{requireBatch: true})
	registerPostHog("/i/v0/e/", postHogBody{})
	registerPostHog("/e/", postHogBody{allowUnmarkedGzip: true})

	register("/ingest/v1/events", producerHeaders, s.captureNative)
	registerSegment := func(path string, limit int) {
		register(path, producerHeaders, func(w http.ResponseWriter, r *http.Request) {
			s.captureSegment(w, r, path, limit)
		})
	}
	registerSegment("/v1/track", ingestfmt.SegmentEventLimit)
	registerSegment("/v1/page", ingestfmt.SegmentEventLimit)
	registerSegment("/v1/identify", ingestfmt.SegmentEventLimit)
	registerSegment("/v1/batch", ingestfmt.SegmentBatchLimit)
	return captureHeaders
}

func (s *Service) allowCaptureOrigin(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")
	switch {
	case origin == "":
	case len(s.allowedOrigins) == 0:
		w.Header().Set("Access-Control-Allow-Origin", "*")
	case s.allowedOrigins[origin]:
		w.Header().Set("Access-Control-Allow-Origin", origin)
	default:
		problem(w, 403, "origin is not allowed")
		return false
	}
	return true
}

func (s *Service) ingestionCORS(captureHeaders map[string][]string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowedHeaders, capture := captureHeaders[r.URL.Path]
		if !capture {
			next.ServeHTTP(w, r)
			return
		}
		if !s.allowCaptureOrigin(w, r) {
			return
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
				if header != "" && !slices.ContainsFunc(allowedHeaders, func(allowed string) bool {
					return strings.ToLower(allowed) == header
				}) {
					problem(w, 403, "preflight header is not allowed")
					return
				}
			}
			w.Header().Set("Access-Control-Allow-Methods", "POST")
			w.Header().Set("Access-Control-Allow-Headers", strings.Join(allowedHeaders, ", "))
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
		if s.shared != nil && s.queries.Status().Unavailable() {
			problem(w, 503, "analytics execution unavailable")
			return
		}
		if s.hosted != nil {
			if _, err := s.hosted.incarnation.current(); err != nil || !s.hosted.buffer.Available() {
				problem(w, 503, "hosted ingestion unavailable")
				return
			}
		}
		if s.db != nil && s.db.PingContext(r.Context()) != nil {
			problem(w, 503, "storage unavailable")
			return
		}
		{
			ctx, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			if err := s.control.PingContext(ctx); err != nil {
				problem(w, 503, "control storage unavailable")
				return
			}
		}
		write(w, 200, map[string]string{"status": "ready"})
	})
	s.registerOAuth(m)
	s.registerResourceMetadata(m)
	api := s.registerManagement(m)
	m.Handle("/mcp", s.mcpHandler(api.OpenAPI(), m))
	captureHeaders := s.registerCaptureRoutes(m)
	m.HandleFunc("GET /ingest/v1/schema", s.serveNativeInputSchema)
	return s.observeHTTP(captureHeaders, s.ingestionCORS(captureHeaders, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/oauth/") {
			s.catalogMu.RLock()
			defer s.catalogMu.RUnlock()
		}
		m.ServeHTTP(w, r)
	})))
}

type eventInput struct {
	APIKey     string          `json:"api_key"`
	Token      string          `json:"token"`
	UUID       string          `json:"uuid"`
	Event      string          `json:"event"`
	DistinctID string          `json:"distinct_id"`
	Properties map[string]any  `json:"properties"`
	Timestamp  string          `json:"timestamp"`
	SentAt     json.RawMessage `json:"sent_at"`
	Offset     json.RawMessage `json:"offset"`
	Set        json.RawMessage `json:"$set,omitempty"`
	SetOnce    json.RawMessage `json:"$set_once,omitempty"`
	ingestion  *ingestfmt.Metadata
	sentAt     clientSentAt
}
type ingestInput struct {
	eventInput
	Batch eventBatch `json:"batch"`
}

type postHogBody struct {
	requireBatch      bool
	allowUnmarkedGzip bool
}

func (s *Service) capturePostHog(w http.ResponseWriter, r *http.Request, body postHogBody) {
	raw, ok := readBody(w, r, body.allowUnmarkedGzip)
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
			if body.requireBatch {
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
	query := r.URL.Query()
	epoch := query.Get("_")
	if _, err := strconv.ParseInt(epoch, 10, 64); err != nil {
		epoch = ""
	}
	rootSentAt, querySentAt := sentAtString(input.SentAt), query.Get("sent_at")
	for i := range min(len(batch), maxCaptureEvents) {
		batch[i].sentAt = parseClientSentAt(sentAtString(batch[i].SentAt), rootSentAt, querySentAt, epoch)
	}
	s.captureNormalized(w, r, input.eventInput, batch)
}

// captureNormalized accepts only server-translated event inputs. Its private
// ingestion metadata cannot be populated by the public JSON decoder.
func (s *Service) captureNormalized(w http.ResponseWriter, r *http.Request, root eventInput, batch eventBatch) {
	nativeResponse := len(batch) > 0 && batch[0].ingestion != nil
	for _, e := range batch {
		if (e.ingestion != nil) != nativeResponse {
			ingestionProblem(w, "invalid_event", "mixed ingestion protocols")
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
				ingestionProblem(w, "invalid_credential", "token must be a string")
				return
			}
		}
		propertyKey, _ := propertyToken.(string)
		for _, candidate := range []string{e.APIKey, e.Token, propertyKey} {
			if candidate != "" {
				if rawKey != "" && rawKey != candidate {
					ingestionProblem(w, "invalid_credential", "batch credentials must match")
					return
				}
				rawKey = candidate
			}
		}
	}
	if s.hosted != nil {
		ctx, cancel := s.hosted.incarnation.writeContext(r.Context())
		defer cancel()
		if ctx.Err() != nil {
			w.Header().Set("Retry-After", "1")
			problem(w, 503, "hosted ingestion unavailable")
			return
		}
		r = r.WithContext(ctx)
	}
	var a auth.Principal
	var err error
	authenticatedAPI := false
	if len(r.Header.Values("Authorization")) != 0 {
		var access string
		access, err = auth.BearerCredential(r)
		if err == nil && strings.HasPrefix(access, clientIngestionKeyPrefix) {
			a, err = s.authenticateIngestionKey(r.Context(), access)
		} else if err == nil {
			resource := ""
			if s.resources != nil {
				resource = s.resources.apiResource
			}
			a, err = s.authenticateResource(r.Context(), access, resource)
			authenticatedAPI = err == nil && s.resources != nil && (a.ServiceKey || a.External != nil || strings.HasPrefix(access, "pc_at_"))
		}
		if err == nil && rawKey != "" {
			var selector auth.Principal
			selector, err = s.authenticateIngestionKey(r.Context(), rawKey)
			if err == nil {
				if selector.Can("events:write") && len(selector.Projects) == 1 && selector.Projects[0] != "*" && auth.Authorize(a, "events:write", selector.Projects[0]) && (a.SourceID == "" || selector.SourceID == "" || a.SourceID == selector.SourceID) {
					a.Projects = []string{selector.Projects[0]}
					if selector.SourceID != "" {
						a.SourceID = selector.SourceID
					}
				} else {
					err = errors.New("project key outside token scope")
				}
			}
		}
	} else {
		a, err = s.authenticateIngestionKey(r.Context(), rawKey)
	}
	if err != nil {
		if writeAuthenticationUnavailable(w, err) {
			return
		}
		problem(w, 401, "invalid ingestion credential")
		return
	}
	if !a.Can("events:write") || len(a.Projects) != 1 || a.Projects[0] == "*" {
		problem(w, 403, "ingestion requires a key scoped to one project with events:write")
		return
	}
	projectID := a.Projects[0]
	if err := s.requireProjectActive(r.Context(), projectID); err != nil {
		writeManagementProblem(w, err)
		return
	}
	ctx, finish, active := s.activity.begin(r.Context(), projectID)
	if !active {
		problem(w, 404, "project unavailable")
		return
	}
	defer finish()
	r = r.WithContext(ctx)
	if s.hosted == nil {
		if err := s.ensureProjectLocal(ctx, projectID, a); err != nil {
			writeManagementProblem(w, err)
			return
		}
	}
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
		ingestionProblem(w, "invalid_event", "batch must contain 1 to 1000 events")
		return
	}
	receiptTime, releaseReceiptTime := s.pipelines.BeginReceipt(projectID)
	defer releaseReceiptTime()
	prepared, err := prepareLiveBatch(projectID, a.SourceID, batch, receiptTime, s.metrics)
	if err != nil {
		ingestionProblem(w, "invalid_event", err.Error())
		return
	}
	peer := acceptance.Peer{UserAgent: r.UserAgent()}
	if prepared.Cookieless {
		peer.Address, err = s.clientIP(r)
		if err != nil {
			ingestionProblem(w, "invalid_event", "invalid cookieless client address")
			return
		}
	}
	if s.hosted != nil {
		for _, input := range prepared.Events {
			if err := lakeapi.ValidateLiveProjection(input.Event); err != nil {
				markIngestionRejection(w, "invalid_event")
				write(w, 400, map[string]string{"error": err.Error(), "code": "invalid_live_projection"})
				return
			}
		}
		tenant, err := s.projectTenant(ctx, projectID)
		if err != nil {
			s.observeAcceptance(ctx, nil, err)
			writeManagementProblem(w, err)
			return
		}
		if err := s.hosted.flusher.policy.admit(ctx, tenant.ID); err != nil {
			s.observeAcceptance(ctx, nil, err)
			if !writeTenantIngestionError(w, r, err, nativeResponse) {
				w.Header().Set("Retry-After", "1")
				problem(w, 503, "tenant ingestion policy unavailable")
			}
			return
		}
	}
	if s.geoIPCountry != nil && a.ClientIngestionKey {
		s.enrichLiveGeoIP(r, prepared, batch, peer.Address)
	}
	result, err := s.acceptance.Accept(ctx, prepared, peer, nativeResponse)
	s.observeAcceptance(ctx, result, err)
	if err != nil {
		if writeTenantIngestionError(w, r, err, nativeResponse) {
			return
		}
		var managed *apierr.Error
		if errors.As(err, &managed) && managed.GetStatus() >= 400 && managed.GetStatus() < 500 {
			write(w, managed.GetStatus(), managed)
			return
		}
		var failure *acceptance.Failure
		if errors.As(err, &failure) && failure.Retry {
			w.Header().Set("Retry-After", "1")
		}
		if errors.Is(err, acceptance.ErrProjectQuarantined) {
			write(w, 503, map[string]string{"error": err.Error(), "code": "project_ingestion_quarantined"})
			return
		}
		problem(w, 503, err.Error())
		return
	}
	if s.hosted == nil {
		s.pipelines.WakeWebhook()
	}
	response, err := liveAcceptanceResponse(result, nativeResponse)
	if err != nil {
		problem(w, 503, err.Error())
		return
	}
	write(w, 200, response)
}
