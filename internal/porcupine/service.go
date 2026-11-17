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
	"io"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/auth"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
	"github.com/olup/porcupine/internal/usage"
	_ "modernc.org/sqlite"
)

type Config struct {
	DBPath         string
	ControlDBPath  string
	BootstrapToken string
	QueueMaxEvents int
	QueueMaxBytes  int64
	AcceptanceMode AcceptanceMode
	// ReceiptRetention bounds local retry receipts when DuckLake is configured.
	// Zero, the default, keeps every receipt.
	ReceiptRetention   time.Duration
	AllowedOrigins     []string
	TrustedProxies     []string
	Clock              func() time.Time
	DuckLake           *lakeapi.DuckLakeConfig
	Pipelines          *output.PipelineConfig
	Exports            *output.ExportConfig
	OAuth              *OAuthConfig
	Auth               *AuthConfig
	NativeBackup       *lakeapi.NativeBackupConfig
	InteractiveQueries *InteractiveQueryConfig
	Shared             *SharedConfig
	Usage              *usage.Config
	// Internal integration gate until hosted lifecycle, backup and acceptance are activated together.
	hostedImports bool
}

type Service struct {
	control                     *sql.DB
	localAdmission              contextMutex
	shared                      *sharedRuntime
	sharedIngestion             *sharedIngestionState
	lakeS3Storage               bool
	lakeS3RequestTimeoutSeconds int
	lakeMetadataTimeoutSeconds  int
	backups                     *nativeBackupRuntime
	activity                    projectActivity
	catalogMu                   sync.RWMutex
	deletionMu                  contextMutex
	deletionCancel              context.CancelFunc
	deletionDone                chan struct{}
	deletionWake                chan struct{}
	db                          *sql.DB
	*eventRules
	acceptance            liveAcceptor
	imports               *importManager
	allowedOrigins        map[string]bool
	trustedProxies        []netip.Prefix
	lake                  lakeapi.LakeStore
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
	sharedQueries         sharedQueryExecutions
	hostedQueries         *hostedQueryRuntime
	captureSlots          chan struct{}
	importSlots           chan struct{}
	tokenSlots            chan struct{}
	statusSlots           chan struct{}
	mutationMu            sync.Mutex
	hostedMutations       *hostedLiveFlusher
	pipelines             *output.PipelineJobs
	pipelineClient        *http.Client
	batchSlot             chan struct{}
	exports               *output.ExportJobs
	oauth                 *oauthRuntime
	resources             *resourceRuntime
	projectionGuard       *projectionGuard
	usage                 *usageRuntime
}

func Open(c Config) (*Service, error) {
	if err := ValidateConfig(c); err != nil {
		return nil, err
	}
	if err := validateSharedConfiguration(c); err != nil {
		return nil, err
	}
	interactiveConfig, err := normalizeInteractiveQueries(c.InteractiveQueries)
	if err != nil {
		return nil, err
	}
	if c.DuckLake != nil {
		cleanup := *c.DuckLake
		if err := lakeapi.NormalizeDuckLakeCleanup(&cleanup); err != nil {
			return nil, err
		}
	}
	if c.DuckLake != nil && c.DuckLake.MaintenanceInterval < 0 {
		return nil, errors.New("DuckLake maintenance interval must be nonnegative")
	}
	if c.DBPath == "" {
		c.DBPath = "./data/porcupine.db"
	}
	if c.Shared == nil {
		if c.ControlDBPath == "" {
			c.ControlDBPath = localControlPath(c.DBPath)
		}
		if err := validateLocalDatabasePaths(c.DBPath, c.ControlDBPath); err != nil {
			return nil, err
		}
		if err := checkNativeRestoreMarker(c.ControlDBPath); err != nil {
			return nil, err
		}
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
	if c.QueueMaxBytes < 0 {
		return nil, errors.New("queue byte limit must be nonnegative")
	}
	if c.QueueMaxBytes == 0 {
		c.QueueMaxBytes = DefaultQueueMaxBytes
	}
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
	if c.Exports != nil && c.DuckLake == nil {
		return nil, errors.New("exports require an analytical store")
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
	origins, err := parseAllowedOrigins(c.AllowedOrigins)
	if err != nil {
		return nil, err
	}
	db, err := openLocalSQLite(c.DBPath)
	if err != nil {
		return nil, err
	}
	var control *sql.DB
	if c.Shared == nil {
		control, err = openLocalSQLite(c.ControlDBPath)
	} else {
		control, err = openSharedControl(context.Background(), *c.Shared)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	if c.Shared == nil {
		if err := validateLocalDatabasePaths(c.DBPath, c.ControlDBPath); err != nil {
			control.Close()
			db.Close()
			return nil, err
		}
	}
	s := &Service{db: db, backups: backups, eventRules: &eventRules{queueMax: c.QueueMaxEvents, queueMaxBytes: c.QueueMaxBytes, clock: c.Clock, receiptRetention: c.ReceiptRetention}, allowedOrigins: origins, trustedProxies: proxies,
		batchSlot: make(chan struct{}, 1), oauth: oauth, resources: resources, interactiveConfig: interactiveConfig, querySlots: make(chan struct{}, interactiveConfig.MaxConcurrent)}
	s.control = control
	if c.Shared != nil {
		copyConfig := *c.Shared
		copyConfig.RoutingSecret = append([]byte(nil), copyConfig.RoutingSecret...)
		s.shared = &sharedRuntime{config: copyConfig}
	}
	s.eventRules.knownEvents = s.knownEventsForAcceptance
	durable := &durableAcceptance{db: db, rules: s.eventRules}
	s.acceptance = durable
	s.imports = &importManager{db: db, control: control, rules: s.eventRules, deliveryEnabled: c.DuckLake != nil}
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
		if s.exports != nil {
			s.exports.Close()
		}
		db.Close()
		if s.control != nil && s.control != db {
			s.closeSharedRouting()
			s.control.Close()
		}
		s.projectionGuard.close()
		return nil, err
	}
	if c.Shared == nil {
		err = initializeLocalControl(context.Background(), control, c.BootstrapToken, interactiveConfig, s.externalAuthEnabled())
	} else {
		err = initializeSharedControl(context.Background(), control, c.BootstrapToken, interactiveConfig, s.externalAuthEnabled())
	}
	if err != nil {
		return fail(err)
	}
	if err := migrateSQLite(context.Background(), db, journalSchema); err != nil {
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
	var lakeBound, sharedBound bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM metadata WHERE key='ducklake_source_id'),EXISTS(SELECT 1 FROM metadata WHERE key='shared_worker_id')`).Scan(&lakeBound, &sharedBound); err != nil {
		return txFail(err)
	}
	if sharedBound && s.shared == nil {
		return txFail(errors.New("this worker journal requires its shared control configuration"))
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
	if s.shared == nil {
		if err := s.bindLocalControl(context.Background()); err != nil {
			return fail(err)
		}
		s.eventRules.admission = &s.localAdmission
		s.eventRules.prepareAcceptance = s.prepareLocalAcceptance
	}
	if s.shared != nil {
		if err := s.bindSharedControl(context.Background()); err != nil {
			return fail(err)
		}
		if err := s.initializeSharedRouting(context.Background()); err != nil {
			return fail(err)
		}
		if err := s.initializeSharedIngestion(context.Background()); err != nil {
			return fail(err)
		}
		if c.DuckLake != nil {
			lakeConfig := *c.DuckLake
			if err := s.control.QueryRow(`SELECT value FROM metadata WHERE key='shared_source_id'`).Scan(&lakeConfig.SharedSourceID); err != nil {
				return fail(errors.New("shared analytical source identity unavailable"))
			}
			c.DuckLake = &lakeConfig
		}
	}
	if c.DuckLake != nil {
		s.lake, err = lakeapi.NewDuckLake(*c.DuckLake)
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
		if c.DuckLake.PostgresURL != "" {
			s.lakeMetadataTimeoutSeconds = c.DuckLake.MetadataTimeoutSeconds
			if s.lakeMetadataTimeoutSeconds == 0 {
				s.lakeMetadataTimeoutSeconds = 10
			}
		}
		if s.lakeMemoryMB == 0 {
			s.lakeMemoryMB = lakeapi.DefaultDuckLakeMemoryMB
		}
		s.deliveryMaxEvents = 1000
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
	if c.hostedImports {
		native, ok := s.lake.(*lakeapi.DuckLakeStore)
		if c.Shared == nil || !ok || s.usage == nil {
			return fail(errors.New("hosted imports require shared storage and usage"))
		}
		coordinator, err := lakeapi.NewPostgresProjectCoordinator(s.control, lakeapi.DefaultProjectCoordinatorConfig())
		if err != nil {
			return fail(err)
		}
		flusher, err := newHostedLiveFlusher(s.control, native, coordinator, s.usage.store)
		if err != nil {
			return fail(err)
		}
		s.imports = newHostedImportManager(s.eventRules, flusher)
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
	}
	if s.lake != nil {
		s.startLakeMaintenance(c.DuckLake.MaintenanceInterval)
	}
	s.exports.Start()
	if err := s.startNativeBackups(); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	s.startSharedIngestion()
	s.startUsage()
	return s, nil
}

func (s *Service) Close() error {
	if s.shared != nil {
		s.BeginQueryDrain()
		if s.sharedQueries.status().Active != 0 {
			return errors.New("shared analytics execution is still active; permit renewal continues until it returns")
		}
	}
	s.stopSharedIngestion()
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
	s.closeSharedRouting()
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
	if raw == "" || strings.HasPrefix(raw, serviceKeyPrefix) {
		return a, errors.New("invalid access key")
	}
	err := s.control.QueryRowContext(ctx, `SELECT id,permissions,projects,source_id FROM access_keys WHERE token_hash=$1 AND revoked_at IS NULL`, hash(raw)).Scan(&a.ID, &permissions, &projects, &a.SourceID)
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
	*r = *sharedReplayRequest(r, raw)
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
		if s.shared != nil && s.sharedQueries.status().unavailable() {
			problem(w, 503, "analytics execution unavailable")
			return
		}
		if err := s.db.PingContext(r.Context()); err != nil {
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
	capture := limitConcurrent(s.captureSlots, s.capture)
	m.HandleFunc("POST /capture/", capture)
	m.HandleFunc("POST /batch/", capture)
	m.HandleFunc("POST /i/v0/e/", capture)
	m.HandleFunc("POST /e/", capture)
	m.HandleFunc("POST /ingest/v1/events", limitConcurrent(s.captureSlots, s.captureNative))
	m.HandleFunc("GET /ingest/v1/schema", s.serveNativeInputSchema)
	for _, path := range []string{"/v1/track", "/v1/page", "/v1/identify", "/v1/batch"} {
		m.HandleFunc("POST "+path, limitConcurrent(s.captureSlots, s.captureSegment))
	}
	return s.sharedRoutingMiddleware(s.ingestionCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	Set        json.RawMessage `json:"$set,omitempty"`
	SetOnce    json.RawMessage `json:"$set_once,omitempty"`
	ingestion  *ingestfmt.Metadata
}
type ingestInput struct {
	eventInput
	Batch eventBatch `json:"batch"`
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
	var a auth.Principal
	var err error
	authenticatedAPI := false
	if len(r.Header.Values("Authorization")) != 0 {
		var access string
		access, err = auth.BearerCredential(r)
		if err == nil {
			resource := ""
			if s.resources != nil {
				resource = s.resources.apiResource
			}
			a, err = s.authenticateResource(r.Context(), access, resource)
			authenticatedAPI = err == nil && s.resources != nil && (a.ServiceKey || a.External != nil || strings.HasPrefix(access, "pc_at_"))
		}
		if err == nil && rawKey != "" {
			var selector auth.Principal
			selector, err = s.authenticate(r.Context(), rawKey)
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
		a, err = s.authenticate(r.Context(), rawKey)
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
		writeSharedRouteProblem(w, err)
		return
	}
	if s.forwardSharedProject(w, r, projectID) {
		return
	}
	ctx, finish, active := s.activity.begin(r.Context(), projectID)
	if !active {
		problem(w, 404, "project unavailable")
		return
	}
	defer finish()
	r = r.WithContext(ctx)
	if err := s.ensureProjectLocal(ctx, projectID, a); err != nil {
		writeSharedRouteProblem(w, err)
		return
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
		problem(w, 400, "batch must contain 1 to 1000 events")
		return
	}
	receiptTime, releaseReceiptTime := s.pipelines.BeginReceipt(projectID)
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
		var managed *managementError
		if errors.As(err, &managed) && managed.GetStatus() >= 400 && managed.GetStatus() < 500 {
			write(w, managed.GetStatus(), managed)
			return
		}
		var failure *acceptanceFailure
		if errors.As(err, &failure) && failure.retry {
			w.Header().Set("Retry-After", "1")
		}
		problem(w, 503, err.Error())
		return
	}
	s.pipelines.WakeWebhook()
	response, err := liveAcceptanceResponse(result, nativeResponse)
	if err != nil {
		problem(w, 503, err.Error())
		return
	}
	write(w, 200, response)
}
