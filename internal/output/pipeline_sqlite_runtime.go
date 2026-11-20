package output

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/exportstore"
	"github.com/olup/porcupine/internal/usage"
)

// PipelineConfig contains operator-owned settings. A nil config disables destinations.
type PipelineConfig struct {
	Key                           []byte
	AllowedS3Endpoints            []string
	ExportLimits                  ExportConfig
	WebhookMaxQueuedEvents        int
	WebhookMaxQueuedBytes         int64
	WebhookProjectMaxQueuedEvents int
	WebhookProjectMaxQueuedBytes  int64
	WebhookMaxAttempts            int
	WebhookTimeoutSeconds         int
}

type PipelineJobs struct {
	pg                *hostedPipelineRuntime
	db, control       *sql.DB
	clock             func() time.Time
	lake              PipelineReader
	usage             *usage.Recorder
	loadActor         func(context.Context, string) (auth.Principal, error)
	credentialMissing func(error) bool
	requireActive     func(context.Context, string) error
	beginActivity     func(context.Context, string) (context.Context, func(), bool)
	acquire           func(context.Context) (func(), bool)

	activeS3Mu     sync.Mutex
	activeS3ID     string
	activeS3Cancel context.CancelFunc
	s3StepMu       sync.Mutex
	retiredMu      sync.Mutex
	s3Lifecycle    sync.Map // per-pipeline exclusion between resume and remote deletion cleanup
	admissionsMu   sync.Mutex
	admissions     map[*pipelineAdmission]struct{}

	config      PipelineConfig
	aead        cipher.AEAD
	fingerprint string
	s3Origins   map[string]bool
	webhooks    *sqliteWebhookQueue
	webhookWake chan struct{}
	s3Wake      chan struct{}
	cancel      context.CancelFunc
	workers     sync.WaitGroup
}

func newPipelineRuntime(input *PipelineConfig) (*PipelineJobs, error) {
	if input == nil {
		return nil, nil
	}
	c := *input
	c.AllowedS3Endpoints = append([]string(nil), c.AllowedS3Endpoints...)
	if len(c.Key) != 32 {
		return nil, errors.New("pipeline encryption key must be 32 bytes")
	}
	block, err := aes.NewCipher(c.Key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256Hex(c.Key)
	c.Key = nil
	c.ExportLimits, err = NormalizeExportConfig(c.ExportLimits)
	if err != nil {
		return nil, err
	}
	if c.WebhookMaxQueuedEvents == 0 {
		c.WebhookMaxQueuedEvents = 10000
	}
	if c.WebhookMaxQueuedBytes == 0 {
		c.WebhookMaxQueuedBytes = 64 << 20
	}
	if c.WebhookProjectMaxQueuedEvents == 0 {
		c.WebhookProjectMaxQueuedEvents = 1000
	}
	if c.WebhookProjectMaxQueuedBytes == 0 {
		c.WebhookProjectMaxQueuedBytes = 16 << 20
	}
	if c.WebhookMaxAttempts == 0 {
		c.WebhookMaxAttempts = 8
	}
	if c.WebhookTimeoutSeconds == 0 {
		c.WebhookTimeoutSeconds = 10
	}
	if c.WebhookMaxQueuedEvents < 1 || c.WebhookMaxQueuedBytes < 1 || c.WebhookProjectMaxQueuedEvents < 1 || c.WebhookProjectMaxQueuedBytes < 1 || c.WebhookMaxAttempts < 1 || c.WebhookMaxAttempts > 100 || c.WebhookTimeoutSeconds < 1 || c.WebhookTimeoutSeconds > 120 {
		return nil, errors.New("invalid webhook delivery limits")
	}
	origins := map[string]bool{}
	for _, raw := range c.AllowedS3Endpoints {
		origin, err := pipelineS3Origin(strings.TrimSpace(raw))
		if err != nil {
			return nil, err
		}
		origins[origin] = true
	}
	return &PipelineJobs{admissions: make(map[*pipelineAdmission]struct{}), config: c, aead: aead, fingerprint: fingerprint, s3Origins: origins, webhookWake: make(chan struct{}, 1), s3Wake: make(chan struct{}, 1)}, nil
}

func pipelineS3Origin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || !exportstore.ValidEndpoint(raw) || strings.ContainsAny(raw, "?#*") || (u.EscapedPath() != "" && u.EscapedPath() != "/") {
		return "", errors.New("S3 endpoint must be an exact HTTP(S) origin")
	}
	return u.Scheme + "://" + strings.ToLower(u.Host), nil
}

func (s *PipelineJobs) ValidateS3Endpoint(raw string) error {
	if raw == "" {
		return nil
	}
	origin, err := pipelineS3Origin(raw)
	if err != nil || !s.Enabled() || !s.s3Origins[origin] {
		return errors.New("S3 endpoint is not allowed by this deployment")
	}
	return nil
}

func pipelineAAD(projectID, pipelineID string) []byte {
	data, _ := json.Marshal([3]string{"porcupine/pipeline/v1", projectID, pipelineID})
	return data
}

func (s *PipelineJobs) SealSecret(projectID, pipelineID string, plaintext []byte) ([]byte, error) {
	if !s.Enabled() {
		return nil, errors.New("pipelines disabled")
	}
	aead := s.aead
	blob := make([]byte, 1+aead.NonceSize())
	blob[0] = 1
	if _, err := rand.Read(blob[1:]); err != nil {
		return nil, err
	}
	return aead.Seal(blob, blob[1:], plaintext, pipelineAAD(projectID, pipelineID)), nil
}

func (s *PipelineJobs) OpenSecret(projectID, pipelineID string, blob []byte) ([]byte, error) {
	if !s.Enabled() {
		return nil, errors.New("pipelines disabled")
	}
	aead := s.aead
	start := 1 + aead.NonceSize()
	if len(blob) < start+aead.Overhead() || blob[0] != 1 {
		return nil, errors.New("invalid encrypted destination")
	}
	return aead.Open(nil, blob[1:start], blob[start:], pipelineAAD(projectID, pipelineID))
}

func (s *PipelineJobs) Initialize(ctx context.Context) error {
	if s.pg != nil {
		return s.initializeHostedPipelines(ctx)
	}
	if err := s.verifyPipelineConfig(ctx); err != nil {
		return err
	}
	if err := initializeWebhooks(ctx, s.db); err != nil {
		return err
	}
	return s.initializeS3Pipelines(ctx)
}

func (s *PipelineJobs) verifyPipelineConfig(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists, activeS3 bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pipelines UNION ALL SELECT 1 FROM pipeline_retired_destinations),EXISTS(SELECT 1 FROM pipelines WHERE kind='s3' AND state='active')").Scan(&exists, &activeS3); err != nil {
		return err
	}
	var previous string
	err = tx.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key='pipeline_key_fingerprint'").Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if !s.Enabled() {
		if exists {
			return errors.New("existing pipelines require their original encryption key")
		}
		return nil
	}
	if activeS3 && s.lake == nil {
		return errors.New("active S3 pipelines require an analytical store")
	}
	if exists && previous != s.fingerprint {
		return errors.New("pipeline encryption key does not match existing destinations")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO metadata(key,value) VALUES('pipeline_key_fingerprint',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", s.fingerprint); err != nil {
		return err
	}
	return tx.Commit()
}

func signalPipelineWorker(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (s *PipelineJobs) WakeWebhook() {
	if s.Enabled() {
		signalPipelineWorker(s.webhookWake)
	}
}

func (s *PipelineJobs) Wake() {
	if !s.Enabled() {
		return
	}
	s.WakeWebhook()
	signalPipelineWorker(s.s3Wake)
}

const pipelinePollInterval = time.Second

func (s *PipelineJobs) Start() {
	rt := s
	if !s.Enabled() {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt.cancel = cancel
	start := func(destination string, wake chan struct{}, poll func(context.Context) (time.Duration, error)) {
		rt.workers.Add(1)
		go func() {
			defer rt.workers.Done()
			var lastWarning time.Time
			for ctx.Err() == nil {
				delay, err := poll(ctx)
				if err != nil && ctx.Err() == nil && time.Since(lastWarning) >= time.Minute {
					slog.Warn("pipeline worker could not advance; retrying", "destination_type", destination)
					lastWarning = time.Now()
				}
				if delay == 0 {
					continue
				}
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-wake:
					timer.Stop()
				case <-timer.C:
				}
			}
		}()
	}
	startFixed := func(destination string, wake chan struct{}, step func(context.Context) (bool, error)) {
		start(destination, wake, func(ctx context.Context) (time.Duration, error) {
			return pipelineStepDelay(step(ctx))
		})
	}
	if s.pg != nil {
		if s.pg.step != nil {
			start("webhook", rt.webhookWake, s.hostedWebhookPoll())
		}
		if s.pg.scheduled.ready() {
			startFixed("s3", rt.s3Wake, s.RunS3)
			startFixed("retired-s3", make(chan struct{}, 1), s.cleanHostedScheduledRetirement)
		}
		return
	}
	startFixed("webhook", rt.webhookWake, s.RunWebhook)
	startFixed("retired-s3", make(chan struct{}, 1), func(ctx context.Context) (bool, error) { return s.cleanupRetired(ctx, "", "") })
	if s.lake != nil {
		startFixed("s3", rt.s3Wake, s.RunS3)
	}
}

func pipelineStepDelay(progress bool, err error) (time.Duration, error) {
	if progress && err == nil {
		return 0, nil
	}
	return pipelinePollInterval, err
}

func (s *PipelineJobs) Stop() {
	if s != nil && s.cancel != nil {
		s.cancel()
		s.workers.Wait()
	}
}

// Receipt timestamps are assigned before validation. A closed-window snapshot
// waits for older arrivals still in flight, including captures blocked on SQLite.
// The capture admission limit bounds this map; no database lock is held here.
type pipelineAdmission struct {
	projectID string
	arrivedMS int64
}

func (s *PipelineJobs) BeginReceipt(projectID string) (time.Time, func()) {
	rt := s
	if !s.Enabled() || s.pg != nil || s.lake == nil {
		return s.clock().UTC(), func() {}
	}
	rt.admissionsMu.Lock()
	arrived := s.clock().UTC()
	a := &pipelineAdmission{projectID: projectID, arrivedMS: arrived.UnixMilli()}
	rt.admissions[a] = struct{}{}
	rt.admissionsMu.Unlock()
	return arrived, func() {
		rt.admissionsMu.Lock()
		delete(rt.admissions, a)
		rt.admissionsMu.Unlock()
	}
}

func (s *PipelineJobs) WindowHasAdmissions(projectID string, startMS, endMS int64) bool {
	rt := s
	rt.admissionsMu.Lock()
	defer rt.admissionsMu.Unlock()
	for a := range rt.admissions {
		if a.projectID == projectID && a.arrivedMS >= startMS && a.arrivedMS < endMS {
			return true
		}
	}
	return false
}

// Both operations touch the same destination's work. A resume must not restart
// its worker while deletion is checking the remote commit marker.
func (s *PipelineJobs) lockS3Lifecycle(id string) (func(), error) {
	if _, busy := s.s3Lifecycle.LoadOrStore(id, struct{}{}); busy {
		return nil, apierr.New(409, "pipeline lifecycle change in progress")
	}
	return func() { s.s3Lifecycle.Delete(id) }, nil
}

func (s *PipelineJobs) cancelActiveS3(id string) {
	s.activeS3Mu.Lock()
	defer s.activeS3Mu.Unlock()
	if s.activeS3ID == id && s.activeS3Cancel != nil {
		s.activeS3Cancel()
	}
}
