package porcupine

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
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

type pipelineRuntime struct {
	s3Lifecycle  sync.Map // per-pipeline exclusion between resume and remote deletion cleanup
	admissionsMu sync.Mutex
	admissions   map[*pipelineAdmission]struct{}

	config        PipelineConfig
	aead          cipher.AEAD
	fingerprint   string
	s3Origins     map[string]bool
	webhookClient *http.Client
	webhookWake   chan struct{}
	s3Wake        chan struct{}
	cancel        context.CancelFunc
	workers       sync.WaitGroup
}

func newPipelineRuntime(input *PipelineConfig) (*pipelineRuntime, error) {
	if input == nil {
		return nil, nil
	}
	c := *input
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
	digest := sha256.Sum256(c.Key)
	c.Key = nil
	c.ExportLimits, err = normalizeExportConfig(c.ExportLimits)
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
	return &pipelineRuntime{admissions: make(map[*pipelineAdmission]struct{}), config: c, aead: aead, fingerprint: hex.EncodeToString(digest[:]), s3Origins: origins, webhookWake: make(chan struct{}, 1), s3Wake: make(chan struct{}, 1)}, nil
}

func pipelineS3Origin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || !exportstore.ValidEndpoint(raw) || strings.ContainsAny(raw, "?#*") || (u.EscapedPath() != "" && u.EscapedPath() != "/") {
		return "", errors.New("S3 endpoint must be an exact HTTP(S) origin")
	}
	return u.Scheme + "://" + strings.ToLower(u.Host), nil
}

func (s *Service) validatePipelineS3Endpoint(raw string) error {
	if raw == "" {
		return nil
	}
	origin, err := pipelineS3Origin(raw)
	if err != nil || s.pipelines == nil || !s.pipelines.s3Origins[origin] {
		return errors.New("S3 endpoint is not allowed by this deployment")
	}
	return nil
}

func pipelineAAD(projectID, pipelineID string) []byte {
	data, _ := json.Marshal([3]string{"porcupine/pipeline/v1", projectID, pipelineID})
	return data
}

func (s *Service) sealPipelineSecret(projectID, pipelineID string, plaintext []byte) ([]byte, error) {
	if s.pipelines == nil {
		return nil, errors.New("pipelines disabled")
	}
	aead := s.pipelines.aead
	blob := make([]byte, 1+aead.NonceSize())
	blob[0] = 1
	if _, err := rand.Read(blob[1:]); err != nil {
		return nil, err
	}
	return aead.Seal(blob, blob[1:], plaintext, pipelineAAD(projectID, pipelineID)), nil
}

func (s *Service) openPipelineSecret(projectID, pipelineID string, blob []byte) ([]byte, error) {
	if s.pipelines == nil {
		return nil, errors.New("pipelines disabled")
	}
	aead := s.pipelines.aead
	start := 1 + aead.NonceSize()
	if len(blob) < start+aead.Overhead() || blob[0] != 1 {
		return nil, errors.New("invalid encrypted destination")
	}
	return aead.Open(nil, blob[1:start], blob[start:], pipelineAAD(projectID, pipelineID))
}

func (s *Service) initializePipelines(ctx context.Context) error {
	if err := s.initializePipelineSchema(ctx); err != nil {
		return err
	}
	if err := s.verifyPipelineConfig(ctx); err != nil {
		return err
	}
	if err := s.initializeWebhook(ctx); err != nil {
		return err
	}
	return s.initializeS3Pipelines(ctx)
}

func (s *Service) verifyPipelineConfig(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists, activeS3 bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pipelines),EXISTS(SELECT 1 FROM pipelines WHERE kind='s3' AND state='active')").Scan(&exists, &activeS3); err != nil {
		return err
	}
	var previous string
	err = tx.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key='pipeline_key_fingerprint'").Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if s.pipelines == nil {
		if exists {
			return errors.New("existing pipelines require their original encryption key")
		}
		return nil
	}
	if activeS3 && s.ch == nil {
		return errors.New("active S3 pipelines require ClickHouse")
	}
	if exists && previous != s.pipelines.fingerprint {
		return errors.New("pipeline encryption key does not match existing destinations")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO metadata(key,value) VALUES('pipeline_key_fingerprint',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", s.pipelines.fingerprint); err != nil {
		return err
	}
	return tx.Commit()
}

// Heavy queries share one slot so a batch cannot occupy the last interactive slot.
func (s *Service) acquirePipelineExport(ctx context.Context) (func(), bool) {
	if ctx.Err() != nil || len(s.querySlots) != 0 {
		return nil, false
	}
	select {
	case s.batchSlot <- struct{}{}:
	default:
		return nil, false
	}
	select {
	case s.querySlots <- struct{}{}:
		return func() { <-s.querySlots; <-s.batchSlot }, true
	default:
		<-s.batchSlot
		return nil, false
	}
}

func signalPipelineWorker(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (s *Service) wakeWebhook() {
	if s.pipelines != nil {
		signalPipelineWorker(s.pipelines.webhookWake)
	}
}

func (s *Service) pipelineWake() {
	if s.pipelines == nil {
		return
	}
	s.wakeWebhook()
	signalPipelineWorker(s.pipelines.s3Wake)
}

func (s *Service) startPipelines() {
	rt := s.pipelines
	if rt == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt.cancel = cancel
	start := func(destination string, wake chan struct{}, step func(context.Context) (bool, error)) {
		rt.workers.Add(1)
		go func() {
			defer rt.workers.Done()
			var lastWarning time.Time
			for ctx.Err() == nil {
				progress, err := step(ctx)
				if err != nil && ctx.Err() == nil && time.Since(lastWarning) >= time.Minute {
					slog.Warn("pipeline worker could not advance; retrying", "destination_type", destination)
					lastWarning = time.Now()
				}
				if progress && err == nil {
					continue
				}
				timer := time.NewTimer(time.Second)
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
	start("webhook", rt.webhookWake, s.stepWebhook)
	if s.ch != nil {
		start("s3", rt.s3Wake, s.stepPipelineS3)
	}
}

func (s *Service) stopPipelines() {
	if s.pipelines != nil && s.pipelines.cancel != nil {
		s.pipelines.cancel()
		s.pipelines.workers.Wait()
	}
}

// Receipt timestamps are assigned before validation. A closed-window snapshot
// waits for older arrivals still in flight, including captures blocked on SQLite.
// The capture admission limit bounds this map; no database lock is held here.
type pipelineAdmission struct {
	projectID string
	arrivedMS int64
}

func (s *Service) beginCaptureReceipt(projectID string) (time.Time, func()) {
	rt := s.pipelines
	if rt == nil || s.ch == nil {
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

func (s *Service) pipelineWindowHasAdmissions(projectID string, startMS, endMS int64) bool {
	rt := s.pipelines
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
func (s *Service) lockS3Lifecycle(id string) (func(), error) {
	if _, busy := s.pipelines.s3Lifecycle.LoadOrStore(id, struct{}{}); busy {
		return nil, apiError(409, "pipeline lifecycle change in progress")
	}
	return func() { s.pipelines.s3Lifecycle.Delete(id) }, nil
}
