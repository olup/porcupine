package delivery

import (
	"context"
	"database/sql"
	"errors"
	"github.com/olup/porcupine/internal/usage"
	"net/http"
	"time"
)

var ErrWebhookCapacity = errors.New("webhook queue is full")

type WebhookLimits struct {
	MaxQueuedEvents        int
	MaxQueuedBytes         int64
	ProjectMaxQueuedEvents int
	ProjectMaxQueuedBytes  int64
	MaxAttempts            int
	TimeoutSeconds         int
}

// WebhookRule is the persisted routing rule. The private destination is encrypted
// separately from this metadata.
type WebhookRule struct {
	IncludeEvents   []string `json:"include_events,omitempty"`
	ExcludeEvents   []string `json:"exclude_events,omitempty"`
	DeliveryMode    string   `json:"delivery_mode,omitempty"`
	IntervalSeconds int64    `json:"interval_seconds,omitempty"`
	StartAt         string   `json:"start_at,omitempty"`
}

type WebhookDestination struct {
	Endpoint string            `json:"endpoint"`
	Headers  map[string]string `json:"headers,omitempty"`
}

// WebhookHost supplies the authorities the journal does not own: current
// permissions, encrypted destinations, and a project deletion activity lease.
type WebhookHost interface {
	AuthorizeWebhook(context.Context, string, string) (bool, error)
	OpenWebhookSecret(string, string, []byte) ([]byte, error)
	BeginWebhookActivity(context.Context, string) (context.Context, func(), bool)
}

// WebhookQueue owns delivery attempts and their journal transitions. Acceptance
// and event mutation retain ownership of their transactions through Enqueue and
// CancelWebhookEvent.
type WebhookQueue struct {
	db     *sql.DB
	limits *WebhookLimits
	clock  func() time.Time
	host   WebhookHost
	Client *http.Client
	Usage  *usage.Recorder
}

func NewWebhookQueue(db *sql.DB, limits *WebhookLimits, clock func() time.Time, host WebhookHost) *WebhookQueue {
	return &WebhookQueue{db: db, limits: limits, clock: clock, host: host, Client: NewWebhookHTTPClient()}
}

// Enqueuer retains no database, transport, host, or commit capability.
func (q *WebhookQueue) Enqueuer() *WebhookEnqueuer {
	return &WebhookEnqueuer{limits: q.limits, clock: q.clock}
}

// InitializeWebhooks seeds accounting and recovers interrupted attempts.
func InitializeWebhooks(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO metadata(key,value) VALUES('webhook_history_changes','0'),('webhook_history_pruned_ms','0');
INSERT OR IGNORE INTO webhook_queue_usage VALUES('instance',0,0);
UPDATE webhook_deliveries SET state='queued',next_attempt_ms=0 WHERE state='sending';`)
	return err
}
