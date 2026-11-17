package output

import (
	"context"
	"database/sql"

	"net/http"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	"github.com/olup/porcupine/internal/usage"
)

type webhookLimits struct {
	MaxQueuedEvents        int
	MaxQueuedBytes         int64
	ProjectMaxQueuedEvents int
	ProjectMaxQueuedBytes  int64
	MaxAttempts            int
	TimeoutSeconds         int
}

type webhookDestination struct {
	Endpoint string            `json:"endpoint"`
	Headers  map[string]string `json:"headers,omitempty"`
}

// sqliteWebhookQueue owns delivery attempts and their journal transitions. Acceptance
// and event mutation retain ownership of their transactions through Enqueue and
// CancelWebhookEvent.
type sqliteWebhookQueue struct {
	db            *sql.DB
	limits        *webhookLimits
	clock         func() time.Time
	authorize     func(context.Context, string, string) (bool, error)
	openSecret    func(string, string, []byte) ([]byte, error)
	beginActivity func(context.Context, string) (context.Context, func(), bool)
	Client        *http.Client
	Usage         *usage.Recorder
}

func newWebhookQueue(db *sql.DB, limits *webhookLimits, clock func() time.Time, authorize func(context.Context, string, string) (bool, error), openSecret func(string, string, []byte) ([]byte, error), beginActivity func(context.Context, string) (context.Context, func(), bool)) *sqliteWebhookQueue {
	return &sqliteWebhookQueue{db: db, limits: limits, clock: clock, authorize: authorize, openSecret: openSecret, beginActivity: beginActivity, Client: delivery.NewWebhookHTTPClient()}
}

// Enqueuer retains no database, transport, host, or commit capability.
func (q *sqliteWebhookQueue) Enqueuer() *WebhookEnqueuer {
	return &WebhookEnqueuer{limits: q.limits, clock: q.clock}
}

// initializeWebhooks seeds accounting and recovers interrupted attempts.
func initializeWebhooks(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO metadata(key,value) VALUES('webhook_history_changes','0'),('webhook_history_pruned_ms','0');
INSERT OR IGNORE INTO webhook_queue_usage VALUES('instance',0,0);
UPDATE webhook_deliveries SET state='queued',next_attempt_ms=0 WHERE state='sending';`)
	return err
}
