package delivery

import (
	"github.com/olup/porcupine/internal/usage"
	"testing"
	"time"
)

func TestWebhookUsageCommitsWithSuccessfulACKOnce(t *testing.T) {
	t.Parallel()
	q, at := webhookQueueFixture(t, webhookTestHost{})
	if _, e := q.db.Exec(usage.JournalSchema); e != nil {
		t.Fatal(e)
	}
	q.Usage = usage.NewRecorder(q.db, "source", func() time.Time { return *at })
	enqueueWebhookTestEvent(t, q)
	d, e := q.claimWebhook(t.Context())
	if e != nil || d == nil {
		t.Fatal(d, e)
	}
	// Metering failure must also roll back the transition to delivered.
	if _, e = q.db.Exec(`CREATE TRIGGER reject_usage BEFORE INSERT ON usage_rollups BEGIN SELECT RAISE(ABORT,'meter unavailable'); END`); e != nil {
		t.Fatal(e)
	}
	if e = q.Complete(t.Context(), d, 200, "", false); e == nil {
		t.Fatal("successful ACK escaped its metering transaction")
	}
	var state string
	q.db.QueryRow(`SELECT state FROM webhook_deliveries WHERE id=?`, d.ID).Scan(&state)
	if state != "sending" {
		t.Fatal(state)
	}
	q.db.Exec(`DROP TRIGGER reject_usage`)
	if e = q.Complete(t.Context(), d, 200, "", false); e != nil {
		t.Fatal(e)
	}
	if e = q.Complete(t.Context(), d, 200, "", false); e != nil {
		t.Fatal(e)
	}
	reports, e := q.Usage.Pending(t.Context())
	if e != nil || len(reports) != 1 || reports[0].Counters.Webhooks != 1 {
		t.Fatal(reports, e)
	}
}
