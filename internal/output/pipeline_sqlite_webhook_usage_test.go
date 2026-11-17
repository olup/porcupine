package output

import (
	"testing"
	"time"

	"github.com/olup/porcupine/internal/usage"
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

func TestWebhookStaleAttemptCannotACKCurrentRetry(t *testing.T) {
	t.Parallel()
	q, at := webhookQueueFixture(t, webhookTestHost{})
	if _, err := q.db.Exec(usage.JournalSchema); err != nil {
		t.Fatal(err)
	}
	q.Usage = usage.NewRecorder(q.db, "retry-source", func() time.Time { return *at })
	enqueueWebhookTestEvent(t, q)
	old, err := q.claimWebhook(t.Context())
	if err != nil || old == nil {
		t.Fatal(old, err)
	}
	if err = q.Complete(t.Context(), old, 503, "http_rejected", true); err != nil {
		t.Fatal(err)
	}
	*at = at.Add(time.Minute)
	current, err := q.claimWebhook(t.Context())
	if err != nil || current == nil || current.AttemptID == old.AttemptID || current.ID != old.ID || current.Payload != old.Payload {
		t.Fatal("retry claim/body was not fenced", current, err)
	}
	if err = q.Complete(t.Context(), old, 200, "", false); err != nil {
		t.Fatal(err)
	}
	var state string
	q.db.QueryRow(`SELECT state FROM webhook_deliveries WHERE id=?`, old.ID).Scan(&state)
	if state != "sending" {
		t.Fatal("old attempt changed current state", state)
	}
	if reports, err := q.Usage.Pending(t.Context()); err != nil || len(reports) != 0 {
		t.Fatal("old attempt metered", reports, err)
	}
	if err = q.Complete(t.Context(), current, 200, "", false); err != nil {
		t.Fatal(err)
	}
	if reports, err := q.Usage.Pending(t.Context()); err != nil || len(reports) != 1 || reports[0].Counters.Webhooks != 1 {
		t.Fatal(reports, err)
	}
}
