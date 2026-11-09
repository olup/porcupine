package porcupine

import (
	"strings"
	"testing"
)

func TestIdentitySQLUsesResolutionOnlyWhenRequested(t *testing.T) {
	for _, raw := range []string{"SELECT count(*) FROM events", "SELECT event,count() FROM events GROUP BY event", "SELECT 'person_id' AS label FROM events", "SELECT e.uuid FROM events e"} {
		canonical, err := validateSQL(raw)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := identitySQL(raw, "pc_scope")
		if err != nil || actual != canonical {
			t.Fatalf("ordinaryquery acquired identitywork: %s %v", actual, err)
		}
	}
	for _, raw := range []string{"SELECT * FROM events", "SELECT e.* FROM events AS e", "SELECT distinct_id,person_id FROM events", "SELECT JSONExtractString(person_properties,'name') FROM events", "SELECT e.person_id FROM events e", "WITH a AS(SELECT person_id FROM events) SELECT person_id FROM a UNION ALL SELECT person_id FROM events"} {
		actual, err := identitySQL(raw, "pc_scope")
		if err != nil || !strings.Contains(actual, "events_with_persons") {
			t.Fatalf("identitysource missing: %s: %s %v", raw, actual, err)
		}
	}
	for _, raw := range []string{"SELECT identity_snapshot FROM events", "SELECT original_snapshot FROM events", "SELECT e.identity_snapshot FROM events e", "SELECT e.original_snapshot FROM events e", "SELECT * FROM events_with_persons", "SELECT * FROM other.events"} {
		if _, err := identitySQL(raw, "pc_scope"); err == nil {
			t.Errorf("raw identity/private source accepted: %s", raw)
		}
	}
	plain, err := pipelineWindowSQL("SELECT count(*) FROM events", "pc_scope")
	if err != nil || strings.Contains(plain, "events_with_persons") {
		t.Fatal("plain pipeline forces identityjoins")
	}
	resolved, err := pipelineWindowSQL("SELECT events.person_id FROM events", "pc_scope")
	if err != nil || !strings.Contains(resolved, "pc_scope.events_with_persons AS _porcupine_window WHERE _porcupine_window.received_at_ms") {
		t.Fatalf("pipelinewindow lost identity source: %s %v", resolved, err)
	}
}
func TestIdentitySQLInternalViewCannotBeShadowed(t *testing.T) {
	raw := "WITH events_with_persons AS (SELECT event FROM events) SELECT person_id FROM events"
	rewritten, err := identitySQL(raw, "pc_trusted")
	if err != nil || strings.Count(rewritten, "pc_trusted.events_with_persons") != 2 {
		t.Fatalf("internal source can be shadowed: %s %v", rewritten, err)
	}
	if _, err = validateSQL(raw); err != nil {
		t.Fatal("valid user CTE must stay supported")
	}
}
func TestIdentityViewUsesPrivateScopedCanonicalSnapshots(t *testing.T) {
	sql := identityViewSQL("analytics", "pc_project", "project-a")
	for _, required := range []string{"SQL SECURITY INVOKER", "LEFT ANY JOIN", "argMax(", "toUInt64OrZero(", "identity_snapshot!=''", "JSONExtractString(identity_snapshot,'identified_id')!=''", "AS person_id", "AS person_properties"} {
		if !strings.Contains(sql, required) {
			t.Errorf("identityview missing %s", required)
		}
	}
	if strings.Contains(sql, "original_snapshot") || strings.Contains(analyticsPhysicalColumns, "original_snapshot") {
		t.Fatal("originaleventoriginals granted/exposed")
	}
	if strings.Count(sql, "project_id=unhex(") != 2 {
		t.Fatal("each private history source must bind project explicitly")
	}
}
