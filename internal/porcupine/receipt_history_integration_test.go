package porcupine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// Keep this probe in both integration profiles. ClickHouse normalizes TTL
// expressions and encodes aggregate types differently from a Go fake.
func TestClickHouseIntegrationReceiptHistory(t *testing.T) {
	ctx := context.Background()
	ch, err := newClickHouse(integrationClickHouseConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = ch.initialize(ctx); err != nil {
		t.Fatal(err)
	}
	source, empty, err := ch.receiptFacts(ctx, nil)
	if err != nil || source == "" || len(empty) != 0 {
		ddl, _ := ch.admin(ctx, "SHOW CREATE TABLE "+ch.config.Database+"."+eventTable)
		t.Fatalf("empty source proof failed: %v, ddl=%s", err, ddl)
	}

	original := Event{ID: 1, ProjectID: "one", UUID: "present", Event: "read", DistinctID: "example", Timestamp: "2026-01-01T00:00:00Z", ReceivedAt: "2026-01-01T00:00:00Z", Properties: json.RawMessage(`{}`)}
	tombstone := original
	tombstone.ID = 2
	tombstone.UUID = "deleted"
	if err = ch.insert(ctx, []Event{original}); err != nil {
		t.Fatal(err)
	}
	if err = ch.insertProjections(ctx, []eventProjection{{Event: tombstone, Revision: 1, IsDeleted: true}}); err != nil {
		t.Fatal(err)
	}

	keys := []receiptKey{{"one", "present"}, {"one", "deleted"}, {"one", "unseen"}, {"two", "present"}}
	if _, err = ch.admin(ctx, "OPTIMIZE TABLE "+ch.config.Database+"."+eventTable+" FINAL"); err != nil {
		t.Fatal(err)
	}
	gotSource, seen, err := ch.receiptFacts(ctx, keys)
	if err != nil || gotSource != source || len(seen) != 2 || !seen[keys[0]] || !seen[keys[1]] {
		t.Fatalf("history membership source=%s, facts=%v, err=%v", gotSource, seen, err)
	}
	// Server profiles can request FINAL or truncate aggregation and IN sets.
	// The service must override those settings or reject the probe as unverified.
	config := ch.config
	config.User = "receipt_profile_" + strings.ReplaceAll(newID(), "-", "")
	config.Password = token()
	user := config.User
	for _, ddl := range []string{
		"CREATE USER " + user + " IDENTIFIED WITH sha256_password BY '" + config.Password + "' SETTINGS final=1,max_rows_to_group_by=1,group_by_overflow_mode='any',max_rows_in_set=1,set_overflow_mode='break'",
		"GRANT SELECT ON " + ch.config.Database + "." + eventTable + " TO " + user,
		"GRANT SHOW ROW POLICIES ON *.* TO " + user,
		"GRANT SELECT ON system.tables TO " + user,
		"GRANT SELECT ON system.row_policies TO " + user,
		"CREATE ROW POLICY receipt_profile_allow ON " + ch.config.Database + "." + eventTable + " USING 1 TO " + user,
	} {
		if _, err = ch.admin(ctx, ddl); err != nil {
			t.Fatal("cannot configure synthetic probe account", err)
		}
	}
	t.Cleanup(func() { _, _ = ch.admin(context.Background(), "DROP USER IF EXISTS "+user) })
	profile, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	_, seen, err = profile.receiptFacts(ctx, keys)
	if err != nil || len(seen) != 2 || !seen[keys[0]] || !seen[keys[1]] {
		t.Fatalf("profile settings truncated the proof: %v, %v", seen, err)
	}
	_, err = ch.admin(ctx, "CREATE ROW POLICY OR REPLACE receipt_profile_allow ON "+ch.config.Database+"."+eventTable+" USING project_id='two' TO "+user)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = profile.receiptFacts(ctx, keys); err == nil {
		t.Fatal("accepted negative evidence from filtered history")
	}

}
