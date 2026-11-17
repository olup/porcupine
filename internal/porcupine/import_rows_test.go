package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/olup/porcupine/internal/importer"
)

func importRowsForTest(t *testing.T, s *Service, project, kind string, raw ...string) []ImportRowResult {
	return importRowsForSourceTest(t, s, project, "posthog", kind, raw...)
}
func importRowsForSourceTest(t *testing.T, s *Service, project, source, kind string, raw ...string) []ImportRowResult {
	t.Helper()
	rows := make([]importer.Row, len(raw))
	for i, value := range raw {
		var err error
		rows[i], err = importer.ParsePostHog([]byte(value), kind)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := prepareImportUploadSource(project, source, rows); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		t.Fatal(err)
	}
	results := make([]ImportRowResult, len(rows))
	for i, row := range rows {
		results[i], err = s.eventRules.AcceptHistoricalTx(ctx, tx, project, row, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return results
}
func TestImportRowsDedupBothDirectionsAndNoDestinations(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	p, key := project(t, s)
	live := `{"api_key":%q,"uuid":"same","event":"live","distinct_id":"u"}`
	request(t, s, "POST", "/capture/", "", fmt.Sprintf(live, key), 200)
	result := importRowsForTest(t, s, p, "events", `{"uuid":"same","event":"archive","distinct_id":"u","timestamp":"2024-01-01T00:00:00Z","properties":{}}`)[0]
	if result.Duplicates != 1 {
		t.Fatal("import duplicated an accepted live UUID")
	}
	result = importRowsForTest(t, s, p, "events", `{"uuid":"import-first","event":"$autocapture","distinct_id":"u","timestamp":"2024-01-01T00:00:00Z","properties":{}}`)[0]
	if result.Accepted != 1 {
		t.Fatal("import rejected")
	}
	duplicate := request(t, s, "POST", "/capture/", "", fmt.Sprintf(`{"api_key":%q,"uuid":"import-first","event":"different-live","distinct_id":"u"}`, key), 200)
	if duplicate["duplicates"] != float64(1) {
		t.Fatal("live retry duplicated imported UUID")
	}
	var imported bool
	var timestamp string
	if s.db.QueryRow(`SELECT is_imported,timestamp FROM events WHERE uuid='import-first'`).Scan(&imported, &timestamp) != nil || !imported || timestamp != "2024-01-01T00:00:00Z" {
		t.Fatal("archive event time/marker lost")
	}
	var deliveries int
	if s.db.QueryRow(`SELECT count(*) FROM webhook_deliveries`).Scan(&deliveries) != nil || deliveries != 0 {
		t.Fatal("import triggered outbound delivery")
	}
}
func TestImportPersonsProfilesWithoutInventedLinksOrFreshOverwrite(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	p, key := project(t, s)
	person := func(version int, name string) string {
		return fmt.Sprintf(`{"person_id":"posthog-internal-uuid","distinct_id":"anonymous-original","person_version":%d,"person_distinct_id_version":1,"properties":{"name":%q},"created_at":"2024-01-01T00:00:00.123456Z","is_deleted":false}`, version, name)
	}
	first := importRowsForTest(t, s, p, "persons", person(10, "archive"))[0]
	if first.Accepted != 1 || first.Unresolved != 1 {
		t.Fatal("person map should hydrate a profile without claiming an app-user merge")
	}
	traits, identified, _ := storedIdentityProfile(t, s, p, "anonymous-original")
	if traits["name"] != "archive" || identified {
		t.Fatal("import invented identified app identity")
	}
	var links int
	s.db.QueryRow(`SELECT count(*) FROM identity_links`).Scan(&links)
	if links != 0 {
		t.Fatal("internal person UUID turned into a live identity link")
	}
	importRowsForTest(t, s, p, "persons", person(11, "newer archive"))
	traits, _, _ = storedIdentityProfile(t, s, p, "anonymous-original")
	if traits["name"] != "newer archive" {
		t.Fatal("newer import-owned profile version ignored")
	}
	ignored := importRowsForTest(t, s, p, "persons", person(9, "stale archive"))[0]
	if ignored.Ignored != 1 {
		t.Fatal("out-of-order snapshot changed current profile")
	}
	identityCapture(t, s, key, []map[string]any{setIdentityEvent("live-profile", "anonymous-original", map[string]any{"name": "fresh live"}, map[string]any{})}, 200)
	ignored = importRowsForTest(t, s, p, "persons", person(99, "must-not-overwrite-live"))[0]
	traits, _, _ = storedIdentityProfile(t, s, p, "anonymous-original")
	if ignored.Ignored != 1 || traits["name"] != "fresh live" {
		t.Fatal("historical snapshot overwrote fresh live traits")
	}
}
func TestImportIdentityNeedsExplicitEvidenceAndPreservesLiveProfiles(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	p, key := project(t, s)
	rows := importRowsForTest(t, s, p, "events", `{"uuid":"import-link","event":"$identify","distinct_id":"account","timestamp":"2024-01-01T00:00:00Z","properties":{"$anon_distinct_id":"anon","$set":{"stale":"ignored"}}}`)
	if rows[0].Accepted != 1 || rows[0].Unresolved != 0 {
		t.Fatal("explicit archived link rejected")
	}
	var target string
	if s.db.QueryRow(`SELECT identified_id FROM identity_links WHERE anonymous_id='anon'`).Scan(&target) != nil || target != "account" {
		t.Fatal("explicit known link missing")
	}
	profile, _, _ := storedIdentityProfile(t, s, p, "account")
	if len(profile) != 0 {
		t.Fatal("archived incremental traits were replayed")
	}
	identityCapture(t, s, key, []map[string]any{identifyEvent("live-identify", "live-account", "", map[string]any{"name": "current"}, map[string]any{})}, 200)
	ignored := importRowsForTest(t, s, p, "events", `{"uuid":"stale-control","event":"$identify","distinct_id":"live-account","timestamp":"2020-01-01T00:00:00Z","properties":{"$anon_distinct_id":"old-anon","$set":{"name":"stale"}}}`)[0]
	profile, _, _ = storedIdentityProfile(t, s, p, "live-account")
	if ignored.Unresolved != 1 || profile["name"] != "current" {
		t.Fatal("archive identity overwrote live state")
	}
	noArgs := importRowsForTest(t, s, p, "events", `{"uuid":"stripped-set","event":"$set","distinct_id":"live-account","timestamp":"2020-01-01T00:00:00Z","properties":{}}`)[0]
	if noArgs.Accepted != 1 || noArgs.Unresolved != 1 {
		t.Fatal("stripped historical $set should be an acknowledged control noop")
	}
	var raw string
	s.db.QueryRow(`SELECT identity_snapshot FROM events WHERE uuid='stripped-set'`).Scan(&raw)
	var snapshot identitySnapshot
	if json.Unmarshal([]byte(raw), &snapshot) != nil || snapshot.IdentifiedID != "" {
		t.Fatal("stripped archived control made up profile state")
	}
}

func TestImportPersonDeletionCannotResurrectOlderOrEraseLive(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	p, key := project(t, s)
	importRowsForTest(t, s, p, "persons", `{"person_id":"internal","distinct_id":"u","person_version":10,"person_distinct_id_version":1,"properties":{"old":true}}`)
	deleted := importRowsForTest(t, s, p, "persons", `{"person_id":"internal","distinct_id":"u","person_version":11,"person_distinct_id_version":1,"properties":null,"is_deleted":true}`)[0]
	traits, _, _ := storedIdentityProfile(t, s, p, "u")
	if deleted.Accepted != 1 || len(traits) != 0 {
		t.Fatal("deleted import-owned person retained archived traits")
	}
	old := importRowsForTest(t, s, p, "persons", `{"person_id":"internal","distinct_id":"u","person_version":9,"person_distinct_id_version":1,"properties":{"stale":true}}`)[0]
	traits, _, _ = storedIdentityProfile(t, s, p, "u")
	if old.Ignored != 1 || len(traits) != 0 {
		t.Fatal("old archive resurrected deleted person")
	}
	identityCapture(t, s, key, []map[string]any{setIdentityEvent("live-after-delete", "u", map[string]any{"fresh": true}, map[string]any{})}, 200)
	freshDelete := importRowsForTest(t, s, p, "persons", `{"person_id":"internal","distinct_id":"u","person_version":99,"person_distinct_id_version":1,"properties":{},"is_deleted":true}`)[0]
	traits, _, _ = storedIdentityProfile(t, s, p, "u")
	if freshDelete.Ignored != 1 || traits["fresh"] != true {
		t.Fatal("archive deletion erased a newer live profile")
	}
}

func TestImportDeletedPersonCannotBeRevivedByOlderIdentify(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	p, _ := project(t, s)
	importRowsForTest(t, s, p, "persons", `{"person_id":"internal-deleted","distinct_id":"deleted-account","person_version":20,"person_distinct_id_version":1,"properties":null,"is_deleted":true}`)
	result := importRowsForTest(t, s, p, "events", `{"uuid":"ancient-identify","event":"$identify","distinct_id":"deleted-account","timestamp":"2000-01-01T00:00:00Z","properties":{"$anon_distinct_id":"old-browser"}}`)[0]
	var profiles, links int
	s.db.QueryRow(`SELECT count(*) FROM identity_profiles`).Scan(&profiles)
	s.db.QueryRow(`SELECT count(*) FROM identity_links`).Scan(&links)
	if result.Unresolved != 1 || profiles != 0 || links != 0 {
		t.Fatal("old identity event resurrected deleted exported person")
	}
}

func TestImportPersonVersionsCannotCrossSourceNamespaces(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	p, _ := project(t, s)
	first := importRowsForSourceTest(t, s, p, "archive-a", "persons", `{"person_id":"internal-a","distinct_id":"shared-id","person_version":1,"person_distinct_id_version":1,"properties":{"name":"source a"}}`)[0]
	second := importRowsForSourceTest(t, s, p, "archive-b", "persons", `{"person_id":"internal-b","distinct_id":"shared-id","person_version":999999,"person_distinct_id_version":999999,"properties":{"name":"source b"}}`)[0]
	traits, _, _ := storedIdentityProfile(t, s, p, "shared-id")
	if first.Accepted != 1 || second.Ignored != 1 || second.Unresolved != 1 || traits["name"] != "source a" {
		t.Fatal("unrelated PostHog source versions overwrote imported person")
	}
}

func TestImportExplicitTargetOnlyIdentifyDoesNotInventLink(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	p, _ := project(t, s)
	result := importRowsForTest(t, s, p, "events", `{"uuid":"node-identify-history","event":"$identify","distinct_id":"explicit-user","timestamp":"2020-01-01T00:00:00Z","properties":{}}`)[0]
	traits, identified, _ := storedIdentityProfile(t, s, p, "explicit-user")
	var links int
	s.db.QueryRow(`SELECT count(*) FROM identity_links`).Scan(&links)
	if result.Accepted != 1 || result.Unresolved != 0 || !identified || len(traits) != 0 || links != 0 {
		t.Fatal("target-only evidence must not create anonymous links or traits")
	}
}
