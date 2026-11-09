package porcupine

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestClickHouseIntegrationQueryCache(t *testing.T) {
	config := integrationClickHouseConfig(t)
	ch, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	localConfig := Config{DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken, QueueMaxEvents: 100}
	s, err := Open(localConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	configureAnalyticsTest(s, ch)
	a, ingest := project(t, s)
	b, otherIngest := project(t, s)
	keyID, readerToken := key(t, s, []string{"events:read"}, []string{a})
	ctx := context.Background()
	query := func(project, event string, cache *bool) string {
		t.Helper()
		input := queryInput(project, "SELECT count() AS total FROM events WHERE event={event:String}")
		input.Body.Parameters = map[string]string{"event": event}
		input.Body.UseCache = cache
		result, err := s.analyticsQuery(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Trim(string(result.Body.Result.Data[0]["total"]), `"`)
	}
	capture := func(token, uuid, event string) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"api_key": token, "uuid": uuid, "event": event, "distinct_id": "synthetic", "properties": map[string]any{}})
		request(t, s, "POST", "/capture/", "", string(body), 200)
		if err := s.flushDelivery(ctx); err != nil {
			t.Fatal(err)
		}
	}
	adminNumber := func(sql string) uint64 {
		t.Helper()
		data, err := ch.admin(ctx, sql+" FORMAT TSV")
		if err != nil {
			t.Fatal(err)
		}
		number, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			t.Fatal("invalid diagnostic response")
		}
		return number
	}
	hits := func() uint64 {
		return adminNumber("SELECT sum(value) FROM system.events WHERE event='QueryCacheHits'")
	}
	cacheEntries := func() uint64 {
		return adminNumber("SELECT count() FROM system.query_cache WHERE tag='" + s.queryCacheEpoch + ":0' AND NOT stale")
	}
	capture(ingest, "cache-original", "purchase")
	capture(otherIngest, "other-project", "purchase")
	if query(a, "purchase", nil) != "1" {
		t.Fatal("cache warmup wrong")
	}
	before := hits()
	if query(a, "purchase", new(true)) != "1" || hits() <= before {
		t.Fatal("second native query did not hit ClickHouse result cache")
	}
	if cacheEntries() == 0 {
		t.Fatal("native result cache contains no project entry")
	}
	if query(a, "missing", nil) != "0" || query(b, "purchase", nil) != "1" {
		t.Fatal("typed parameters or project credentials shared cached results")
	}
	capture(ingest, "cache-new", "purchase")
	if query(a, "purchase", nil) != "1" || query(a, "purchase", new(false)) != "2" {
		t.Fatal("cached/fresh result contract not respected after insert")
	}
	if query(a, "purchase", nil) != "1" {
		t.Fatal("fresh request overwrote the cached result")
	}
	beforeEntries := cacheEntries()
	if query(a, "never-cached", new(false)) != "0" || cacheEntries() != beforeEntries {
		t.Fatal("fresh request wrote a cache entry")
	}
	// now() remains accepted, but its nondeterministic result must not be cached.
	input := queryInput(a, "SELECT now() AS moment FROM events LIMIT 1")
	if _, err := s.analyticsQuery(ctx, input); err != nil {
		t.Fatal("nondeterministic query should bypass caching", err)
	}
	if cacheEntries() != beforeEntries {
		t.Fatal("nondeterministic query was cached")
	}
	_, cachedReader, cachedPassword, err := s.analyticsReaderForCache(ctx, a, true)
	if err != nil {
		t.Fatal(err)
	}
	namespace, _, _, _ := s.analyticsReader(ctx, a)
	for _, sql := range []string{
		"SELECT event FROM events SETTINGS final=0",
		"SELECT event FROM events SETTINGS max_result_rows=0",
		"SELECT event FROM events SETTINGS readonly=0",
		"SELECT event FROM events SETTINGS query_cache_share_between_users=1",
		"SELECT original_snapshot FROM " + config.Database + "." + eventTable,
	} {
		if _, err := ch.query(ctx, cachedReader, cachedPassword, namespace, sql, nil); err == nil {
			t.Fatal("cached reader bypassed immutable permissions or limits")
		}
	}
	edit, err := s.editEvent(mutationContext("cache-editor"), mutationEdit(a, "cache-original", "cache-edit", "0"))
	if err != nil {
		t.Fatal(err)
	}
	before = hits()
	if query(a, "purchase", nil) != "2" || hits() != before {
		t.Fatal("pending edit reused old cached result")
	}
	// A mandatory native get sees the accepted snapshot, independently of cache.
	current, err := s.getEvent(ctx, &EventGetInput{ID: a, UUID: "cache-original"})
	if err != nil || !current.Body.Pending || current.Body.Event.Event != "corrected" {
		t.Fatal("mandatory mutation read used cached data")
	}
	if _, err := s.deliverMutations(ctx); err != nil {
		t.Fatal(err)
	}
	if query(a, "purchase", nil) != "1" || query(a, "corrected", nil) != "1" {
		t.Fatal("applied edit reused pre-edit cached results")
	}
	before = hits()
	if query(a, "corrected", nil) != "1" || hits() <= before {
		t.Fatal("caching failed to resume after edit projection")
	}
	deleted, err := s.deleteEvent(mutationContext("cache-editor"), &EventDeleteInput{ID: a, UUID: "cache-original", Body: EventDeleteBody{IdempotencyKey: "cache-delete", ExpectedRevision: edit.Body.Revision}})
	if err != nil || deleted.Body.State != "pending" {
		t.Fatal("deletion was not accepted", err)
	}
	before = hits()
	if query(a, "corrected", nil) != "1" || hits() != before {
		t.Fatal("pending deletion did not force fresh projected read")
	}
	if _, err := s.deliverMutations(ctx); err != nil {
		t.Fatal(err)
	}
	if query(a, "corrected", nil) != "0" {
		t.Fatal("deleted fact survived through pre-delete cache")
	}
	before = hits()
	if query(a, "corrected", nil) != "0" || hits() <= before {
		t.Fatal("caching failed to resume after deletion")
	}
	// MCP uses the identical optional field and fresh reader.
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	mcpCtx, session := mcpTestSession(t, host.URL, readerToken, "")
	for _, mode := range []bool{true, false} {
		result := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "query-events", Path: map[string]string{"id": a}, Body: map[string]any{"sql": "SELECT count() AS total FROM events WHERE event={event:String}", "parameters": map[string]string{"event": "corrected"}, "use_cache": mode}}, 200)
		if result["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "0" {
			t.Fatal("MCP cache mode differs from API")
		}
	}
	request(t, s, "DELETE", "/api/v1/access-keys/"+keyID, adminToken, "", 204)
	request(t, s, "POST", "/api/v1/projects/"+a+"/query", readerToken, `{"sql":"SELECT count() FROM events"}`, 401)
	// Native TTL is fixed, cannot be extended by the reader.
	ttl := adminNumber("SELECT max(dateDiff('second',now(),expires_at)) FROM system.query_cache WHERE tag LIKE '" + s.queryCacheEpoch + ":%' AND NOT stale")
	if ttl > 60 || ttl < 45 {
		t.Fatal("unexpected native cache TTL", ttl)
	}
	// Migrate persisted reader settings before first use after restart. A new
	// process epoch also makes old server cache entries unreachable, even when
	// restoring a SQLite backup with older mutation metadata.
	previousEpoch := s.queryCacheEpoch
	if _, err := ch.admin(ctx, "ALTER USER "+cachedReader+" SETTINGS query_cache_ttl=3600 READONLY,query_cache_share_between_users=1 READONLY"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(localConfig)
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, ch)
	before = hits()
	if query(a, "corrected", nil) != "0" || hits() != before || previousEpoch == s.queryCacheEpoch {
		t.Fatal("new process reused previous cache epoch")
	}
	_, account, password, err := s.analyticsReaderForCache(ctx, a, true)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := ch.query(ctx, account, password, namespace, "SELECT toString(getSetting('query_cache_ttl')) AS ttl,toString(getSetting('query_cache_share_between_users')) AS shared", nil)
	if err != nil || !strings.Contains(string(settings), `"ttl": "60"`) || !strings.Contains(string(settings), `"shared": "false"`) {
		t.Fatalf("persisted reader cache settings were not migrated before use: %s %v", settings, err)
	}
	t.Log("native result cache hits, fresh bypass, typed parameters, project isolation, mutation generations and TTL verified", time.Now().UTC().Format(time.RFC3339))
}
