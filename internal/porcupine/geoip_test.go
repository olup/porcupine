package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

const geoIPTestDatabase = "testdata/geoip-country.mmdb"
const geoIPTestOrigin = "https://browser.example"

func TestGeoIPLocalDatabase(t *testing.T) {
	t.Parallel()
	lookup, err := loadGeoIPCountry(geoIPTestDatabase)
	if err != nil {
		t.Fatal(err)
	}
	for ip, want := range map[string]string{
		"8.8.8.8": "US", "2606:4700::1111": "FR", "10.0.0.3": "AU",
		"9.9.9.9": "", "1.0.0.1": "", "1.1.1.1": "",
	} {
		if got := lookup(netip.MustParseAddr(ip)); got != want {
			t.Errorf("lookup %s got %q, want %q", ip, got, want)
		}
	}
	file := filepath.Join(t.TempDir(), "invalid.mmdb")
	if err := os.WriteFile(file, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"", file, file + ".missing"} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()
			s := openGeoIPTestService(t, filepath.Join(t.TempDir(), "geoip.db"), path)
			projectID, browser := project(t, s)
			geoIPRequest(t, s, "/capture/", captureBody(browser, "no-database"), "8.8.8.8:80", geoIPTestOrigin, nil, 200)
			if geoIPContext(t, s, projectID, "no-database", "") != "" {
				t.Fatal("unexpected enrichment without a usable database")
			}
		})
	}
}

func openGeoIPTestService(t *testing.T, path, mmdb string) *Service {
	t.Helper()
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken, GeoIPDatabase: mmdb, TrustedProxies: []string{"127.0.0.1/32", "10.0.0.2/32"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func geoIPRequest(t *testing.T, s *Service, path, body, peer, origin string, headers map[string]string, status int) map[string]any {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.RemoteAddr = peer
	r.Header.Set("Content-Type", "application/json")
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("%s status %d, want %d: %s", path, w.Code, status, w.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func geoIPContext(t *testing.T, s *Service, projectID, uuid, country string) string {
	t.Helper()
	var properties, metadata string
	if err := s.db.QueryRow(`SELECT properties,ingestion_metadata FROM events WHERE project_id=? AND uuid=?`, projectID, uuid).Scan(&properties, &metadata); err != nil {
		t.Fatal(err)
	}
	// The lookup address and the rest of the MMDB record must never be copied.
	for _, text := range []string{"8.8.8.8", "2606:4700::1111", "never-persist"} {
		if strings.Contains(properties, text) || strings.Contains(metadata, text) {
			t.Fatalf("lookup input or unrelated database record leaked into %s", uuid)
		}
	}
	var record storedIngestionMetadata
	if metadata != "" && json.Unmarshal([]byte(metadata), &record) != nil {
		t.Fatal("invalid stored metadata")
	}
	var value struct {
		GeoIP struct {
			CountryCode string `json:"country_code"`
		} `json:"geoip"`
	}
	if len(record.Context) > 0 && json.Unmarshal(record.Context, &value) != nil {
		t.Fatal("invalid stored context")
	}
	if value.GeoIP.CountryCode != country {
		t.Fatalf("%s got country %q, want %q (context %s)", uuid, value.GeoIP.CountryCode, country, record.Context)
	}
	return string(record.Context)
}

func TestLiveGeoIPProtocolsAndStableRetries(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "geoip.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken, GeoIPDatabase: geoIPTestDatabase})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	projectID, browser := project(t, s)
	raw, err := os.ReadFile("../../compat/fixtures/browser-capture.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Provenance struct{ Path string }
		Payload    json.RawMessage
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	posthog := string(bytes.ReplaceAll(fixture.Payload, []byte("FIXTURE_INGEST_TOKEN"), []byte(browser)))
	const native = `{"type":"track","message_id":"native-msg","name":"native browser","actor_id":"known","context":{"large":9007199254740993,"geoip":{"provider":"client context"}}}`
	const segment = `{"event":"segment browser","messageId":"segment-msg","userId":"known","context":{"locale":"fr-FR"}}`
	for _, tc := range []struct {
		path, body, uuid, contextPart string
		headers                       map[string]string
	}{
		{fixture.Provenance.Path, posthog, "01900000-0000-7000-8000-000000000002", `"browser":{"name":"Chrome","version":"158"}`, nil},
		{"/ingest/v1/events", native, "", `"large":9007199254740993`, map[string]string{"X-Porcupine-Key": browser}},
		{"/v1/track", segment, "", `"locale":"fr-FR"`, segmentHeaders(browser)},
	} {
		first := geoIPRequest(t, s, tc.path, tc.body, "8.8.8.8:80", geoIPTestOrigin, tc.headers, 200)
		retry := geoIPRequest(t, s, tc.path, tc.body, "[2606:4700::1111]:80", geoIPTestOrigin, tc.headers, 200)
		if retry["duplicates"] != float64(1) {
			t.Fatal("retry did not deduplicate", retry)
		}
		uuid := tc.uuid
		if uuid == "" {
			uuid = receiptFrom(t, first, 0)["uuid"].(string)
			if receiptFrom(t, retry, 0)["uuid"] != uuid {
				t.Fatal("retry changed the generated UUID")
			}
		}
		got := geoIPContext(t, s, projectID, uuid, "US")
		if !strings.Contains(got, tc.contextPart) || tc.path == "/ingest/v1/events" && !strings.Contains(got, `"provider":"client context"`) {
			t.Fatalf("%s lost original context %s", tc.path, got)
		}
	}

	// The durable journal reader must still supply context after a restart.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	s = reopened
	queued, err := s.deliveryBatch(t.Context())
	if err != nil || len(queued) != 3 {
		t.Fatalf("read durable queue after restart: %v, %v", queued, err)
	}
	for _, event := range queued {
		if !bytes.Contains(event.Context, []byte(`"country_code":"US"`)) {
			t.Fatalf("queued %s lost country after restart: %s", event.UUID, event.Context)
		}
	}
}

func TestLiveGeoIPChecksOriginCredentialPeerAndPrivacy(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "geoip.db")
	s := openGeoIPTestService(t, path, geoIPTestDatabase)
	projectID, browser := project(t, s)
	_, server := key(t, s, []string{"events:write"}, []string{projectID})
	lookup := s.geoIPCountry
	var lookups []netip.Addr
	s.geoIPCountry = func(ip netip.Addr) string {
		lookups = append(lookups, ip)
		return lookup(ip)
	}
	// Even a valid public selector cannot opt a server credential into enrichment.
	geoIPRequest(t, s, "/capture/", captureBody(browser, "server"), "8.8.8.8:80", geoIPTestOrigin, map[string]string{"Authorization": "Bearer " + server}, 200)
	geoIPContext(t, s, projectID, "server", "")
	for _, tc := range []struct{ id, ip, xff, origin, country string }{
		{"no-origin", "8.8.8.8:80", "", "", ""},
		{"opaque-origin", "8.8.8.8:80", "", "null", ""},
		{"spoofed-xff", "8.8.8.8:80", "2606:4700::1111", geoIPTestOrigin, "US"},
		{"trusted-xff", "127.0.0.1:80", "8.8.8.8, 2606:4700::1111, 10.0.0.2", geoIPTestOrigin, "FR"},
		{"trusted-no-xff", "127.0.0.1:80", "", geoIPTestOrigin, ""},
		{"trusted-all", "127.0.0.1:80", "10.0.0.2", geoIPTestOrigin, ""},
		{"bad-xff", "127.0.0.1:80", "unknown, 8.8.8.8", geoIPTestOrigin, ""},
		{"bad-peer", "invalid", "", geoIPTestOrigin, ""},
		{"private-peer", "10.0.0.3:80", "", geoIPTestOrigin, ""},
		{"shared-peer", "100.64.0.7:80", "", geoIPTestOrigin, ""},
		{"no-match", "1.1.1.1:80", "", geoIPTestOrigin, ""},
		{"bad-country", "9.9.9.9:80", "", geoIPTestOrigin, ""},
	} {
		var headers map[string]string
		if tc.xff != "" {
			headers = map[string]string{"X-Forwarded-For": tc.xff}
		}
		geoIPRequest(t, s, "/capture/", captureBody(browser, tc.id), tc.ip, tc.origin, headers, 200)
		geoIPContext(t, s, projectID, tc.id, tc.country)
	}
	geoIPRequest(t, s, "/capture/", `{"api_key":"`+browser+`","event":"$identify","uuid":"identity","distinct_id":"known"}`, "8.8.8.8:80", geoIPTestOrigin, nil, 200)
	geoIPContext(t, s, projectID, "identity", "")
	geoIPRequest(t, s, "/capture/", captureBody("pc_ci_invalid", "invalid"), "8.8.8.8:80", geoIPTestOrigin, nil, 401)
	geoIPRequest(t, s, "/capture/", `{"api_key":"`+browser+`","batch":[{"event":"purchase","distinct_id":"known"},{}]}`, "8.8.8.8:80", geoIPTestOrigin, nil, 400)
	optOut := geoIPRequest(t, s, "/v1/track", `{"event":"segment opt out","messageId":"segment-opt-out","userId":"known","context":{"geoip_disable":true}}`, "8.8.8.8:80", geoIPTestOrigin, segmentHeaders(browser), 200)
	geoIPContext(t, s, projectID, receiptFrom(t, optOut, 0)["uuid"].(string), "")
	for _, flags := range []string{`"$geoip_disable":true`, `"$is_server":true`} {
		geoIPRequest(t, s, "/capture/", `{"api_key":"`+browser+`","event":"purchase","distinct_id":"known","properties":{`+flags+`}}`, "8.8.8.8:80", geoIPTestOrigin, nil, 200)
	}

	expectedLookups := []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("2606:4700::1111"), netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("9.9.9.9")}
	if !slices.Equal(lookups, expectedLookups) {
		t.Fatalf("non-eligible event reached lookup: got %v want %v", lookups, expectedLookups)
	}
	lookups = nil
	body := `{"api_key":"` + browser + `","batch":[
	  {"event":"purchase","uuid":"cookieless","properties":{"distinct_id":"` + ingestfmt.CookielessSentinel + `","$ip":"203.0.113.41","$session_id":"do-not-store","$current_url":"https://browser.example/path"}},
	  {"event":"purchase","uuid":"second","distinct_id":"known"},
	  {"event":"purchase","uuid":"opt-out","distinct_id":"known","properties":{"$geoip_disable":true}}
	]}`
	geoIPRequest(t, s, "/batch/", body, "8.8.8.8:80", geoIPTestOrigin, nil, 200)
	if len(lookups) != 1 {
		t.Fatalf("mixed batch ran %d lookups, want one", len(lookups))
	}
	storedContext := geoIPContext(t, s, projectID, "cookieless", "US")
	if strings.Contains(storedContext, "do-not-store") || !strings.Contains(storedContext, "https://browser.example/path") {
		t.Fatalf("cookieless context has unintended values: %s", storedContext)
	}
	geoIPContext(t, s, projectID, "second", "US")
	geoIPContext(t, s, projectID, "opt-out", "")
	for _, file := range []string{path, path + "-wal"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{"8.8.8.8", "2606:4700::1111", "203.0.113.41", "do-not-store"} {
			if bytes.Contains(raw, []byte(secret)) {
				t.Fatalf("private value in %s", filepath.Base(file))
			}
		}
	}
}

func TestDuckLakeS3SharedPostgresHostedGeoIP(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	config := hostedJourneyConfig(t, dsn, fixture)
	config.GeoIPDatabase = geoIPTestDatabase
	s, host, _ := hostedCleanupWorker(t, config)
	projectID, browser := sharedJourneyProject(t, host.URL, defaultTenantID, "GeoIP")
	_, reader := sharedJourneyKey(t, host.URL, projectID, []string{"events:read"})
	body := `{"api_key":"` + browser + `","event":"purchase","uuid":"hosted-geoip","properties":{"distinct_id":"` + ingestfmt.CookielessSentinel + `","$ip":"203.0.113.41","$session_id":"hosted-private-session"}}`
	for _, ip := range []string{"8.8.8.8:80", "[2606:4700::1111]:80"} {
		ack := geoIPRequest(t, s, "/capture/", body, ip, geoIPTestOrigin, nil, 200)
		if ack["durability"] != "buffered" || ack["received"] != float64(1) {
			t.Fatal("unexpected buffered acknowledgment", ack)
		}
	}
	drain, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := s.hosted.buffer.Drain(drain); err != nil {
		t.Fatal(err)
	}
	rows := sharedJourneyQuery(t, host.URL, projectID, reader, "SELECT uuid, json_extract_string(context, '$.geoip.country_code') AS country, context, properties FROM events", 200)["result"].(map[string]any)["data"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected one event after retry, got %v", rows)
	}
	row := rows[0].(map[string]any)
	if row["uuid"] != "hosted-geoip" || row["country"] != "US" {
		t.Fatalf("GeoIP changed retry identity or first occurrence: %v", row)
	}
	raw, _ := json.Marshal(row)
	for _, private := range []string{"8.8.8.8", "2606:4700::1111", "203.0.113.41", "hosted-private-session"} {
		if bytes.Contains(raw, []byte(private)) {
			t.Fatal("hosted projection contains a private value")
		}
	}
}
