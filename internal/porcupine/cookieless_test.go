package porcupine

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func cookielessRequest(t *testing.T, s *Service, token, uuid, peer, ua string, headers map[string]string, status int) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"api_key": token, "event": "visit", "uuid": uuid, "properties": map[string]any{"distinct_id": cookielessSentinel}})
	if err != nil {
		t.Fatal(err)
	}
	return cookielessRawRequest(t, s, body, peer, ua, headers, status)
}

func cookielessRawRequest(t *testing.T, s *Service, body []byte, peer, ua string, headers map[string]string, status int) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "/capture/", bytes.NewReader(body))
	r.RemoteAddr = peer
	r.Header.Set("User-Agent", ua)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("got %d want %d: %s", w.Code, status, w.Body.String())
	}
	return w
}

func storedCookieless(t *testing.T, s *Service, uuid string) Event {
	t.Helper()
	var e Event
	var props string
	if err := s.db.QueryRow(`SELECT distinct_id,properties,timestamp,received_at FROM events WHERE uuid=?`, uuid).Scan(&e.DistinctID, &props, &e.Timestamp, &e.ReceivedAt); err != nil {
		t.Fatal(err)
	}
	e.Properties = json.RawMessage(props)
	return e
}

func cookielessKey(t *testing.T, s *Service) (string, []byte) {
	t.Helper()
	var day string
	var secret []byte
	if err := s.db.QueryRow(`SELECT day,secret FROM cookieless_keys`).Scan(&day, &secret); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM cookieless_keys`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("expected one daily key, got %d: %v", count, err)
	}
	return day, secret
}

func TestCookielessDailyIdentityAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	receipt := time.Date(2026, 11, 6, 23, 59, 59, 0, time.UTC)
	config := Config{DBPath: path, BootstrapToken: adminToken, Clock: func() time.Time { return receipt }}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	_, ingest := project(t, s)
	_, ingestB := project(t, s)
	for _, uuid := range []string{"first", "same"} {
		cookielessRequest(t, s, ingest, uuid, "192.0.2.73:1234", "browser/1", nil, 200)
	}
	original := storedCookieless(t, s, "first").DistinctID
	if !strings.HasPrefix(original, "cookieless_") || len(original) != len("cookieless_")+64 || storedCookieless(t, s, "same").DistinctID != original {
		t.Fatal("same-day pseudonym not stable SHA256 format")
	}
	day, secret := cookielessKey(t, s)
	if day != "2026-11-06" || len(secret) != 32 || bytes.Equal(secret, make([]byte, 32)) || bytes.Equal(secret, bytes.Repeat(secret[:1], 32)) {
		t.Fatal("daily key must contain 32 independently generated random bytes")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	cookielessRequest(t, s, ingest, "restart", "192.0.2.73:9999", "browser/1", nil, 200)
	_, restartSecret := cookielessKey(t, s)
	if storedCookieless(t, s, "restart").DistinctID != original || !bytes.Equal(secret, restartSecret) {
		t.Fatal("restart changed daily identity or secret")
	}
	for _, tc := range []struct{ uuid, token, peer, ua string }{
		{"different-ip", ingest, "192.0.2.74:1234", "browser/1"},
		{"different-ua", ingest, "192.0.2.73:1234", "browser/2"},
		{"different-project", ingestB, "192.0.2.73:1234", "browser/1"},
	} {
		cookielessRequest(t, s, tc.token, tc.uuid, tc.peer, tc.ua, nil, 200)
		if storedCookieless(t, s, tc.uuid).DistinctID == original {
			t.Fatalf("identity not scoped to %s", tc.uuid)
		}
	}
	cookielessRequest(t, s, ingest, "mapped", "[::ffff:192.0.2.73]:5678", "browser/1", nil, 200)
	if storedCookieless(t, s, "mapped").DistinctID != original {
		t.Fatal("mapped IPv4 did not canonicalize")
	}
	receipt = receipt.Add(time.Second)
	body := []byte(`{"api_key":"` + ingest + `","event":"visit","uuid":"next-day","distinct_id":"$posthog_cookieless","timestamp":"2020-01-01T00:00:00Z"}`)
	cookielessRawRequest(t, s, body, "192.0.2.73:1234", "browser/1", nil, 200)
	event := storedCookieless(t, s, "next-day")
	day, nextSecret := cookielessKey(t, s)
	if event.DistinctID == original || day != "2026-11-07" || bytes.Equal(secret, nextSecret) {
		t.Fatal("UTC boundary did not rotate independent secret and identity")
	}
	if event.Timestamp != "2020-01-01T00:00:00Z" || !strings.Contains(string(event.Properties), `"identity_day":"2026-11-07"`) {
		t.Fatal("event timestamp controlled identity day or was changed")
	}
	cookielessRequest(t, s, ingest, "first", "192.0.2.99:1234", "changed-UA", nil, 200)
	if storedCookieless(t, s, "first").DistinctID != original || eventCount(t, s) != 8 {
		t.Fatal("UUID retry across rotation changed original event")
	}
}

func TestCookielessProxyTrust(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "trusted"}[trusted], func(t *testing.T) {
			config := Config{DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken}
			if trusted {
				config.TrustedProxies = []string{"10.0.0.0/8", "2001:db8::1"}
			}
			s, err := Open(config)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			_, ingest := project(t, s)
			cookielessRequest(t, s, ingest, "direct", "192.0.2.73:1234", "ua", nil, 200)
			direct := storedCookieless(t, s, "direct").DistinctID
			cookielessRequest(t, s, ingest, "spoof", "192.0.2.73:1234", "ua", map[string]string{"X-Forwarded-For": "malformed", "X-Real-IP": "198.51.100.17", "Forwarded": "for=198.51.100.17"}, 200)
			if storedCookieless(t, s, "spoof").DistinctID != direct {
				t.Fatal("untrusted peer controlled identity through headers")
			}
			cookielessRequest(t, s, ingest, "chain", "10.0.0.1:1234", "ua", map[string]string{"X-Forwarded-For": "198.51.100.17, 192.0.2.73, 10.1.2.3"}, 200)
			if (storedCookieless(t, s, "chain").DistinctID == direct) != trusted {
				t.Fatal("proxy chain did not stop at first untrusted hop")
			}
			if trusted {
				cookielessRequest(t, s, ingest, "ipv6proxy", "[2001:db8::1]:1234", "ua", map[string]string{"X-Forwarded-For": "::ffff:192.0.2.73"}, 200)
				if storedCookieless(t, s, "ipv6proxy").DistinctID != direct {
					t.Fatal("exact IPv6 trusted proxy failed")
				}
				for _, chain := range []string{"", "garbage", "192.0.2.73,", "garbage, 192.0.2.73", strings.Repeat("10.1.2.3,", 16) + "192.0.2.73"} {
					cookielessRequest(t, s, ingest, "bad-chain", "10.0.0.1:1234", "ua", map[string]string{"X-Forwarded-For": chain}, 400)
				}
			}
		})
	}
	for _, value := range []string{"", "hostname", "10.0.0.1/999", "fe80::1%en0"} {
		if s, err := Open(Config{TrustedProxies: []string{value}}); err == nil {
			s.Close()
			t.Fatalf("invalid proxy config %q accepted", value)
		}
	}
}

func TestCookielessInvalidPeerCannotCreateIdentity(t *testing.T) {
	s := service(t, 20)
	_, ingest := project(t, s)
	for _, peer := range []string{"", "not-an-ip:80", "192.0.2.73", "[fe80::1%en0]:1234"} {
		cookielessRequest(t, s, ingest, "bad-peer", peer, "ua", nil, 400)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM cookieless_keys`).Scan(&count); err != nil || count != 0 || eventCount(t, s) != 0 {
		t.Fatal("bad peer created identity state", err)
	}
	cookielessRequest(t, s, ingest, "empty-ua", "192.0.2.73:1234", "", nil, 200)
}

func TestCookielessAtomicQueueAndRotation(t *testing.T) {
	s := service(t, 2)
	receipt := time.Date(2026, 11, 6, 23, 59, 59, 0, time.UTC)
	s.clock = func() time.Time { return receipt }
	_, ingest := project(t, s)
	cookielessRequest(t, s, ingest, "first", "192.0.2.73:1234", "ua", nil, 200)
	original := storedCookieless(t, s, "first")
	_, secret := cookielessKey(t, s)
	receipt = receipt.Add(time.Second)
	body := []byte(`{"api_key":"` + ingest + `","batch":[{"uuid":"second","event":"visit","distinct_id":"$posthog_cookieless"},{"uuid":"third","event":"visit","distinct_id":"$posthog_cookieless"}]}`)
	cookielessRawRequest(t, s, body, "192.0.2.73:1234", "ua", nil, 503)
	day, afterFailure := cookielessKey(t, s)
	if day != "2026-11-06" || !bytes.Equal(secret, afterFailure) || eventCount(t, s) != 1 {
		t.Fatal("failed batch committed salt rotation or partial events")
	}
	body = []byte(`{"api_key":"` + ingest + `","batch":[{"uuid":"first","event":"visit","distinct_id":"$posthog_cookieless"},{"uuid":"second","event":"visit","distinct_id":"$posthog_cookieless"}]}`)
	w := cookielessRawRequest(t, s, body, "192.0.2.73:1234", "ua", nil, 200)
	if !strings.Contains(w.Body.String(), `"duplicates":1`) || storedCookieless(t, s, "first").DistinctID != original.DistinctID || bytes.Equal(storedCookieless(t, s, "first").Properties, storedCookieless(t, s, "second").Properties) {
		t.Fatal("batch retry did not preserve old event and accept new daily identity")
	}
	day, afterSuccess := cookielessKey(t, s)
	if day != "2026-11-07" || bytes.Equal(secret, afterSuccess) {
		t.Fatal("successful transaction did not commit rotation")
	}
	cookielessRawRequest(t, s, body, "192.0.2.73:1234", "ua", nil, 200)
}

func TestCookielessKnownIdentityAndIPPropertiesNeverPersist(t *testing.T) {
	s := service(t, 20)
	id, ingest := project(t, s)
	props := map[string]any{"distinct_id": cookielessSentinel, "business": "retained", "$browser": "Firefox"}
	removed := []string{"$device_id", "$user_id", "$anon_distinct_id", "$session_id", "$window_id", "$ip", "ip", "ip_address", "client_ip", "remote_addr", "x-forwarded-for"}
	for _, field := range removed {
		props[field] = "192.0.2.73"
	}
	body, _ := json.Marshal(map[string]any{"api_key": ingest, "event": "visit", "uuid": "private", "properties": props})
	cookielessRawRequest(t, s, body, "192.0.2.73:1234", "Firefox", map[string]string{"X-Forwarded-For": "198.51.100.17"}, 200)
	event := storedCookieless(t, s, "private")
	var stored map[string]any
	if err := json.Unmarshal(event.Properties, &stored); err != nil {
		t.Fatal(err)
	}
	for _, field := range removed {
		if _, ok := stored[field]; ok {
			t.Fatalf("identity/IP property %q persisted", field)
		}
	}
	if stored["distinct_id"] != event.DistinctID || stored["cookieless"] != true || stored["business"] != "retained" || stored["$browser"] != "Firefox" {
		t.Fatal("wrong resolved properties", stored)
	}
	result := request(t, s, "GET", "/api/v1/projects/"+id+"/events", adminToken, "", 200)
	api, _ := json.Marshal(result)
	for _, raw := range []string{"192.0.2.73", "198.51.100.17", cookielessSentinel} {
		if bytes.Contains(api, []byte(raw)) {
			t.Fatalf("raw identity in events API: %s", raw)
		}
		files, err := filepath.Glob(s.dbPathForTest(t) + "*")
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range files {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte(raw)) {
				t.Fatalf("raw identity in SQLite file %s", filepath.Base(path))
			}
		}
	}
	body = []byte(`{"api_key":"` + ingest + `","event":"visit","uuid":"explicit","distinct_id":"known-user","properties":{"cookieless":true,"identity_day":"fake","$device_id":"device-1"}}`)
	cookielessRawRequest(t, s, body, "", "", nil, 200)
	explicit := storedCookieless(t, s, "explicit")
	if explicit.DistinctID != "known-user" || strings.Contains(string(explicit.Properties), "cookieless") || strings.Contains(string(explicit.Properties), "identity_day") || !strings.Contains(string(explicit.Properties), "device-1") {
		t.Fatal("explicit mode retained spoofed server markers or changed browser identity")
	}
}

func (s *Service) dbPathForTest(t *testing.T) string {
	t.Helper()
	var seq int
	var name, path string
	if err := s.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCookielessDelayedRequestUsesDayUnderWriteLock(t *testing.T) {
	s := service(t, 20)
	dayOne := time.Date(2026, 11, 6, 23, 59, 59, 0, time.UTC)
	dayTwo := dayOne.Add(time.Second)
	s.clock = func() time.Time { return dayOne }
	_, ingest := project(t, s)
	cookielessRequest(t, s, ingest, "before-midnight", "192.0.2.73:1234", "ua", nil, 200)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	s.clock = func() time.Time {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			return dayOne
		}
		return dayTwo
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		body := `{"api_key":"` + ingest + `","event":"visit","uuid":"delayed","distinct_id":"$posthog_cookieless"}`
		r := httptest.NewRequest("POST", "/capture/", strings.NewReader(body))
		r.RemoteAddr = "192.0.2.73:1234"
		r.Header.Set("User-Agent", "ua")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		done <- w
	}()
	<-entered
	// The newer request completes rotation while the older request is paused.
	cookielessRequest(t, s, ingest, "after-midnight", "192.0.2.73:1234", "ua", nil, 200)
	close(release)
	select {
	case w := <-done:
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delayed request did not finish")
	}
	delayed := storedCookieless(t, s, "delayed")
	newer := storedCookieless(t, s, "after-midnight")
	day, _ := cookielessKey(t, s)
	if delayed.DistinctID != newer.DistinctID || day != "2026-11-07" {
		t.Fatal("delayed request recreated expired key or split daily identity")
	}
	if delayed.ReceivedAt != dayOne.Format(time.RFC3339Nano) || !strings.Contains(string(delayed.Properties), `"identity_day":"2026-11-07"`) {
		t.Fatal("arrival time was confused with identity acceptance day")
	}
}

func TestCookielessClockRollbackRejectsExpiredDay(t *testing.T) {
	s := service(t, 20)
	receipt := time.Date(2026, 11, 6, 23, 59, 59, 0, time.UTC)
	s.clock = func() time.Time { return receipt }
	_, ingest := project(t, s)
	cookielessRequest(t, s, ingest, "day-one", "192.0.2.73:1234", "ua", nil, 200)
	receipt = receipt.Add(time.Second)
	cookielessRequest(t, s, ingest, "day-two", "192.0.2.73:1234", "ua", nil, 200)
	_, secret := cookielessKey(t, s)
	receipt = receipt.Add(-time.Second)
	cookielessRequest(t, s, ingest, "clock-rollback", "192.0.2.73:1234", "ua", nil, 503)
	day, retained := cookielessKey(t, s)
	if day != "2026-11-07" || !bytes.Equal(secret, retained) || eventCount(t, s) != 2 {
		t.Fatal("clock rollback recreated old secret or committed event")
	}
}

func TestCookielessBatchUsesOneProcessingDay(t *testing.T) {
	s := service(t, 20)
	dayOne := time.Date(2026, 11, 6, 23, 59, 59, 0, time.UTC)
	calls := 0
	s.clock = func() time.Time { calls++; return dayOne.Add(time.Duration(calls-1) * time.Second) }
	_, ingest := project(t, s)
	body := []byte(`{"api_key":"` + ingest + `","batch":[{"uuid":"batch-first","event":"visit","distinct_id":"$posthog_cookieless"},{"uuid":"batch-second","event":"visit","properties":{"distinct_id":"$posthog_cookieless"}},{"uuid":"batch-explicit","event":"visit","distinct_id":"known-user"}]}`)
	cookielessRawRequest(t, s, body, "192.0.2.73:1234", "ua", nil, 200)
	first, second := storedCookieless(t, s, "batch-first"), storedCookieless(t, s, "batch-second")
	if calls != 2 || first.DistinctID != second.DistinctID || !strings.Contains(string(first.Properties), `"identity_day":"2026-11-07"`) || storedCookieless(t, s, "batch-explicit").DistinctID != "known-user" {
		t.Fatal("batch did not use one processing day and shared daily key")
	}
}
