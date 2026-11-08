package porcupine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type migrationServer struct {
	mu                                sync.Mutex
	legacy, current, marked           bool
	legacyUUID, verifiedUUID          string
	copyCount, dropCount, verifyCount int
	mismatch                          bool
	loseMarkerAck                     bool
	failCopy                          bool
	statements                        []string
}

func migrationClient(t *testing.T, state *migrationServer) *clickHouse {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sql := strings.TrimPrefix(string(body), migrationPrefix)
		state.mu.Lock()
		defer state.mu.Unlock()
		state.statements = append(state.statements, sql)
		if r.Header.Get("X-ClickHouse-User") != "admin_user" || r.Header.Get("X-ClickHouse-Key") != "admin_secret" {
			t.Error("migration used unexpected credential")
		}
		switch {
		case strings.HasPrefix(sql, "SELECT legacy_uuid,version"):
			if state.marked {
				json.NewEncoder(w).Encode(map[string]string{"legacy_uuid": state.verifiedUUID, "version": "2"})
			}
		case strings.HasPrefix(sql, "SELECT name,toString(uuid)"):
			if state.legacy {
				json.NewEncoder(w).Encode(map[string]string{"name": "events", "uuid": state.legacyUUID})
			}
			if state.current {
				json.NewEncoder(w).Encode(map[string]string{"name": "events_v2", "uuid": "current-uuid"})
			}
		case strings.HasPrefix(sql, "CREATE TABLE IF NOT EXISTS analytics.events_v2"):
			state.current = true
		case strings.HasPrefix(sql, "INSERT INTO analytics.events_v2"):
			state.copyCount++
			if state.failCopy {
				w.WriteHeader(503)
			}
		case strings.HasPrefix(sql, "SELECT count() AS mismatches"):
			state.verifyCount++
			count := "0"
			if state.mismatch {
				count = "1"
			}
			json.NewEncoder(w).Encode(map[string]string{"mismatches": count})
		case strings.HasPrefix(sql, "INSERT INTO analytics.schema_state"):
			state.marked = true
			state.verifiedUUID = state.legacyUUID
			if state.loseMarkerAck {
				state.loseMarkerAck = false
				w.WriteHeader(503)
			}
		case strings.HasPrefix(sql, "DROP TABLE IF EXISTS analytics.events SYNC"):
			state.dropCount++
			state.legacy = false
		}
	}))
	t.Cleanup(server.Close)
	c, err := newClickHouse(ClickHouseConfig{URL: server.URL, Database: "analytics", User: "admin_user", Password: "admin_secret"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.client.CloseIdleConnections)
	return c
}

func TestMigrationCopiesVerifiesMarksAndRetiresOnce(t *testing.T) {
	state := &migrationServer{legacy: true, legacyUUID: "legacy-uuid"}
	c := migrationClient(t, state)
	for range 2 {
		if err := c.initialize(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.current || !state.marked || state.legacy || state.copyCount != 1 || state.verifyCount != 2 || state.dropCount != 1 {
		t.Fatalf("unexpected migration state: %+v", state)
	}
	joined := strings.Join(state.statements, "\n")
	for _, required := range []string{"ReplacingMergeTree(revision,is_deleted)", "PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(received_at_ms,'UTC'))", "allow_experimental_replacing_merge_with_cleanup=0", "toUInt64(0),toUInt8(0)", "FROM analytics.events FINAL", "source.properties!=projected.properties", "source.timestamp_ms!=projected.timestamp_ms", "JSONExtractRaw(projected.original_snapshot,'properties')!=source.properties", "WHERE revision=0", "unhex('" + hex.EncodeToString([]byte("legacy-uuid")) + "')"} {
		if !strings.Contains(joined, required) {
			t.Errorf("migration invariant missing: %s", required)
		}
	}
	copyIndex, verifyIndex, markerIndex, dropIndex := -1, -1, -1, -1
	for i, sql := range state.statements {
		switch {
		case strings.HasPrefix(sql, "INSERT INTO analytics.events_v2"):
			copyIndex = i
		case strings.HasPrefix(sql, "SELECT count() AS mismatches"):
			verifyIndex = i
		case strings.HasPrefix(sql, "INSERT INTO analytics.schema_state"):
			markerIndex = i
		case strings.HasPrefix(sql, "DROP TABLE"):
			dropIndex = i
		}
	}
	if !(copyIndex < verifyIndex && verifyIndex < markerIndex && markerIndex < dropIndex) {
		t.Fatal("source retirement preceded verification or durable marker")
	}
}

func TestMigrationVerificationAndCopyFailurePreserveSource(t *testing.T) {
	for _, failCopy := range []bool{false, true} {
		state := &migrationServer{legacy: true, legacyUUID: "legacy-uuid", mismatch: !failCopy, failCopy: failCopy}
		c := migrationClient(t, state)
		if err := c.initialize(context.Background()); err == nil {
			t.Fatal("invalid migration succeeded")
		}
		state.mu.Lock()
		if !state.legacy || state.marked || state.dropCount != 0 {
			t.Fatal("unverified source retired")
		}
		state.mismatch = false
		state.failCopy = false
		state.mu.Unlock()
		if err := c.initialize(context.Background()); err != nil {
			t.Fatal(err)
		}
		state.mu.Lock()
		if state.copyCount != 2 || state.dropCount != 1 || !state.marked {
			t.Fatal("partial migration retry did not converge")
		}
		state.mu.Unlock()
	}
}

func TestMigrationLostMarkerAckNeverRecopies(t *testing.T) {
	state := &migrationServer{legacy: true, legacyUUID: "legacy-uuid", loseMarkerAck: true}
	c := migrationClient(t, state)
	if err := c.initialize(context.Background()); err == nil {
		t.Fatal("lost marker acknowledgment ignored")
	}
	state.mu.Lock()
	if !state.marked || !state.legacy || state.copyCount != 1 || state.dropCount != 0 {
		t.Fatal("unexpected pre-restart state")
	}
	state.mu.Unlock()
	// A fresh client models losing all process-local migration state.
	restarted := migrationClient(t, state)
	if err := restarted.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.copyCount != 1 || state.dropCount != 1 || state.legacy {
		t.Fatal("completed migration recopied the legacy source")
	}
}

func TestMigrationCompletedMarkerFailsClosedForMissingV2OrNewSource(t *testing.T) {
	for _, state := range []*migrationServer{
		{marked: true, current: false, verifiedUUID: "legacy-uuid"},
		{marked: true, current: true, legacy: true, legacyUUID: "unverified-new-uuid", verifiedUUID: "legacy-uuid"},
	} {
		c := migrationClient(t, state)
		if err := c.initialize(context.Background()); err == nil {
			t.Fatal("unsafe completed migration state accepted")
		}
		state.mu.Lock()
		if state.copyCount != 0 || state.dropCount != 0 {
			t.Fatal("completed migration imported or dropped an unverified source")
		}
		state.mu.Unlock()
	}
}

func TestMigrationFreshDatabaseDoesNotInventLegacyData(t *testing.T) {
	state := &migrationServer{}
	c := migrationClient(t, state)
	if err := c.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.current || !state.marked || state.copyCount != 0 || state.dropCount != 0 || state.verifyCount != 0 {
		t.Fatal("fresh initialization behaved like a legacy migration")
	}
}

func TestMigrationRefusesUnidentifiableLegacyTable(t *testing.T) {
	state := &migrationServer{legacy: true, legacyUUID: "00000000-0000-0000-0000-000000000000"}
	c := migrationClient(t, state)
	if err := c.initialize(context.Background()); err == nil {
		t.Fatal("legacy source without stable table identity accepted")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.copyCount != 0 || state.dropCount != 0 || state.marked {
		t.Fatal("unidentifiable legacy source was copied or retired")
	}
}
