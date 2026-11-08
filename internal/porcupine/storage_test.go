package porcupine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestStorageConfigurationDefaultsAndBoundaries(t *testing.T) {
	config := ClickHouseConfig{StoragePolicy: "hot_cold"}
	if err := normalizeStorageConfig(&config); err != nil {
		t.Fatal(err)
	}
	if config.HotStorageDays != 30 || config.ColdVolume != "cold" {
		t.Fatal("missing enabled storage defaults")
	}
	for _, config := range []ClickHouseConfig{
		{StoragePolicy: "hot_cold", HotStorageDays: -1},
		{StoragePolicy: "hot_cold", HotStorageDays: 36501},
		{StoragePolicy: "hot_cold'; DELETE FROM events"},
		{StoragePolicy: "hot_cold", ColdVolume: "cold' DELETE"},
		{StoragePolicy: "hot-cold"},
	} {
		if validateStorageConfig(config) == nil {
			t.Errorf("invalid storage config accepted: %#v", config)
		}
	}
	for _, days := range []int{1, 36500} {
		if err := validateStorageConfig(ClickHouseConfig{StoragePolicy: "hot_cold", HotStorageDays: days, ColdVolume: "cold"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateStorageConfig(ClickHouseConfig{}); err != nil {
		t.Fatal("local default config rejected")
	}
}

func TestStorageMovesColdWithoutRepeatedAlter(t *testing.T) {
	policy := "default"
	create := "CREATE TABLE analytics.events (received_at_ms Int64) ENGINE=MergeTree ORDER BY received_at_ms SETTINGS index_granularity=8192"
	var queries []string
	ch := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := string(body)
		queries = append(queries, query)
		if r.Header.Get("X-ClickHouse-User") != "admin_user" || r.Header.Get("X-ClickHouse-Key") != "admin_secret" {
			t.Error("storage operation missing operator credentials")
		}
		switch {
		case strings.HasPrefix(query, "SELECT storage_policy,create_table_query FROM system.tables"):
			if !strings.Contains(query, "database=unhex('616e616c7974696373')") || !strings.Contains(query, "name=unhex('6576656e7473')") {
				t.Error("metadata query lacks exact trusted table")
			}
			json.NewEncoder(w).Encode(map[string]string{"storage_policy": policy, "create_table_query": create})
		case strings.Contains(query, "MODIFY SETTING storage_policy='hot_cold'"):
			policy = "hot_cold"
		case strings.Contains(query, "MODIFY TTL"):
			if strings.Contains(strings.ToUpper(query), "DELETE") || !strings.Contains(query, "TO VOLUME 'cold'") || !strings.Contains(query, "received_at_ms,'UTC'") || !strings.Contains(query, "materialize_ttl_after_modify=1") {
				t.Error("TTL must asynchronously move cold data without deleting")
			}
			create = "CREATE TABLE analytics.events (received_at_ms Int64) ENGINE=MergeTree ORDER BY received_at_ms TTL toDateTime(fromUnixTimestamp64Milli(received_at_ms, 'UTC')) + toIntervalDay(30) TO VOLUME 'cold' SETTINGS storage_policy='hot_cold'"
		default:
			t.Errorf("unexpected storage operation %s", query)
		}
	})
	ch.config.StoragePolicy = "hot_cold"
	if err := ch.applyStorage(context.Background(), "analytics.events"); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 3 {
		t.Fatalf("got %d first-time requests", len(queries))
	}
	if err := ch.applyStorage(context.Background(), "analytics.events"); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 4 {
		t.Fatal("unchanged configuration caused another ALTER")
	}
	ch.config.HotStorageDays = 60
	if err := ch.applyStorage(context.Background(), "analytics.events"); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 6 || !strings.Contains(queries[5], "INTERVAL 60 DAY TO VOLUME 'cold'") || strings.Contains(queries[5], "VOLUME 'hot'") {
		t.Fatal("window change failed or explicitly moved cold parts back to hot")
	}
}

func TestStorageTTLComparison(t *testing.T) {
	for _, query := range []string{
		"CREATE TABLE analytics.events (received_at_ms Int64) ENGINE=MergeTree ORDER BY received_at_ms TTL toDateTime(fromUnixTimestamp64Milli(received_at_ms, 'UTC')) + toIntervalDay(30) TO VOLUME 'cold' SETTINGS storage_policy='hot_cold'",
		"CREATE TABLE analytics.events (received_at_ms Int64) ENGINE=MergeTree ORDER BY received_at_ms TTL toDateTime(fromUnixTimestamp64Milli(received_at_ms,'UTC')) + INTERVAL 30 DAY TO VOLUME 'cold'",
		"CREATE TABLE analytics.events (received_at_ms Int64) ENGINE=MergeTree ORDER BY received_at_ms\nTTL toDateTime(fromUnixTimestamp64Milli(`received_at_ms`, 'UTC')) + toIntervalDay(30) TO VOLUME 'cold'\nSETTINGS index_granularity=8192",
	} {
		if !storageTTLMatches(query, 30, "cold") {
			t.Errorf("equivalent canonical TTL not recognized: %s", query)
		}
		if storageTTLMatches(query, 31, "cold") || storageTTLMatches(query, 30, "Cold") {
			t.Error("different policy erroneously matched")
		}
	}
	for _, ttl := range []string{
		"toDateTime(fromUnixTimestamp64Milli(received_at_ms,'UTC')) + INTERVAL 30 DAY DELETE",
		"toDateTime(fromUnixTimestamp64Milli(received_at_ms,'UTC')) + INTERVAL 30 DAY TO VOLUME 'cold', toDateTime(fromUnixTimestamp64Milli(received_at_ms,'UTC')) + INTERVAL 60 DAY DELETE",
		"toDateTime(fromUnixTimestamp64Milli(timestamp_ms,'UTC')) + INTERVAL 30 DAY TO VOLUME 'cold'",
	} {
		if storageTTLMatches("CREATE TABLE analytics.events (received_at_ms Int64) TTL "+ttl, 30, "cold") {
			t.Fatal("unsafe or different TTL treated as equivalent")
		}
	}
}

func TestStorageDisabledAndUnsafeIdentifiersSendNoSQL(t *testing.T) {
	calls := 0
	ch := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) { calls++ })
	if err := ch.applyStorage(context.Background(), "analytics.events"); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("local-only deployment made storage requests")
	}
	ch.config.StoragePolicy = "hot_cold"
	for _, table := range []string{"events", "analytics.events; DELETE FROM events", "analytics.events.other", "analytics.events WHERE 1", "analytics."} {
		if err := ch.applyStorage(context.Background(), table); err == nil {
			t.Errorf("unsafe table accepted: %s", table)
		}
	}
	ch.config.ColdVolume = "bad'volume"
	if err := ch.applyStorage(context.Background(), "analytics.events"); err == nil {
		t.Fatal("unsafe volume accepted")
	}
	if calls != 0 {
		t.Fatal("invalid configuration reached ClickHouse")
	}
}

func TestStorageInspectionFailuresDoNotAlter(t *testing.T) {
	for _, response := range []string{"", `not json`, `{"storage_policy":"default"}`, `{"storage_policy":"default","create_table_query":""}`} {
		calls := 0
		ch := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) { calls++; io.WriteString(w, response) })
		ch.config.StoragePolicy = "hot_cold"
		if err := ch.applyStorage(context.Background(), "analytics.events"); err == nil {
			t.Fatal("invalid metadata accepted")
		}
		if calls != 1 {
			t.Fatal("ALTER ran after failed inspection")
		}
	}
}
