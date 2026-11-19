package lake

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
)

const catalogBenchEvent = "event_00"

// Run against the disposable PostgreSQL and loopback Silo fixtures:
// PORCUPINE_TEST_CATALOG_BENCH=1 CGO_ENABLED=0 go run ./internal/cmd/shared-postgres-integration -- go run ./internal/cmd/ducklake-s3-integration -run '^TestDuckLakeS3SharedPostgresEventCatalogPerformance$' -race=false -timeout=20m
func TestDuckLakeS3SharedPostgresEventCatalogPerformance(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	if os.Getenv("PORCUPINE_TEST_CATALOG_BENCH") != "1" {
		t.Skip("enable the opt-in event catalog benchmark")
	}
	fixture, requests, getBytes := countedNativeProofS3(t)
	// The 10M legacy profile ranks wide rows for the hot event. Keep both
	// versions within the same memory budget and the store's spill limit.
	config := DuckLakeConfig{PostgresURL: sharedPostgresDSN(t), SharedSourceID: event.NewID(), S3: &fixture.storage,
		MemoryLimitMB: 3072, Threads: 4, MaxOpenProjects: 1, DisableCleanup: true}
	for _, count := range []int{1_000_000, 10_000_000} {
		t.Run(fmt.Sprintf("events_%d", count), func(t *testing.T) {
			key := ProjectWriteKey{config.SharedSourceID, fmt.Sprintf("event-catalog-bench-%d", count)}
			files, parquetBytes := seedEventCatalogBenchmark(t, config, key, count)
			t.Logf("catalog_bench_fixture events=%d names=80 parquet_files=%d parquet_bytes=%d threads=%d memory_mib=%d",
				count, files, parquetBytes, config.Threads, config.MemoryLimitMB)

			type operation struct {
				name string
				run  func(*HostedReader, *ProjectLake, bool) any
			}
			catalog := func(name, exactEvent string) operation {
				return operation{name, func(reader *HostedReader, _ *ProjectLake, narrow bool) any {
					input := EventCatalogInput{ID: key.ProjectID, Event: exactEvent}
					if !narrow {
						return legacyDuckLakeCatalog(t, reader.lease, input)
					}
					result, err := reader.Catalog(t.Context(), input)
					if err != nil {
						t.Fatal(err)
					}
					return result
				}}
			}
			profile := func(name string, activity bool) operation {
				return operation{name, func(_ *HostedReader, p *ProjectLake, narrow bool) any {
					return runCatalogBenchmarkProfile(t, p, activity, narrow)
				}}
			}
			for _, op := range []operation{
				catalog("catalog", ""), catalog("catalog_event", catalogBenchEvent),
				profile("profile_sample", false), profile("profile", true),
			} {
				for sample := 1; sample <= 3; sample++ {
					var results [2][2]any
					// Alternate first access to reduce bias from warming the fixture's disk cache.
					for _, version := range []int{(sample - 1) % 2, sample % 2} {
						withCatalogBenchmarkReader(t, config, key, func(reader *HostedReader, p *ProjectLake) {
							// Cold means a fresh DuckDB instance, after the project has attached.
							// Warm repeats the identical request on that instance.
							for mode, cache := range []string{"cold", "warm"} {
								beforeRequests, beforeBytes := requests.Load(), getBytes.Load()
								start := time.Now()
								results[version][mode] = op.run(reader, p, version == 1)
								t.Logf("catalog_bench events=%d op=%s version=%s cache=%s sample=%d elapsed_us=%d s3_requests=%d s3_get_response_bytes=%d",
									count, op.name, []string{"before", "after"}[version], cache, sample, time.Since(start).Microseconds(),
									requests.Load()-beforeRequests, getBytes.Load()-beforeBytes)
							}
						})
					}
					if !reflect.DeepEqual(results[0], results[1]) || !reflect.DeepEqual(results[0][0], results[0][1]) {
						t.Fatalf("%s sample %d produced different results on the same data", op.name, sample)
					}
				}
			}
			if count == 10_000_000 {
				withCatalogBenchmarkReader(t, config, key, func(_ *HostedReader, p *ProjectLake) {
					detailQuery, ids := measureEventCatalogBenchmarkStages(t, p, requests, getBytes)
					explainEventCatalogBenchmark(t, p, detailQuery, ids)
				})
			}
		})
	}
}

func openEventCatalogBenchmark(t *testing.T, config DuckLakeConfig) *DuckLakeStore {
	t.Helper()
	config.Directory = t.TempDir()
	store, err := NewDuckLake(config)
	if err != nil {
		t.Fatal(err)
	}
	return store.(*DuckLakeStore)
}

func closeEventCatalogBenchmark(t *testing.T, store *DuckLakeStore) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Error(err)
	}
}

func withCatalogBenchmarkReader(t *testing.T, config DuckLakeConfig, key ProjectWriteKey, run func(*HostedReader, *ProjectLake)) {
	t.Helper()
	store := openEventCatalogBenchmark(t, config)
	defer closeEventCatalogBenchmark(t, store)
	reader, err := NewHostedReader(store)
	if err != nil {
		t.Fatal(err)
	}
	p, release, err := reader.lease(t.Context(), key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// The old wide sort exceeded the runtime spill cap at 10M events.
	if _, err := p.DB.ExecContext(t.Context(), "SET max_temp_directory_size='4GiB'"); err != nil {
		t.Fatal(err)
	}
	run(reader, p)
}

func seedEventCatalogBenchmark(t *testing.T, config DuckLakeConfig, key ProjectWriteKey, count int) (int, int64) {
	t.Helper()
	ctx := t.Context()
	store := openEventCatalogBenchmark(t, config)
	defer closeEventCatalogBenchmark(t, store)
	if _, err := store.InitializeHostedProject(ctx, key); err != nil {
		t.Fatal(err)
	}
	p, release, err := store.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := p.DB.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO lake.identity_profiles (identified_id,version,traits,is_identified)
 SELECT 'person-' || i,1,'{"plan":"standard"}',true FROM range(25000) d(i)`)
	exec(`INSERT INTO lake.identity_links (anonymous_id,identified_id,version)
 SELECT 'anon-' || i,'person-' || i,1 FROM range(25000) d(i)`)
	// Each batch spans a disjoint arrival-ID range. A few old imports write to
	// older occurrence-month partitions and keep event time distinct from ID order.
	const batch = 250_000
	for start := 1; start <= count; start += batch {
		exec(`INSERT INTO lake.current_events (id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,event_time,
 received_at,received_at_ms,revision,source_id,message_id,event_type,context,is_imported,identity_snapshot,original_snapshot)
WITH seeded AS (
 SELECT id, hash(id)%100 AS popularity, hash(id,42)%78 AS rare,
  TIMESTAMPTZ '2026-01-01T00:00:00Z' + id * INTERVAL '1 second' AS receipt,
  id%997=0 AS imported
 FROM range(?::BIGINT,?::BIGINT) d(id)
), events_with_times AS (
 SELECT *, receipt - (hash(id,17)%1800)::BIGINT * INTERVAL '1 second' - CASE WHEN imported THEN INTERVAL '45 days' ELSE INTERVAL '0 seconds' END AS occurrence
 FROM seeded
)
SELECT id,?,CAST(uuid() AS VARCHAR),CASE WHEN popularity<45 THEN 'event_00' WHEN popularity<65 THEN 'event_01' ELSE printf('event_%02d',2+rare) END,
 'anon-' || (id%50000),CAST(to_json(struct_pack(plan:=if(id%5=0,'pro','standard'),amount:=id%10000/100.0,
 quantity:=1+id%10,new_customer:=id%13=0,session_id:=md5(id::VARCHAR),screen:=if(id%3=0,'checkout','product'))) AS VARCHAR),
 strftime(occurrence,'%Y-%m-%dT%H:%M:%SZ'),epoch_ms(occurrence),occurrence,
 strftime(receipt,'%Y-%m-%dT%H:%M:%SZ'),epoch_ms(receipt),0,'benchmark-source','','track',
 CAST(to_json(struct_pack(request_id:=md5('request-' || id),user_agent:=if(id%3=0,'mobile','desktop'),campaign:=id%1111)) AS VARCHAR),
 imported::UTINYINT,CASE WHEN id%1009=0 THEN '{}' ELSE '' END,''
FROM events_with_times ORDER BY id`, start, min(start+batch, count+1), key.ProjectID)
	}
	var files int
	var parquetBytes int64
	if err := p.DB.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(data_file_size_bytes),0) FROM ducklake_list_files('lake','current_events')`).Scan(&files, &parquetBytes); err != nil || files < 2 || parquetBytes <= 0 {
		t.Fatalf("expected multiple event files in S3, got %d files and %d bytes: %v", files, parquetBytes, err)
	}
	return files, parquetBytes
}

func catalogBenchmarkProfileQuery(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, narrow bool) (string, []any) {
	t.Helper()
	const limit = 1000
	query, args := legacyDuckLakeProfileSampleSQL, []any{catalogBenchEvent, limit}
	if narrow {
		// The inclusive range contains every selected ID. Re-applying the same
		// order and limit inside that range yields the same sample.
		var low, high int64
		if err := q.QueryRowContext(t.Context(), duckLakeProfileSampleBoundsSQL, catalogBenchEvent, limit).Scan(&low, &high); err != nil {
			t.Fatal(err)
		}
		query, args = duckLakeProfileSampleSQL, []any{catalogBenchEvent, low, high, limit}
	}
	return query, append(args, EventProfileFieldBytes, 3, EventProfileFieldBytes,
		EventProfileSampleBytes, EventProfileSampleBytes, eventProfileMetadataBytes)
}

func runCatalogBenchmarkProfile(t *testing.T, p *ProjectLake, activity, narrow bool) [][]string {
	t.Helper()
	ctx := t.Context()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var result [][]string
	if activity {
		asOf := time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC)
		result = catalogBenchmarkRows(t, tx, duckLakeProfileActivitySQL,
			asOf.Add(-24*time.Hour).UnixMilli(), asOf.UnixMilli(), asOf.Add(-7*24*time.Hour).UnixMilli(),
			asOf.UnixMilli(), asOf.UnixMilli(), catalogBenchEvent)
	}
	query, args := catalogBenchmarkProfileQuery(t, tx, narrow)
	sample := catalogBenchmarkRows(t, tx, query, args...)
	if len(sample) == 0 {
		t.Fatal("profile returned an empty sample")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return append(result, sample...)
}

func catalogBenchmarkRows(t *testing.T, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, args ...any) [][]string {
	t.Helper()
	rows, err := q.QueryContext(t.Context(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]string
	for rows.Next() {
		row := make([]string, len(columns))
		fields := make([]any, len(row))
		for i := range row {
			fields[i] = &row[i]
		}
		if err := rows.Scan(fields...); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

const catalogBenchmarkAggregateSQL = duckLakeCatalogAggregateSQL + " GROUP BY event ORDER BY max(id) DESC LIMIT ? OFFSET ?"

func measureEventCatalogBenchmarkStages(t *testing.T, p *ProjectLake, requests, getBytes *atomic.Int64) (string, []any) {
	t.Helper()
	tx, err := p.DB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	measure := func(stage, query string, args ...any) [][]string {
		t.Helper()
		beforeRequests, beforeBytes := requests.Load(), getBytes.Load()
		start := time.Now()
		rows := catalogBenchmarkRows(t, tx, query, args...)
		t.Logf("catalog_bench_stage op=%s elapsed_us=%d s3_requests=%d s3_get_response_bytes=%d",
			stage, time.Since(start).Microseconds(), requests.Load()-beforeRequests, getBytes.Load()-beforeBytes)
		return rows
	}
	var ids []any
	for _, row := range measure("catalog_after_aggregate", catalogBenchmarkAggregateSQL, 21, 0) {
		for _, raw := range row[2:] {
			id, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
	}
	detailQuery := duckLakeCatalogDetailsSQL + strings.Repeat(",?", len(ids)-1) + ") GROUP BY event"
	measure("catalog_after_details", detailQuery, ids...)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return detailQuery, ids
}

func explainEventCatalogBenchmark(t *testing.T, p *ProjectLake, detailQuery string, ids []any) {
	t.Helper()
	explain := func(name, query string, args ...any) {
		t.Helper()
		for _, row := range catalogBenchmarkRows(t, p.DB, "EXPLAIN ANALYZE "+query, args...) {
			t.Logf("catalog_bench_plan op=%s\n%s", name, row[1])
		}
	}
	explain("catalog_before", `SELECT event,CAST(count(*) AS VARCHAR),arg_min(received_at,id),arg_max(received_at,id),
 arg_max(uuid,id),arg_max(timestamp,id) FROM events GROUP BY event ORDER BY max(id) DESC LIMIT ? OFFSET ?`, 21, 0)
	explain("catalog_after_aggregate", catalogBenchmarkAggregateSQL, 21, 0)
	explain("catalog_after_details", detailQuery, ids...)
	explain("catalog_event_before_sample", `SELECT uuid,received_at,properties FROM events WHERE event=? ORDER BY id DESC LIMIT 1000`, catalogBenchEvent)
	explain("catalog_event_after_ids", duckLakeCatalogSampleBoundsSQL, catalogBenchEvent)
	var low, high int64
	if err := p.DB.QueryRowContext(t.Context(), duckLakeCatalogSampleBoundsSQL, catalogBenchEvent).Scan(&low, &high); err != nil {
		t.Fatal(err)
	}
	explain("catalog_event_after_sample", duckLakeCatalogSampleSQL, catalogBenchEvent, low, high)
	profileQuery, profileArgs := catalogBenchmarkProfileQuery(t, p.DB, false)
	explain("profile_before_sample", profileQuery, profileArgs...)
	explain("profile_after_ids", duckLakeProfileSampleBoundsSQL, catalogBenchEvent, 1000)
	profileQuery, profileArgs = catalogBenchmarkProfileQuery(t, p.DB, true)
	explain("profile_after_sample", profileQuery, profileArgs...)
}
