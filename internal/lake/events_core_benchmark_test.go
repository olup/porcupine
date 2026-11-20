package lake

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
)

const eventsCoreBenchmarkEnv = "PORCUPINE_TEST_EVENTS_CORE_BENCH_BASELINE"
const eventsCoreBenchmarkSample = "events_core_bench_sample "

var eventsCoreBenchmarkOperations = []string{"counts", "daily_uniques", "funnel", "person_id", "profile", "list_events"}

type eventsCoreBenchmarkInput struct {
	Config    DuckLakeConfig
	ProjectID string
	Operation string
}

type eventsCoreBenchmarkResult struct {
	ElapsedUS int64
	Requests  int64
	GetBytes  int64
	Digest    string
}

// Run with scripts/events-core-benchmark.sh. Both versions read the same
// PostgreSQL catalog and S3 Parquet files through their original HostedReader.
func TestDuckLakeS3SharedPostgresEventsCorePerformance(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	baseline := os.Getenv(eventsCoreBenchmarkEnv)
	if baseline == "" {
		t.Skip("run scripts/events-core-benchmark.sh for the paired measurement")
	}
	candidate, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fixture := nativeS3Fixture(t)
	config := DuckLakeConfig{PostgresURL: sharedPostgresDSN(t), SharedSourceID: event.NewID(), S3: &fixture.storage,
		MemoryLimitMB: 8192, Threads: 4, MaxOpenProjects: 1, DisableCleanup: true}
	for _, count := range []int{1_000_000, 10_000_000} {
		t.Run(fmt.Sprintf("events_%d", count), func(t *testing.T) {
			key := ProjectWriteKey{config.SharedSourceID, fmt.Sprintf("events-core-bench-%d", count)}
			files, parquetBytes := seedEventCatalogBenchmark(t, config, key, count, true)
			t.Logf("events_core_bench_fixture events=%d links=1000000 profiles=300000 names=80 parquet_files=%d parquet_bytes=%d threads=%d memory_mib=%d",
				count, files, parquetBytes, config.Threads, config.MemoryLimitMB)
			for _, operation := range eventsCoreBenchmarkOperations {
				input := eventsCoreBenchmarkInput{config, key.ProjectID, operation}
				var digest string
				for sample := 0; sample < 4; sample++ {
					// Give each version equal first access to the Silo disk cache.
					for _, version := range []int{sample % 2, (sample + 1) % 2} {
						measurements := measureEventsCoreBenchmark(t, []string{baseline, candidate}[version], input)
						for mode, result := range measurements {
							t.Logf("events_core_bench events=%d op=%s version=%s cache=%s sample=%d elapsed_us=%d s3_requests=%d s3_get_response_bytes=%d",
								count, operation, []string{"before", "after"}[version], []string{"cold", "warm"}[mode], sample+1,
								result.ElapsedUS, result.Requests, result.GetBytes)
							if digest == "" {
								digest = result.Digest
							}
							if result.Digest != digest {
								t.Fatalf("%s produced different results on the same data", operation)
							}
						}
					}
				}
			}
		})
	}
}

func measureEventsCoreBenchmark(t *testing.T, binary string, input eventsCoreBenchmarkInput) []eventsCoreBenchmarkResult {
	t.Helper()
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	run := exec.CommandContext(t.Context(), binary, "-test.run=^TestEventsCoreBenchmarkSample$", "-test.v")
	run.Env = append(os.Environ(), "PORCUPINE_TEST_EVENTS_CORE_BENCH_CHILD=1")
	run.Stdin = bytes.NewReader(data)
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("measure %s: %v\n%s", input.Operation, err, output)
	}
	var results []eventsCoreBenchmarkResult
	for line := range strings.SplitSeq(string(output), "\n") {
		if _, raw, found := strings.Cut(line, eventsCoreBenchmarkSample); found {
			var result eventsCoreBenchmarkResult
			if err := json.Unmarshal([]byte(raw), &result); err != nil {
				t.Fatal(err)
			}
			results = append(results, result)
		}
	}
	if len(results) != 2 || results[0].Digest == "" || results[1].Digest == "" {
		t.Fatalf("expected cold and warm results for %s, got %d\n%s", input.Operation, len(results), output)
	}
	return results
}

func TestEventsCoreBenchmarkSample(t *testing.T) {
	t.Parallel()
	if os.Getenv("PORCUPINE_TEST_EVENTS_CORE_BENCH_CHILD") != "1" {
		t.Skip("invoked by the paired measurement")
	}
	var input eventsCoreBenchmarkInput
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		t.Fatal(err)
	}
	requests, getBytes := countBenchmarkS3(t, input.Config.S3)
	key := ProjectWriteKey{input.Config.SharedSourceID, input.ProjectID}
	withCatalogBenchmarkReader(t, input.Config, key, func(reader *HostedReader, _ *ProjectLake) {
		// Cold starts with a fresh DuckDB process, after attaching the project.
		// Warm repeats the same operation on the same instance.
		for range 2 {
			beforeRequests, beforeBytes := requests.Load(), getBytes.Load()
			start := time.Now()
			result := runEventsCoreBenchmarkOperation(t, reader, input)
			measurement := eventsCoreBenchmarkResult{ElapsedUS: time.Since(start).Microseconds(),
				Requests: requests.Load() - beforeRequests, GetBytes: getBytes.Load() - beforeBytes}
			if queryResult, ok := result.(AnalyticsResult); ok {
				queryResult.Statistics = AnalyticsStatistics{}
				if input.Operation == "counts" {
					// GROUP BY without ORDER BY does not specify a row order.
					slices.SortFunc(queryResult.Data, func(a, b map[string]json.RawMessage) int {
						return bytes.Compare(a["event"], b["event"])
					})
				}
				result = queryResult
			}
			body, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			measurement.Digest = fmt.Sprintf("%x", sha256.Sum256(body))
			data, err := json.Marshal(measurement)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s%s", eventsCoreBenchmarkSample, data)
		}
	})
}

func runEventsCoreBenchmarkOperation(t *testing.T, reader *HostedReader, input eventsCoreBenchmarkInput) any {
	t.Helper()
	ctx, project := t.Context(), input.ProjectID
	asOf := time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)
	limits := DefaultQueryExecutionLimits()
	limits.Timeout = time.Minute
	var query string
	var parameters map[string]string
	switch input.Operation {
	case "counts":
		query = `SELECT event, count(*) FROM events GROUP BY 1`
	case "daily_uniques":
		query = `SELECT date_trunc('day', event_time) AS day, count(DISTINCT distinct_id) AS people
 FROM events WHERE event=$event GROUP BY 1 ORDER BY 1`
		parameters = map[string]string{"event": catalogBenchEvent}
		limits.TimeRange = &QueryTimeRange{From: asOf.AddDate(0, 0, -30), To: asOf}
	case "funnel":
		query = `WITH steps AS (
 SELECT distinct_id, min(event_time) FILTER (WHERE event='event_00') AS started,
  max(event_time) FILTER (WHERE event='event_01') AS finished
 FROM events WHERE event IN ('event_00','event_01') GROUP BY distinct_id
) SELECT count(started) AS starts, count(*) FILTER (WHERE finished>started) AS conversions FROM steps`
	case "person_id":
		query = `SELECT count(DISTINCT person_id) AS people FROM events WHERE event=$event`
		parameters = map[string]string{"event": catalogBenchEvent}
	case "profile":
		options, err := NormalizeEventProfileInput(EventProfileInput{Event: catalogBenchEvent, ExampleLimit: 3})
		if err != nil {
			t.Fatal(err)
		}
		read, err := reader.Profile(ctx, project, options, asOf)
		if err != nil {
			t.Fatal(err)
		}
		profile, err := SummarizeEventProfile(read, options, asOf)
		if err != nil {
			t.Fatal(err)
		}
		return profile
	case "list_events":
		events, err := reader.ListEvents(ctx, project, 100, AnalyticsMaxBytes)
		if err != nil {
			t.Fatal(err)
		}
		return events
	default:
		t.Fatalf("unknown operation %q", input.Operation)
	}
	result, err := reader.Query(ctx, project, query, parameters, limits)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
