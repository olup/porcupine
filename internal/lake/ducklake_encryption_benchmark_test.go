//go:build darwin || linux

package lake

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
)

const (
	encryptionBenchmarkProject = "parquet-encryption-benchmark"
	encryptionBenchmarkEvent   = "checkout"
	encryptionBenchmarkBatches = 4
	encryptionBenchmarkDays    = 14
)

// PORCUPINE_TEST_ENCRYPTION_BENCH=1 CGO_ENABLED=1 go test ./internal/lake -run '^TestDuckLakeParquetEncryptionBenchmark$' -v -count=1
// Set PORCUPINE_TEST_ENCRYPTION_BENCH_ROWS to measure one custom event count.
// The flush measures the hosted native head lookup and canonical event write
// through transaction commit, with data inlining disabled. It uses a local
// catalog and local Parquet and excludes hosted coordination and effects.
func TestDuckLakeParquetEncryptionBenchmark(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	if os.Getenv("PORCUPINE_TEST_ENCRYPTION_BENCH") != "1" {
		t.Skip("enable the opt-in Parquet encryption benchmark")
	}
	counts := []int{4_000, 40_000}
	if raw := os.Getenv("PORCUPINE_TEST_ENCRYPTION_BENCH_ROWS"); raw != "" {
		count, err := strconv.Atoi(raw)
		if err != nil || count < encryptionBenchmarkDays || count%encryptionBenchmarkBatches != 0 {
			t.Fatal("PORCUPINE_TEST_ENCRYPTION_BENCH_ROWS must be a multiple of four and at least 16")
		}
		counts = []int{count}
	}
	for _, count := range counts {
		t.Run(fmt.Sprintf("events_%d", count), func(t *testing.T) {
			batches, asOf := encryptionBenchmarkData(t, count)
			for sample := 1; sample <= 3; sample++ {
				var results [2]encryptionBenchmarkReads
				for _, mode := range []int{(sample - 1) % 2, sample % 2} {
					results[mode] = runEncryptionBenchmark(t, batches, asOf, mode == 1, sample)
				}
				if !reflect.DeepEqual(results[0], results[1]) {
					t.Fatalf("plain and encrypted reads differed for sample %d", sample)
				}
			}
		})
	}
}

type encryptionBenchmarkBatch struct {
	inputs []LiveCommitInput
	rows   []PreparedLakeProjection
}

func encryptionBenchmarkData(t *testing.T, count int) ([]encryptionBenchmarkBatch, time.Time) {
	t.Helper()
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	var batches []encryptionBenchmarkBatch
	// Keep some Parquet files eligible for the hosted compaction size filter.
	batchSize := min(count/encryptionBenchmarkBatches, 10_000)
	for offset := 0; offset < count; offset += batchSize {
		var batch encryptionBenchmarkBatch
		for i := offset; i < min(offset+batchSize, count); i++ {
			digest := sha256.Sum256([]byte(strconv.Itoa(i)))
			digest[6] = digest[6]&0x0f | 0x40
			digest[8] = digest[8]&0x3f | 0x80
			occurred := start.AddDate(0, 0, i%encryptionBenchmarkDays).Add(time.Duration(i%86_400) * time.Second)
			input := event.Event{
				ID:         int64(i + 1),
				ProjectID:  encryptionBenchmarkProject,
				UUID:       fmt.Sprintf("%x-%x-%x-%x-%x", digest[:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16]),
				Event:      []string{encryptionBenchmarkEvent, "page_viewed", "signed_up", "item_added"}[i%4],
				DistinctID: fmt.Sprintf("visitor-%d", i%2137),
				Properties: json.RawMessage(fmt.Sprintf(`{"amount":%d,"plan":%q,"session":%q,"screen":%q}`,
					i%10000, []string{"free", "pro"}[int(digest[0])%2], fmt.Sprintf("%x", digest[16:24]), []string{"product", "home", "checkout"}[i%3])),
				Timestamp:  occurred.Format(time.RFC3339Nano),
				ReceivedAt: occurred.Add(3 * time.Second).Format(time.RFC3339Nano),
				Context:    json.RawMessage(fmt.Sprintf(`{"request_id":%q,"device":%q}`, fmt.Sprintf("%x", digest[24:]), []string{"phone", "desktop"}[i%2])),
			}
			prepared, err := PrepareLakeProjection(EventProjection{Event: input})
			if err != nil {
				t.Fatal(err)
			}
			input.ID = 0
			batch.inputs = append(batch.inputs, LiveCommitInput{Event: input})
			batch.rows = append(batch.rows, prepared)
		}
		batches = append(batches, batch)
	}
	return batches, start.AddDate(0, 0, encryptionBenchmarkDays+1)
}

type encryptionBenchmarkReads struct {
	daily   AnalyticsResult
	profile EventProfile
	catalog EventCatalog
}

func runEncryptionBenchmark(t *testing.T, batches []encryptionBenchmarkBatch, asOf time.Time, encrypted bool, sample int) encryptionBenchmarkReads {
	t.Helper()
	ctx := t.Context()
	store, err := NewDuckLake(DuckLakeConfig{Directory: t.TempDir(), MemoryLimitMB: 512, Threads: 1, MaxOpenProjects: 1, DisableCleanup: true, Encrypted: encrypted})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	p, release, err := store.LeaseProject(ctx, encryptionBenchmarkProject)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := p.DB.ExecContext(ctx, `CALL ducklake_set_option('lake','data_inlining_row_limit','0')`); err != nil {
		t.Fatal(err)
	}
	var count int
	for _, batch := range batches {
		count += len(batch.rows)
	}
	mode, magic := "plain", "PAR1"
	if encrypted {
		mode, magic = "encrypted", "PARE"
	}
	var engine, extension string
	if err := p.DB.QueryRowContext(ctx, `SELECT version(),extension_version FROM ducklake_settings('lake')`).Scan(&engine, &extension); err != nil {
		t.Fatal(err)
	}
	t.Logf("parquet_encryption_fixture mode=%s rows=%d batches=%d sample=%d engine=%s extension=%s threads=%d memory_mib=%d", mode, count, len(batches), sample, engine, extension, store.Config.Threads, store.Config.MemoryLimitMB)
	measure := func(name string, run func()) {
		before := encryptionBenchmarkCPU(t)
		start := time.Now()
		run()
		elapsed := time.Since(start)
		cpu := encryptionBenchmarkCPU(t) - before
		t.Logf("parquet_encryption_timing mode=%s rows=%d sample=%d operation=%s elapsed_us=%d cpu_us=%d", mode, count, sample, name, elapsed.Microseconds(), cpu.Microseconds())
	}
	measure("native_flush", func() {
		for _, batch := range batches {
			flushEncryptionBenchmarkBatch(t, p.DB, batch)
		}
	})
	before := encryptionBenchmarkFiles(t, p.DB, magic)
	t.Logf("parquet_encryption_files mode=%s rows=%d sample=%d phase=flushed files=%d bytes=%d", mode, count, sample, before.files, before.bytes)
	if before.files < 2*len(batches) {
		t.Fatalf("expected at least two files per batch, got %d", before.files)
	}

	options, err := NormalizeEventProfileInput(EventProfileInput{Event: encryptionBenchmarkEvent, ExampleLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	read := func(measure func(string, func())) encryptionBenchmarkReads {
		var result encryptionBenchmarkReads
		measure("count_by_day", func() {
			result.daily, err = store.Query(ctx, encryptionBenchmarkProject, `SELECT date_trunc('day',event_time) AS day,count(*) AS total FROM events GROUP BY day ORDER BY day`, nil, DefaultQueryExecutionLimits())
			if err != nil {
				t.Fatal(err)
			}
			result.daily.Statistics = AnalyticsStatistics{}
		})
		measure("event_profile", func() {
			profile, err := store.Profile(ctx, encryptionBenchmarkProject, options, asOf)
			if err != nil {
				t.Fatal(err)
			}
			result.profile, err = SummarizeEventProfile(profile, options, asOf)
			if err != nil {
				t.Fatal(err)
			}
		})
		measure("event_catalog", func() {
			result.catalog, err = store.Catalog(ctx, EventCatalogInput{ID: encryptionBenchmarkProject})
			if err != nil {
				t.Fatal(err)
			}
		})
		return result
	}
	withoutTiming := func(_ string, run func()) { run() }
	read(withoutTiming)
	result := read(measure)
	var total int
	for _, row := range result.daily.Data {
		var n string
		if err := json.Unmarshal(row["total"], &n); err != nil {
			t.Fatal(err)
		}
		parsed, err := strconv.Atoi(n)
		if err != nil {
			t.Fatal(err)
		}
		total += parsed
	}
	if len(result.daily.Data) != encryptionBenchmarkDays || total != count || result.profile.Activity.Total != strconv.Itoa(count/4) || len(result.catalog.Events) != 4 {
		t.Fatalf("unexpected fixture results: days=%d total=%d profile=%s catalog=%d", len(result.daily.Data), total, result.profile.Activity.Total, len(result.catalog.Events))
	}

	var stats DuckLakeMaintenanceStats
	measure("compaction", func() {
		if err := p.MaintenanceCommand(ctx, fmt.Sprintf("CALL ducklake_merge_adjacent_files('lake',max_compacted_files=>1,max_file_size=>%d)", hostedMergeFileSize), &stats); err != nil {
			t.Fatal(err)
		}
	})
	after := encryptionBenchmarkFiles(t, p.DB, magic)
	t.Logf("parquet_encryption_files mode=%s rows=%d sample=%d phase=compacted files=%d bytes=%d", mode, count, sample, after.files, after.bytes)
	if after.files >= before.files || stats.FilesProcessed == 0 || stats.FilesCreated == 0 {
		t.Fatalf("compaction did not merge files: before=%d after=%d stats=%+v", before.files, after.files, stats)
	}
	if compacted := read(withoutTiming); !reflect.DeepEqual(result, compacted) {
		t.Fatal("read results changed after compaction")
	}
	return result
}

func flushEncryptionBenchmarkBatch(t *testing.T, db *sql.DB, batch encryptionBenchmarkBatch) {
	t.Helper()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	known, err := liveHeads(ctx, tx, batch.inputs)
	if err != nil || len(known) != 0 {
		t.Fatalf("unexpected known event heads: %d, %v", len(known), err)
	}
	if err := insertCanonicalEvents(ctx, tx, batch.rows); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func encryptionBenchmarkCPU(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

type encryptionBenchmarkFileStats struct {
	files int
	bytes int64
}

func encryptionBenchmarkFiles(t *testing.T, db *sql.DB, magic string) encryptionBenchmarkFileStats {
	t.Helper()
	var stats encryptionBenchmarkFileStats
	for _, table := range []string{"current_events", "heads"} {
		rows, err := db.QueryContext(t.Context(), `SELECT data_file,data_file_size_bytes FROM ducklake_list_files('lake',?)`, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var path string
			var bytes int64
			if err := rows.Scan(&path, &bytes); err != nil {
				t.Fatal(err)
			}
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			var header [4]byte
			_, readErr := io.ReadFull(file, header[:])
			closeErr := file.Close()
			if readErr != nil || closeErr != nil || string(header[:]) != magic || bytes <= 0 {
				t.Fatalf("invalid %s file: magic=%q bytes=%d read=%v close=%v", table, header, bytes, readErr, closeErr)
			}
			stats.files++
			stats.bytes += bytes
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	return stats
}
