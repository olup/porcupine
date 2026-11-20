package porcupine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

// Run separately from correctness tests, after other load tests have stopped:
//
// CGO_ENABLED=1 go test ./internal/porcupine -run '^$' -bench '^BenchmarkReceiptFreshIDLookup$' -benchtime=10x -benchmem -count=3 -timeout=300s
//
// This measures receipt admission preflight for fresh producer IDs.
// It excludes HTTP decoding, authorization, SQLite acceptance/ACK, identity,
// projection, pruning, extension loading, and fixture setup. The same IDs are
// looked up repeatedly without inserting them. Both stages use a warm local
// project engine and filesystem cache, 128 MiB engine memory, and one DuckDB
// thread. It is not an ingestion throughput or S3/shared-catalog benchmark.
func BenchmarkReceiptFreshIDLookup(b *testing.B) {
	for _, history := range []int{20_000, 100_000} {
		b.Run(fmt.Sprintf("history_%d", history), func(b *testing.B) {
			b.StopTimer()
			s, project := receiptLookupBenchmarkFixture(b, history)
			ctx := context.Background()
			for _, stage := range []string{"unpruned", "pruned"} {
				if stage == "pruned" {
					// Exactly one old receipt is eligible. The actual prune operation
					// verifies its lake head and sets the durable marker atomically.
					if err := s.receipts.Prune(ctx, s.receiptRetention); err != nil {
						b.Fatal(err)
					}
					var remaining int
					if err := s.db.QueryRow("SELECT count(*) FROM event_receipts").Scan(&remaining); err != nil || remaining != history-1 {
						b.Fatalf("prune one receipt: remaining=%d error=%v", remaining, err)
					}
				}
				b.Run(stage, func(b *testing.B) {
					for _, batchSize := range []int{1, 100, 1000} {
						b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
							b.StopTimer()
							uuids := make([]string, batchSize)
							for i := range uuids {
								uuids[i] = receiptLookupBenchmarkUUID("fresh", i)
							}
							lookup := func() {
								known, release, err := s.receipts.BeginAdmission(ctx, project, uuids)
								if err != nil {
									b.Fatal(err)
								}
								release()
								if len(known) != 0 {
									b.Fatalf("fresh-ID lookup found %d known IDs", len(known))
								}
							}
							lookup()
							b.ReportAllocs()
							b.ResetTimer()
							b.StartTimer()
							for range b.N {
								lookup()
							}
							b.StopTimer()
							b.ReportMetric(float64(batchSize), "ids/op")
							b.ReportMetric(float64(batchSize*b.N)/b.Elapsed().Seconds(), "ids/s")
							b.ReportMetric(float64(history), "history_rows")
							b.ReportMetric(128, "lake_memory_MiB")
						})
					}
				})
			}
		})
	}
}

func receiptLookupBenchmarkUUID(group string, i int) string {
	// Use deterministic, interleaved hashes of the same width as real native
	// source IDs. Separate lexical prefixes would let Parquet skip the history.
	key := fmt.Sprintf("receipt-benchmark/%s/%09d", group, i)
	head, tail := sha256.Sum256([]byte(key)), sha256.Sum256([]byte("tail/"+key))
	return lakeapi.SourceEventUUIDPrefix + hex.EncodeToString(head[:]) + hex.EncodeToString(tail[:])
}

func receiptLookupBenchmarkFixture(b *testing.B, history int) (*Service, string) {
	b.Helper()
	ctx := context.Background()
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	s, err := Open(Config{DBPath: filepath.Join(b.TempDir(), "journal.sqlite"), BootstrapToken: adminToken,
		Clock: func() time.Time { return now }, ReceiptRetention: 48 * time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Error(err)
		}
	})
	// Attach storage after Open so no delivery, maintenance, or cleanup task
	// competes with the narrowly measured lookup.
	s.lake, err = lakeapi.NewDuckLake(lakeapi.DuckLakeConfig{Directory: filepath.Join(b.TempDir(), "lake"),
		MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 1, DisableCleanup: true})
	if err != nil {
		b.Fatal(err)
	}
	const project = "receipt-benchmark-project"
	if _, err := s.db.ExecContext(ctx, `INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES(?,'default','Receipt benchmark',?,'mixed')`, project, now.Format(time.RFC3339Nano)); err != nil {
		b.Fatal(err)
	}
	store := s.lake
	p, release, err := store.LeaseProject(ctx, project)
	if err != nil {
		b.Fatal(err)
	}
	// Seed heads directly: the fixture is a UUID-index history, not event data.
	// Its hashes match receiptLookupBenchmarkUUID and do not benchmark ingestion.
	_, err = p.DB.ExecContext(ctx, `INSERT INTO lake.heads SELECT ? || sha256('receipt-benchmark/history/' || printf('%09d',i)) || sha256('tail/receipt-benchmark/history/' || printf('%09d',i)), i+1, 0, false, ? FROM range(?) AS fixture(i)`, lakeapi.SourceEventUUIDPrefix, now.Format(time.RFC3339Nano), history)
	release()
	if err != nil {
		b.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, `INSERT INTO event_receipts(project_id,uuid,event_id,recorded_at_ms) VALUES(?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Close()
	for i := range history {
		recorded := now.UnixMilli()
		if i == 0 {
			recorded = now.Add(-72 * time.Hour).UnixMilli()
		}
		if _, err := statement.ExecContext(ctx, project, receiptLookupBenchmarkUUID("history", i), i+1, recorded); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	// Validate the independently seeded lake/SQLite UUID encoding and warm the
	// engine before the unpruned/pruned comparison.
	known, err := s.lake.KnownEvents(ctx, project, []string{receiptLookupBenchmarkUUID("history", 0)})
	if err != nil || known[receiptLookupBenchmarkUUID("history", 0)] != 1 {
		b.Fatalf("seeded head mismatch: %v, %v", known, err)
	}
	return s, project
}
