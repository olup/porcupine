package lake

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/olup/porcupine/internal/event"
)

func TestDuckLakeS3SharedPostgresLiveProofLookupPerformance(t *testing.T) {
	t.Parallel()
	if os.Getenv("PORCUPINE_TEST_LIVE_PROOF_LOOKUP_BENCH") != "1" {
		t.Skip("enable the opt-in live proof lookup performance run")
	}
	fixture, requests, getBytes := countedNativeProofS3(t)
	dsn := sharedPostgresDSN(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(t.Context(), `CREATE SEQUENCE IF NOT EXISTS shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	const tenant = "benchmark-tenant"
	at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	config := DuckLakeConfig{PostgresURL: dsn, SharedSourceID: event.NewID(), S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 1, DisableCleanup: true}
	for _, count := range []int64{4096, 65536, 262144} {
		perFile := min(int64(4096), count/4)
		for _, layout := range []struct {
			name   string
			v4Rows int64
			prefix string
		}{
			{name: "v4", v4Rows: count},
			{name: "v7"},
			{name: "mixed_v7", v4Rows: count*3/4 - perFile/2},
			{name: "mixed_prefixed_v7", v4Rows: count*3/4 - perFile/2, prefix: "live-"},
		} {
			t.Run(fmt.Sprintf("proofs_%d/%s", count, layout.name), func(t *testing.T) {
				ctx := t.Context()
				open := func() *DuckLakeStore {
					t.Helper()
					config := config
					config.Directory = t.TempDir()
					store, err := NewDuckLake(config)
					if err != nil {
						t.Fatal(err)
					}
					store.liveClock = func() time.Time { return at }
					t.Cleanup(func() {
						if err := store.Close(); err != nil {
							t.Error(err)
						}
					})
					return store
				}
				lease := func(store *DuckLakeStore, key ProjectWriteKey) *ProjectLake {
					t.Helper()
					p, release, err := store.LeaseProject(ctx, key.ProjectID)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(release)
					return p
				}
				store := open()
				key := ProjectWriteKey{config.SharedSourceID, fmt.Sprintf("proof-lookup-%d-%s", count, layout.name)}
				scope, err := store.InitializeHostedProject(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				session := acquireFenceSession(t, coordinator, key)
				fence := session.Fence(scope.StreamID)
				if err := store.AdvanceWriteFence(ctx, fence); err != nil {
					t.Fatal(err)
				}
				p := lease(store, key)
				operationID := func(sequence int64) string {
					if layout.name == "v4" || sequence <= layout.v4Rows {
						return event.NewID()
					}
					return layout.prefix + proofBenchmarkV7(at.Add(time.Duration(sequence)*2*time.Second))
				}
				seedNativeProofHistoryWithIDs(t, p, scope, fence, tenant, at, count, perFile, operationID)
				var files int64
				if err := p.DB.QueryRowContext(ctx, `SELECT count(*) FROM ducklake_list_files('lake','native_commit_proofs')`).Scan(&files); err != nil || files != count/perFile {
					t.Fatalf("expected %d history files, got %d: %v", count/perFile, files, err)
				}
				last, err := scanNativeCommitProof(p.DB.QueryRowContext(ctx, "SELECT "+nativeProofColumns+" FROM lake.native_commit_proofs WHERE effect_seq=?", count), scope)
				if err != nil {
					t.Fatal(err)
				}
				checkpoint := NativeEffectCheckpoint{Sequence: last.Sequence, Fingerprint: last.ProofFingerprint}
				measure := func(operation string, sample int, run func()) {
					t.Helper()
					beforeRequests, beforeBytes := requests.Load(), getBytes.Load()
					start := time.Now()
					run()
					t.Logf("live_proof_bench history=%s proofs=%d v4_rows=%d parquet_files=%d op=%s sample=%d elapsed_us=%d s3_requests=%d s3_get_response_bytes=%d",
						layout.name, count, layout.v4Rows, files, operation, sample, time.Since(start).Microseconds(), requests.Load()-beforeRequests, getBytes.Load()-beforeBytes)
				}

				// The independent reader starts with no cached Parquet pages.
				reader := lease(open(), key)
				for sample := 1; sample <= 8; sample++ {
					id := operationID(count + int64(sample))
					measure("miss", sample, func() {
						tx, err := reader.DB.BeginTx(ctx, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Rollback()
						if _, found, err := readNativeCommitProof(ctx, tx, scope, id); found || err != nil {
							t.Fatal(found, err)
						}
					})
				}
				for sample := 1; sample <= 8; sample++ {
					batch := LiveCommitBatch{ID: operationID(count + int64(sample)), Scope: scope, TenantID: tenant,
						Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "viewed", event.NewID(), "person", `{}`)}}
					stager := &liveProofStager{objects: store.Remote, attempt: fence.AcquisitionID}
					measure("append", sample, func() {
						reservation, err := session.ReserveEventIDs(ctx, scope.StreamID, batch.ID, len(batch.Inputs))
						if err != nil {
							t.Fatal(err)
						}
						err = session.RunNative(ctx, func(ctx context.Context) error {
							result, err := store.CommitLiveFromCheckpoint(ctx, fence, batch, reservation, stager, checkpoint, func(_ context.Context, commit func() error) error { return commit() }, nil)
							if err != nil {
								return err
							}
							commit, ok := result.(CommittedLiveBatch)
							if !ok || !commit.Fresh || commit.Evidence.Sequence != checkpoint.Sequence+1 || commit.Evidence.OperationID != batch.ID {
								t.Fatalf("unexpected live outcome: %#v", result)
							}
							checkpoint = NativeEffectCheckpoint{Sequence: commit.Evidence.Sequence, Fingerprint: commit.Evidence.ProofFingerprint}
							return nil
						})
						if err != nil {
							t.Fatal(err)
						}
					})
				}
			})
		}
	}
}

func proofBenchmarkV7(at time.Time) string {
	id := uuid.New()
	var millis [8]byte
	binary.BigEndian.PutUint64(millis[:], uint64(at.UnixMilli()))
	copy(id[:6], millis[2:])
	id[6] = id[6]&0x0f | 0x70
	return id.String()
}
