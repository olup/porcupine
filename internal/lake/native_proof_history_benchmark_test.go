package lake

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
)

// Run with the isolated shared PostgreSQL and Silo fixtures, on an otherwise
// idle runner. The same harness can run against the parent implementation.
func TestDuckLakeS3SharedPostgresNativeProofHistoryPerformance(t *testing.T) {
	t.Parallel()
	if os.Getenv("PORCUPINE_TEST_NATIVE_PROOF_BENCH") != "1" {
		t.Skip("enable the opt-in comparative performance run")
	}
	fixture := nativeS3Fixture(t)
	target, err := url.Parse(fixture.storage.Endpoint)
	if err != nil || target.Scheme != "http" {
		t.Fatalf("expected the loopback HTTP Silo fixture: %v", err)
	}
	var requests, getBytes atomic.Int64
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Method == http.MethodGet && response.StatusCode < 300 && response.ContentLength > 0 {
			getBytes.Add(response.ContentLength)
		}
		return nil
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	fixture.storage.Endpoint = server.URL
	dsn := sharedPostgresDSN(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(t.Context(), `CREATE SEQUENCE IF NOT EXISTS shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	config := DuckLakeConfig{PostgresURL: dsn, SharedSourceID: event.NewID(), S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 1, DisableCleanup: true}

	for _, rows := range []int64{4096, 65536, 262144} {
		t.Run(fmt.Sprintf("proofs_%d", rows), func(t *testing.T) {
			ctx := t.Context()
			config.Directory = t.TempDir()
			lake, err := NewDuckLake(config)
			if err != nil {
				t.Fatal(err)
			}
			store := lake.(*DuckLakeStore)
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			key := ProjectWriteKey{config.SharedSourceID, fmt.Sprintf("proof-history-%d", rows)}
			scope, err := store.InitializeHostedProject(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			session := acquireFenceSession(t, coordinator, key)
			fence := session.Fence(scope.StreamID)
			if err := store.AdvanceWriteFence(ctx, fence); err != nil {
				t.Fatal(err)
			}
			p, release, err := store.LeaseProject(ctx, key.ProjectID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()

			const perFile = 4096
			const tenant = "benchmark-tenant"
			at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
			store.liveClock = func() time.Time { return at }
			seedNativeProofHistory(t, p, scope, fence, tenant, at, rows, perFile)
			var files int
			if err := p.DB.QueryRowContext(ctx, `SELECT count(*) FROM ducklake_list_files('lake','native_commit_proofs')`).Scan(&files); err != nil || int64(files) != rows/perFile {
				t.Fatalf("expected %d history files, got %d: %v", rows/perFile, files, err)
			}
			t.Logf("fixture proofs=%d parquet_files=%d", rows, files)

			measure := func(operation string, sample int, run func()) {
				t.Helper()
				beforeRequests, beforeBytes := requests.Load(), getBytes.Load()
				start := time.Now()
				run()
				t.Logf("proof_bench proofs=%d parquet_files_at_start=%d op=%s sample=%d elapsed_us=%d s3_requests=%d s3_get_response_bytes=%d",
					rows, files, operation, sample, time.Since(start).Microseconds(), requests.Load()-beforeRequests, getBytes.Load()-beforeBytes)
			}
			pending := func(after, ack int64, proof NativeCommitEvidence) {
				t.Helper()
				page, err := store.PendingNativeEffects(ctx, scope, after, 2)
				if err != nil || page.AcknowledgedThrough != ack {
					t.Fatal(page, err)
				}
				if proof.Sequence == 0 {
					if len(page.Evidence) != 0 || page.NativeHighWater != after {
						t.Fatal(page)
					}
				} else if len(page.Evidence) != 1 || page.NativeHighWater != proof.Sequence || page.Evidence[0].ProofFingerprint != proof.ProofFingerprint {
					t.Fatal(page)
				}
			}
			measure("cold_pending", 0, func() { pending(rows, rows, NativeCommitEvidence{}) })
			for sample := 1; sample <= 8; sample++ {
				previous := rows + int64(sample) - 1
				measure("warm_pending", sample, func() { pending(previous, previous, NativeCommitEvidence{}) })
				batch := LiveCommitBatch{ID: fmt.Sprintf("append-%02d", sample), TenantID: tenant, Scope: scope,
					Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "viewed", event.NewID(), "person", `{}`)}}
				stager := &liveProofStager{objects: store.Remote, attempt: fence.AcquisitionID}
				var proof NativeCommitEvidence
				measure("append", sample, func() {
					result, ok := commitLiveFixture(t, store, session, batch, stager).(CommittedLiveBatch)
					if !ok || !result.Fresh || result.Evidence.Sequence != previous+1 || result.Evidence.Totals[0].Live != previous+1 {
						t.Fatal(result)
					}
					proof = result.Evidence
				})
				measure("pending_after_append", sample, func() { pending(previous, previous, proof) })
				measure("ack", sample, func() {
					if err := store.AcknowledgeNativeEffects(ctx, fence, scope, proof.Sequence); err != nil {
						t.Fatal(err)
					}
				})
				measure("pending_after_ack", sample, func() { pending(proof.Sequence, proof.Sequence, NativeCommitEvidence{}) })
			}
		})
	}
}

func seedNativeProofHistory(t *testing.T, p *ProjectLake, scope HostedProjectScope, fence ProjectWriteFence, tenant string, at time.Time, count, perFile int64) {
	t.Helper()
	ctx := t.Context()
	day := at.UTC().Format("2006-01-02T00:00:00Z")
	csvPath := filepath.Join(p.directory, "benchmark-proof-seed.csv")
	defer os.Remove(csvPath)
	for start := int64(1); start <= count; start += perFile {
		file, err := os.Create(csvPath)
		if err != nil {
			t.Fatal(err)
		}
		writer := csv.NewWriter(file)
		for sequence := start; sequence < start+perFile && sequence <= count; sequence++ {
			operation := fmt.Sprintf("retained-%08d", sequence)
			evidence := NativeCommitEvidence{
				Kind: NativeCommitLive, Scope: scope, TenantID: tenant, Sequence: sequence, OperationID: operation,
				InputFingerprint: nativeDigest([]byte(operation)), FenceEpoch: fence.Epoch,
				AccountedAt: at, AcceptedLive: 1, Effects: StagedLiveEffects{PlanID: "benchmark-seed"},
				Totals: []NativeSourceTotals{{Day: day, Revision: sequence, Live: sequence}},
			}
			evidence.ProofFingerprint, err = nativeEvidenceFingerprint(evidence)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Write([]string{strconv.Itoa(nativeProofVersion), strconv.FormatInt(sequence, 10), operation,
				evidence.InputFingerprint, evidence.ProofFingerprint, string(evidence.Kind), string(raw)}); err != nil {
				t.Fatal(err)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := p.DB.ExecContext(ctx, `INSERT INTO lake.native_commit_proofs (`+nativeProofColumns+`)
SELECT * FROM read_csv(?,header=false,columns={'proof_version':'INTEGER','effect_seq':'BIGINT','operation_id':'VARCHAR','input_fingerprint':'VARCHAR','proof_fingerprint':'VARCHAR','kind':'VARCHAR','evidence_json':'VARCHAR'})`, csvPath); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE lake.hosted_write_guard SET generation=generation+1,next_effect_seq=?,ack_effect_seq=?,tenant_id=? WHERE id=1`, count, count, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO lake.native_usage_totals VALUES(?,?,?,0)`, day, count, count); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify retained rows decode and fingerprint across the fixture boundaries.
	page, err := p.DB.QueryContext(ctx, "SELECT "+nativeProofColumns+" FROM lake.native_commit_proofs WHERE effect_seq IN (1,?) ORDER BY effect_seq", count)
	if err != nil {
		t.Fatal(err)
	}
	defer page.Close()
	for _, expected := range []int64{1, count} {
		if !page.Next() {
			t.Fatal(page.Err())
		}
		proof, err := scanNativeCommitProof(page, scope)
		if err != nil || proof.Sequence != expected {
			t.Fatal(proof, err)
		}
	}
	if page.Next() || page.Err() != nil {
		t.Fatal("unexpected rows in seed validation", page.Err())
	}
}
