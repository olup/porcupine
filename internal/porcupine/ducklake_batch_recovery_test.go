//go:build ducklake

package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

func TestDuckLakeLargeBatchFailureRecovery(t *testing.T) {
	s, project, token := comparisonServiceMemory(t, "ducklake", 128)
	s.queueMax = 10000
	s.deliveryMaxEvents, s.deliveryMaxBytes = 3000, 32<<20
	handler := s.Handler()
	comparisonQuery(t, handler, project, "SELECT count(*) AS total FROM events")
	payloads := comparisonPayloads(30)
	for _, payload := range payloads {
		comparisonAccept(t, handler, token, payload)
	}
	ctx := context.Background()
	batch, err := s.deliveryBatch(ctx)
	if err != nil || len(batch) != 3000 {
		t.Fatalf("selected batch=%d error=%v", len(batch), err)
	}
	prepared := make([]preparedLakeProjection, len(batch))
	for i, event := range batch {
		snapshot, err := originalSnapshot(event)
		if err != nil {
			t.Fatal(err)
		}
		prepared[i], err = prepareLakeProjection(eventProjection{Event: event, OriginalSnapshot: snapshot})
		if err != nil {
			t.Fatal(err)
		}
	}
	lake := s.lake.(*duckLakeStore)
	p, release, err := lake.leaseProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	// Prove this exact selection exceeds the native transaction budget before
	// asking the real delivery worker to recover it automatically.
	err = p.applyChunk(ctx, prepared)
	release()
	var nativeError *duckdb.Error
	if !errors.As(err, &nativeError) || nativeError.Type != duckdb.ErrorTypeOutOfMemory {
		t.Fatalf("expected bounded native allocation failure, got %v", err)
	}
	t.Logf("native allocation failure: type=%v error=%v", nativeError.Type, nativeError)
	count, _ := queueUsage(t, s)
	if count != 3000 {
		t.Fatalf("failed analytical write lost queued events: %d", count)
	}
	result := comparisonQuery(t, handler, project, "SELECT count(*) AS total FROM events")
	if len(result.Data) != 1 || string(result.Data[0]["total"]) != `"0"` {
		t.Fatal("failed batch partially committed")
	}
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	count, _ = queueUsage(t, s)
	if count != 0 {
		t.Fatalf("automatic allocation recovery left %d accepted events queued", count)
	}
	if s.deliveryMaxEvents != 3000 || s.deliveryMaxBytes != 32<<20 {
		t.Fatal("recovery changed worker selection limits")
	}
	var quarantined int
	if err := s.db.QueryRow("SELECT count(*) FROM delivery_quarantine").Scan(&quarantined); err != nil || quarantined != 0 {
		t.Fatalf("native allocation failure quarantined accepted rows: count=%d error=%v", quarantined, err)
	}
	comparisonAccept(t, handler, token, payloads[0])
	result = comparisonQuery(t, handler, project, "SELECT count(*) AS total FROM events")
	if len(result.Data) != 1 || string(result.Data[0]["total"]) != `"3000"` {
		t.Fatal("recovery or retry changed the analytical count")
	}
	count, _ = queueUsage(t, s)
	if count != 0 {
		t.Fatal("retry requeued already projected events")
	}
}

func TestDuckLakeBatchNonAllocationFailureKeepsTransactionAtomic(t *testing.T) {
	ctx := context.Background()
	lake := openLakeStorageTest(t, t.TempDir(), 1)
	existing := lakeStorageProjection(t, "project", "existing", 1)
	if err := lake.Apply(ctx, []eventProjection{existing}); err != nil {
		t.Fatal(err)
	}
	fresh := lakeStorageProjection(t, "project", "fresh", 2)
	conflicting := existing
	conflicting.ID = 3
	err := lake.Apply(ctx, []eventProjection{fresh, conflicting})
	if err == nil {
		t.Fatal("changed stable event ID was accepted")
	}
	if n := lakeStorageCount(t, lake, "project", "events"); n != 1 {
		t.Fatalf("non-allocation failure split the transaction: rows=%d", n)
	}
	if _, err := lake.Load(ctx, "project", fresh.UUID); !errors.Is(err, errEventNotFound) {
		t.Fatalf("failed transaction applied its healthy prefix: %v", err)
	}
}

func TestDuckLakeBatchUnfitSingletonRemainsPending(t *testing.T) {
	s, projectID, token := comparisonServiceMemory(t, "ducklake", 64)
	s.deliveryMaxEvents = 1
	handler := s.Handler()
	comparisonQuery(t, handler, projectID, "SELECT count(*) AS total FROM events")
	// Literal HTML fits the accepted request limit but expands during durable
	// JSON encoding, so this also exercises the single-event soft byte budget.
	payload := []byte(`[{"type":"track","message_id":"unfit","actor_id":"visitor","name":"large","properties":{"wide":"` + strings.Repeat("<", 200000) + `"}}]`)
	comparisonAccept(t, handler, token, payload)
	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := s.flushDelivery(ctx)
		cancel()
		var nativeError *duckdb.Error
		if !errors.As(err, &nativeError) || nativeError.Type != duckdb.ErrorTypeOutOfMemory {
			t.Fatalf("expected singleton native allocation failure, got %v", err)
		}
		t.Logf("singleton allocation failure: type=%v error=%v", nativeError.Type, nativeError)
		count, _ := queueUsage(t, s)
		var quarantined int
		if err := s.db.QueryRow("SELECT count(*) FROM delivery_quarantine").Scan(&quarantined); err != nil || count != 1 || quarantined != 0 {
			t.Fatalf("singleton allocation failure lost durable acceptance: queued=%d quarantined=%d error=%v", count, quarantined, err)
		}
	}
	status, err := s.deliveryStatus(context.Background(), projectID)
	if err != nil || status.Pending != 1 || status.Quarantined != 0 || status.Error == "" {
		t.Fatalf("unfit singleton status=%+v error=%v", status, err)
	}
	otherProject, otherToken := project(t, s)
	comparisonAccept(t, handler, otherToken, []byte(`[{"type":"track","message_id":"healthy","actor_id":"other-visitor","name":"healthy"}]`))
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count, _ := queueUsage(t, s); count != 1 {
		t.Fatalf("healthy project progress changed retained singleton: queued=%d", count)
	}
	result := comparisonQuery(t, handler, otherProject, "SELECT count(*) AS total FROM events")
	if len(result.Data) != 1 || string(result.Data[0]["total"]) != `"1"` {
		t.Fatal("unfit singleton blocked the healthy project's later event")
	}
}

func TestDuckLakeLargeMutationBatchAllocationRecovery(t *testing.T) {
	s, project, token := comparisonServiceMemory(t, "ducklake", 128)
	handler := s.Handler()
	payload := comparisonPayloads(1)[0]
	comparisonAccept(t, handler, token, payload)
	originals, err := s.deliveryBatch(context.Background())
	if err != nil || len(originals) != 100 {
		t.Fatalf("queued originals=%d error=%v", len(originals), err)
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		body, err := json.Marshal(map[string]any{
			"expected_revision": "0", "idempotency_key": fmt.Sprintf("wide-edit-%d", i),
			"snapshot": map[string]any{"event": "corrected", "distinct_id": fmt.Sprintf("visitor-%03d", i), "timestamp": "2026-11-01T00:00:00Z", "properties": map[string]any{"wide": strings.Repeat("x", 15<<10)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		request(t, s, "PATCH", fmt.Sprintf("/api/v1/projects/%s/events/%s", project, originals[i].UUID), adminToken, string(body), 202)
	}
	rows, err := s.db.Query("SELECT payload FROM mutation_outbox ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	var prepared []preparedLakeProjection
	for rows.Next() {
		var payload string
		var projection eventProjection
		if err := rows.Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(payload), &projection); err != nil {
			t.Fatal(err)
		}
		value, err := prepareLakeProjection(projection)
		if err != nil {
			t.Fatal(err)
		}
		prepared = append(prepared, value)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(prepared) != 100 {
		t.Fatalf("queued mutations=%d error=%v", len(prepared), err)
	}
	lake := s.lake.(*duckLakeStore)
	p, release, err := lake.leaseProject(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	err = p.applyChunk(context.Background(), prepared)
	release()
	var nativeError *duckdb.Error
	if !errors.As(err, &nativeError) || nativeError.Type != duckdb.ErrorTypeOutOfMemory {
		t.Fatalf("expected native wide mutation allocation failure, got %v", err)
	}
	t.Logf("native mutation allocation failure: rows=%d type=%v error=%v", len(prepared), nativeError.Type, nativeError)
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	var pending, applied int
	if err := s.db.QueryRow("SELECT count(*) FROM mutation_outbox").Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("automatic mutation recovery left pending=%d error=%v", pending, err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM mutation_audit WHERE state='applied'").Scan(&applied); err != nil || applied != 100 {
		t.Fatalf("automatic mutation recovery applied=%d error=%v", applied, err)
	}
	comparisonAccept(t, handler, token, payload)
	result := comparisonQuery(t, handler, project, "SELECT count(*) AS total FROM events WHERE event='corrected' AND length(json_extract_string(properties,'$.wide'))=15360 AND revision>0")
	if len(result.Data) != 1 || string(result.Data[0]["total"]) != `"100"` {
		t.Fatalf("mutation recovery or original retry lost wide corrections: %+v", result.Data)
	}
}

func TestDuckLakeLargeEventBatchLimits(t *testing.T) {
	for _, candidate := range []struct {
		name        string
		rows, bytes int
	}{{"production", 1000, 4 << 20}, {"100_rows_8MiB", 100, 8 << 20}, {"10_rows_4MiB", 10, 4 << 20}} {
		t.Run(candidate.name, func(t *testing.T) {
			s, project, token := comparisonServiceMemory(t, "ducklake", 512)
			s.deliveryMaxEvents, s.deliveryMaxBytes = candidate.rows, candidate.bytes
			handler := s.Handler()
			for offset := 0; offset < 100; offset += 10 {
				comparisonAccept(t, handler, token, largeEventPayload(offset, 10))
			}
			ctx := context.Background()
			batch, err := s.deliveryBatch(ctx)
			if err != nil || len(batch) == 0 {
				t.Fatalf("selected rows=%d error=%v", len(batch), err)
			}
			prepared := make([]preparedLakeProjection, len(batch))
			for i, event := range batch {
				snapshot, err := originalSnapshot(event)
				if err != nil {
					t.Fatal(err)
				}
				prepared[i], err = prepareLakeProjection(eventProjection{Event: event, OriginalSnapshot: snapshot})
				if err != nil {
					t.Fatal(err)
				}
			}
			lake := s.lake.(*duckLakeStore)
			p, release, err := lake.leaseProject(ctx, project)
			if err != nil {
				t.Fatal(err)
			}
			err = p.applyChunk(ctx, prepared)
			release()
			var nativeError *duckdb.Error
			allocationFailure := errors.As(err, &nativeError) && nativeError.Type == duckdb.ErrorTypeOutOfMemory
			t.Logf("configured rows=%d bytes=%d selected rows=%d properties bytes=%d raw native allocation failure=%t error=%v", candidate.rows, candidate.bytes, len(batch), len(batch[0].Properties), allocationFailure, err)
			if err != nil && (!allocationFailure || candidate.name != "100_rows_8MiB") {
				t.Fatal(err)
			}
			for pending := int64(100); pending > 0; {
				if err := s.flushDelivery(ctx); err != nil {
					t.Fatal(err)
				}
				remaining, _ := queueUsage(t, s)
				if remaining >= pending {
					t.Fatal("adaptive delivery made no progress")
				}
				pending = remaining
			}
			result := comparisonQuery(t, handler, project, "SELECT count(*) AS total FROM events")
			if len(result.Data) != 1 || string(result.Data[0]["total"]) != `"100"` {
				t.Fatal("raw transaction retry or adaptive delivery changed event count")
			}
		})
	}
}
