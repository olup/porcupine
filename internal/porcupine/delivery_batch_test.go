package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestDeliveryBatchRowAndByteBudgets(t *testing.T) {
	s := service(t, 500)
	_, token := project(t, s)
	events := make([]map[string]any, 250)
	for i := range events {
		events[i] = map[string]any{"uuid": fmt.Sprintf("batch-budget-%03d", i), "event": "visit", "distinct_id": "visitor", "properties": map[string]any{}}
	}
	identityCapture(t, s, token, events, 200)
	batch, err := s.deliveryBatch(context.Background())
	if err != nil || len(batch) != 100 {
		t.Fatalf("default batch: %d events, %v", len(batch), err)
	}
	s.deliveryMaxEvents = 200
	batch, err = s.deliveryBatch(context.Background())
	if err != nil || len(batch) != 200 {
		t.Fatalf("larger batch: %d events, %v", len(batch), err)
	}
	properties, _ := json.Marshal(map[string]any{"padding": strings.Repeat("x", 32<<10)})
	if _, err := s.db.Exec("UPDATE events SET properties=?", string(properties)); err != nil {
		t.Fatal(err)
	}
	batch, err = s.deliveryBatch(context.Background())
	if err != nil || len(batch) == 0 || len(batch) >= 32 {
		t.Fatalf("4 MiB byte budget: %d events, %v", len(batch), err)
	}
	// A previously accepted oversized event must still make progress by itself.
	properties, _ = json.Marshal(map[string]any{"padding": strings.Repeat("x", 2<<20)})
	if _, err := s.db.Exec("UPDATE events SET properties=? WHERE id=(SELECT min(id) FROM events)", string(properties)); err != nil {
		t.Fatal(err)
	}
	batch, err = s.deliveryBatch(context.Background())
	if err != nil || len(batch) != 1 {
		t.Fatalf("oversized singleton: %d events, %v", len(batch), err)
	}
}
