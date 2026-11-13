//go:build ducklake

package porcupine

import (
	"context"
	"testing"
)

func TestDuckLakeIdentityControlRemainsPrivateAfterDelivery(t *testing.T) {
	s := duckLakeJourneyService(t)
	project, ingest := project(t, s)
	receipt := receiptFrom(t, nativeRequest(t, s, ingest, `{"type":"identify","message_id":"private-control","anonymous_id":"browser","user_id":"account","set":{"plan":"private"}}`, 200), 0)
	uuid := receipt["uuid"].(string)
	duckLakeJourneyDrain(t, s)
	loaded, err := s.lake.Load(context.Background(), project, uuid)
	if err != nil || loaded.IdentitySnapshot == "" {
		t.Fatalf("private identity marker lost after delivery: %v", err)
	}
	path := "/api/v1/projects/" + project + "/events/" + uuid
	request(t, s, "GET", path, adminToken, "", 404)
	request(t, s, "PATCH", path, adminToken, `{"expected_revision":"0","idempotency_key":"private-edit","snapshot":{"event":"illegal","distinct_id":"browser","timestamp":"2026-11-13T10:00:00Z","properties":{}}}`, 409)
	request(t, s, "DELETE", path, adminToken, `{"expected_revision":"0","idempotency_key":"private-delete"}`, 409)
}
