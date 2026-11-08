package porcupine

import (
	"context"
	"strings"
	"testing"
)

func TestReceiptFactsRequireCompleteAuthoritativeEnvelope(t *testing.T) {
	f := receiptHistory(t)
	key := receiptKey{"project", "old"}
	f.facts[key] = true
	source, seen, err := f.c.receiptFacts(context.Background(), []receiptKey{key})
	if err != nil || source != "receipt-source" || !seen[key] {
		t.Fatal("complete physical history rejected", err)
	}
	tests := []struct{ name, response string }{
		{"source-only-jsonlines", `{"kind":"source","source_uuid":"receipt-source"}`},
		{"truncated-envelope", `{"data":[{"source_uuid":"receipt-source","facts":[]}]`},
		{"missing-completion", `{"data":[{"source_uuid":"receipt-source","source_engine":"ReplacingMergeTree","definition":"SETTINGS allow_experimental_replacing_merge_with_cleanup=0","policy_count":"0","restrictive_count":"0","allow_count":"0","facts":[]}]}`},
		{"missing-facts", `{"data":[{"source_uuid":"receipt-source","source_engine":"ReplacingMergeTree","definition":"SETTINGS allow_experimental_replacing_merge_with_cleanup=0","policy_count":"0","restrictive_count":"0","allow_count":"0"}],"rows":1,"statistics":{}}`},
		{"trailing-output", `{"data":[],"rows":0,"statistics":{}} {"error":"late"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f.response = test.response
			if _, _, err := f.c.receiptFacts(context.Background(), []receiptKey{key}); err == nil {
				t.Fatal("partial history proved absence")
			}
		})
	}
	f.response = ""
	f.restrictive = 1
	f.policies = 1
	f.allow = 1
	if _, _, err = f.c.receiptFacts(context.Background(), nil); err == nil {
		t.Fatal("restrictive policy accepted")
	}
	f.restrictive = 0
	f.allow = 0
	if _, _, err = f.c.receiptFacts(context.Background(), nil); err == nil {
		t.Fatal("missing explicit unrestricted policy accepted")
	}
	f.allow = 1
	if _, _, err = f.c.receiptFacts(context.Background(), nil); err != nil {
		t.Fatal("explicit admin visibility rejected", err)
	}
}
func TestReceiptFactsRejectAllDeletingAndUnknownTTLs(t *testing.T) {
	base := `CREATE TABLE events_v2 ENGINE = ReplacingMergeTree(revision,is_deleted) `
	settings := ` SETTINGS allow_experimental_replacing_merge_with_cleanup = 0`
	for _, ttl := range []string{
		`TTL toDateTime(fromUnixTimestamp64Milli(received_at_ms,'UTC')) + INTERVAL 1 DAY`,
		`TTL toDateTime(fromUnixTimestamp64Milli(received_at_ms,'UTC')) + INTERVAL 1 DAY DELETE`,
		`TTL toDateTime(fromUnixTimestamp64Milli(received_at_ms,'UTC')) + INTERVAL 1 DAY TO VOLUME 'cold', toDateTime(received_at_ms) + INTERVAL 2 DAY`,
		`TTL received_at + INTERVAL 1 DAY TO VOLUME 'cold'`,
	} {
		if receiptDefinitionSafe(base + ttl + settings) {
			t.Fatal("unsafe or unknown TTL accepted", ttl)
		}
	}
	for _, ttl := range []string{"", `TTL toDateTime(fromUnixTimestamp64Milli(received_at_ms,'UTC')) + INTERVAL 1 DAY TO VOLUME 'cold'`, `TTL toDateTime(fromUnixTimestamp64Milli(received_at_ms, 'UTC')) + toIntervalDay(1) TO VOLUME 'cold'`} {
		if !receiptDefinitionSafe(base + ttl + settings) {
			t.Fatal("deployment move TTL rejected", ttl)
		}
	}
	if receiptDefinitionSafe(strings.ReplaceAll(base+settings, "cleanup = 0", "cleanup = 1")) {
		t.Fatal("tombstone cleanup accepted")
	}
}

func TestReceiptFactsRequireProjectUUIDSortingKey(t *testing.T) {
	for _, key := range []string{"project_id, uuid", "(project_id,uuid)", "tuple(`project_id`, `uuid`)"} {
		if !receiptSortingKeySafe(key) {
			t.Fatal("expected sorting key rejected", key)
		}
	}
	for _, key := range []string{"project_id", "uuid", "project_id, event", "lower(project_id), uuid", "project_id, uuid, event"} {
		if receiptSortingKeySafe(key) {
			t.Fatal("potentially destructive replacement key accepted", key)
		}
	}
}
