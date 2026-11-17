package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestHistoricalAdaptersKeepOriginalTimeAndSourceIdentity(t *testing.T) {
	t.Parallel()
	inputs := map[string]string{
		"native":  `{"type":"track","message_id":"stable","name":"stock_updated","timestamp":"2020-01-01T23:15:00.123456789-02:00","properties":{"quantity":9007199254740993,"token":"business"}}`,
		"segment": `{"type":"track","messageId":"stable","event":"stock_updated","userId":"actor","timestamp":"2020-01-01T23:15:00.123456789-02:00","properties":{"quantity":9007199254740993,"token":"business"}}`,
		"posthog": `{"uuid":"stable","event":"stock_updated","distinct_id":"actor","timestamp":"2020-01-01T23:15:00.123456789-02:00","properties":{"quantity":9007199254740993,"token":"credential"}}`,
	}
	for format, raw := range inputs {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			row, err := ParseRow([]byte(raw), "events", format)
			if err != nil {
				t.Fatal(err)
			}
			if row.Event.UUID != "stable" || row.Event.Timestamp != "2020-01-02T01:15:00.123456789Z" || row.Event.Event != "stock_updated" {
				t.Fatal(row.Event)
			}
			var props map[string]any
			d := json.NewDecoder(strings.NewReader(string(row.Event.Properties)))
			d.UseNumber()
			if err := d.Decode(&props); err != nil {
				t.Fatal(err)
			}
			if props["quantity"] != json.Number("9007199254740993") {
				t.Fatal(props)
			}
			if format == "posthog" {
				if _, ok := props["token"]; ok {
					t.Fatal("imported a PostHog credential")
				}
			} else if props["token"] != "business" {
				t.Fatal("business property lost", props)
			}
			if format == "native" && row.Event.DistinctID != "" {
				t.Fatal("invented an actor")
			}
		})
	}
}

func TestHistoricalRowsRequireDatesIDsAndUnambiguousFields(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"native", "segment", "posthog"} {
		for _, raw := range []string{`{}`, `{"type":"track","name":"v","event":"v","uuid":"id","message_id":"id","messageId":"id","distinct_id":"u","userId":"u"}`, `{"uuid":"a","uuid":"b","event":"v","distinct_id":"u","timestamp":"2020-01-01T00:00:00Z"}`} {
			if _, err := ParseRow([]byte(raw), "events", format); !errors.Is(err, ErrRow) {
				t.Fatal(format, raw, err)
			}
		}
	}
}

func TestChunkResumesIncompleteRowsAndBoundsOversizedRows(t *testing.T) {
	t.Parallel()
	row := `{"type":"track","message_id":"stable","name":"event","timestamp":"2020-01-01T00:00:00Z"}`
	file := ChunkState{FileID: "file", Kind: "events", Format: "native"}
	first, err := ParseChunk(context.Background(), file, []byte(row[:20]), false)
	if err != nil || len(first.Rows) != 0 || len(first.State.Tail) != 20 {
		t.Fatal(first, err)
	}
	next, err := ParseChunk(context.Background(), first.State, []byte(row[20:]+"\n\n"+row), true)
	if err != nil || len(next.Rows) != 2 || next.State.Counts.Rows != 2 || next.State.LineNumber != 2 || len(next.State.Tail) != 0 || next.Positions[0] != 1 || next.Positions[1] != 3 {
		t.Fatal(next, err)
	}
	huge, err := ParseChunk(context.Background(), file, []byte(strings.Repeat("x", MaxRowBytes+1)), false)
	if err != nil || len(huge.State.Tail) != 0 || !huge.State.Discarding || huge.State.Counts.Invalid != 1 || huge.Samples[0].Reason != "row_too_large" {
		t.Fatal(huge, err)
	}
	recovered, err := ParseChunk(context.Background(), huge.State, []byte("discarded\n"+row), true)
	if err != nil || len(recovered.Rows) != 1 || recovered.State.Counts.Rows != 2 || recovered.Positions[0] != 2 {
		t.Fatal(recovered, err)
	}
}

func TestChunkLimitsAndCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ParseChunk(ctx, ChunkState{}, []byte("x"), false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := ParseChunk(context.Background(), ChunkState{}, []byte(strings.Repeat("{}\n", MaxChunkRows+1)), false); !errors.Is(err, ErrChunkRows) {
		t.Fatal(err)
	}
}

func TestManifestAndHashCheckpoint(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("AB", 32)
	original := []ImportUploadFileSpec{{Path: "events/z.jsonl", Kind: "events", SHA256: digest}, {Path: "people/a.jsonl", Kind: "persons", SHA256: digest}}
	normalized, err := NormalizeManifest(original)
	if err != nil || normalized[0].Kind != "persons" || normalized[0].SHA256 != strings.ToLower(digest) || original[0].Kind != "events" || original[0].SHA256 != digest {
		t.Fatal(normalized, err)
	}
	for _, path := range []string{"../secret", "/absolute", "./relative", "a/../b", "a\\b", "a\nb", "."} {
		if _, err := NormalizeManifest([]ImportUploadFileSpec{{Path: path, Kind: "events", SHA256: digest}}); err == nil {
			t.Fatal(path)
		}
	}
	if _, err := NormalizeManifest(append(original, original[0])); err == nil {
		t.Fatal("accepted duplicate path")
	}
	hash := sha256.New()
	hash.Write([]byte("first"))
	state, err := HashState(hash)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreHash(state)
	if err != nil {
		t.Fatal(err)
	}
	restored.Write([]byte("second"))
	expected := sha256.Sum256([]byte("firstsecond"))
	if hex.EncodeToString(restored.Sum(nil)) != hex.EncodeToString(expected[:]) {
		t.Fatal("hash checkpoint changed bytes")
	}
	if _, err := RestoreHash([]byte("invalid")); err == nil {
		t.Fatal("accepted invalid checkpoint")
	}
}
