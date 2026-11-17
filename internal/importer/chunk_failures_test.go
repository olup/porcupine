package importer

import (
	"bytes"
	"context"
	"testing"
)

func TestChunkFailurePositionsOutliveSampleBound(t *testing.T) {
	t.Parallel()
	data := bytes.Repeat([]byte("invalid\n"), MaxSamples+5)
	chunk, err := ParseChunk(context.Background(), ChunkState{FileID: "file", Kind: "events", Format: "native"}, data, false)
	if err != nil || len(chunk.Samples) != MaxSamples || len(chunk.Failures) != MaxSamples+5 || chunk.State.Counts.Invalid != int64(MaxSamples+5) {
		t.Fatal(chunk, err)
	}
	for i, failure := range chunk.Failures {
		if failure.Position != int64(i)+1 || failure.Reason != "invalid_row" {
			t.Fatal(failure)
		}
	}
	maximum, err := ParseChunk(context.Background(), ChunkState{FileID: "maximum", Kind: "events", Format: "native"}, bytes.Repeat([]byte("x\n"), MaxChunkRows), false)
	if err != nil || len(maximum.Failures) != MaxChunkRows || maximum.State.Counts.Rows != MaxChunkRows {
		t.Fatal("valid maximum invalid rows rejected", len(maximum.Failures), err)
	}
	oversized, err := ParseChunk(context.Background(), ChunkState{FileID: "large", Kind: "events", Format: "native"}, bytes.Repeat([]byte("x"), MaxRowBytes+1), false)
	if err != nil || len(oversized.Failures) != 1 || oversized.Failures[0] != (RowFailure{Position: 1, Reason: "row_too_large"}) || !oversized.State.Discarding {
		t.Fatal(oversized, err)
	}
	continued, err := ParseChunk(context.Background(), oversized.State, []byte("more\ninvalid\n"), false)
	if err != nil || len(continued.Failures) != 1 || continued.Failures[0] != (RowFailure{Position: 2, Reason: "invalid_row"}) {
		t.Fatal(continued, err)
	}
}
