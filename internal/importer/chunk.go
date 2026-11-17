package importer

import (
	"bytes"
	"context"
	"errors"
)

var ErrChunkRows = errors.New("chunk exceeds 1000 JSONL rows")

type ChunkState struct {
	FileID, Kind, Format string
	LineNumber           int64
	Tail                 []byte
	Discarding           bool
	Counts               ImportCounts
}
type Chunk struct {
	State     ChunkState
	Rows      []Row
	Positions []int64
	Samples   []ImportRowSample
}

func ParseChunk(ctx context.Context, file ChunkState, data []byte, final bool) (Chunk, error) {
	work := Chunk{State: file}
	addSample := func(reason string) {
		if len(work.Samples) < MaxSamples {
			work.Samples = append(work.Samples, ImportRowSample{FileID: file.FileID, RowNumber: work.State.LineNumber + 1, Reason: reason})
		}
	}
	parse := func(raw []byte) error {
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			return nil
		}
		work.State.Counts.Rows++
		if work.State.Counts.Rows-file.Counts.Rows > MaxChunkRows {
			return ErrChunkRows
		}
		row, err := ParseRow(raw, file.Kind, file.Format)
		if err != nil {
			work.State.Counts.Invalid++
			addSample("invalid_row")
			return nil
		}
		work.Rows = append(work.Rows, row)
		work.Positions = append(work.Positions, work.State.LineNumber+1)
		return nil
	}
	for len(data) > 0 {
		if ctx.Err() != nil {
			return work, ctx.Err()
		}
		end := bytes.IndexByte(data, '\n')
		complete := end >= 0
		if !complete {
			end = len(data)
		}
		segment := data[:end]
		if work.State.Discarding {
			if complete {
				work.State.Discarding = false
				work.State.LineNumber++
			}
		} else if len(work.State.Tail)+len(segment) > MaxRowBytes {
			work.State.Tail = nil
			work.State.Counts.Rows++
			work.State.Counts.Invalid++
			if work.State.Counts.Rows-file.Counts.Rows > MaxChunkRows {
				return work, ErrChunkRows
			}
			addSample("row_too_large")
			work.State.Discarding = !complete
			if complete {
				work.State.LineNumber++
			}
		} else {
			work.State.Tail = append(work.State.Tail, segment...)
			if complete {
				if err := parse(work.State.Tail); err != nil {
					return work, err
				}
				work.State.Tail = nil
				work.State.LineNumber++
			}
		}
		data = data[end:]
		if complete {
			data = data[1:]
		}
	}
	if final {
		if !work.State.Discarding {
			if err := parse(work.State.Tail); err != nil {
				return work, err
			}
		}
		work.State.Tail = nil
		work.State.Discarding = false
	}
	return work, nil
}
