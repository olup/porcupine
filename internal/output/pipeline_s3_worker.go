package output

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

const scheduledMaxAttempts = 3

func (s *PipelineJobs) RunS3(ctx context.Context) (bool, error) {
	if !s.Enabled() || !s.s3StepMu.TryLock() {
		return false, nil
	}
	defer s.s3StepMu.Unlock()
	if s.pg == nil && s.lake == nil || s.pg != nil && !s.pg.scheduled.ready() {
		return false, nil
	}
	if s.pg != nil && s.beginActivity != nil {
		work, finish, active := s.beginActivity(ctx, "")
		if !active {
			return false, nil
		}
		defer finish()
		ctx = work
	}
	release, ok := s.acquire(ctx)
	if !ok {
		return false, nil
	}
	defer release()
	if s.pg != nil {
		return s.runHostedScheduled(ctx)
	}
	return s.runSQLiteScheduled(ctx)
}

func scheduledFailure(ctx context.Context, attempts int, authorityRevoked bool) (state, code string) {
	state, code = "queued", "attempt_failed"
	if attempts >= scheduledMaxAttempts {
		state, code = "failed", "attempts_exhausted"
	}
	if authorityRevoked {
		state, code = "failed", "creator_unauthorized"
	}
	if ctx.Err() != nil {
		code = "interrupted"
	}
	return state, code
}

func (s *PipelineJobs) buildScheduledManifest(ctx context.Context, w *s3PipelineWork, hosted *hostedScheduledWork, store *exportstore.Store) (pipelineS3Manifest, error) {
	sink := newPipelineS3Sink(ctx, s, w, hosted, store)
	callbacks := lakeapi.ExportStreamCallbacks{Columns: sink.setColumns, Row: sink.writeRow}
	window := lakeapi.AnalyticalWindow{StartMS: w.start, EndMS: w.end}
	maxRowBytes := s.config.ExportLimits.MaxRowBytes
	var err error
	if hosted == nil {
		_, err = s.lake.Export(ctx, w.pipeline.ProjectID, w.spec.SQL, w.spec.Parameters, &window, maxRowBytes, callbacks)
	} else {
		_, err = s.pg.scheduled.Reader.ExportWindowAtSnapshot(ctx, w.pipeline.ProjectID, w.spec.SQL, w.spec.Parameters, window, hosted.snapshot, maxRowBytes, callbacks)
	}
	if err != nil {
		return pipelineS3Manifest{}, err
	}
	return sink.finishManifest()
}

func pipelineManifestKey(pipeline string, start int64) string {
	return fmt.Sprintf("%s/%d/manifest.json", pipeline, start)
}

func encodePipelineManifest(manifest pipelineS3Manifest) ([]byte, error) {
	encoded, err := json.Marshal(manifest)
	if err != nil || len(encoded) > exportstore.MaxManifestBytes {
		return nil, errExportOutputLimit
	}
	return encoded, nil
}

func decodePipelineManifest(w *s3PipelineWork, encoded []byte) (pipelineS3Manifest, bool) {
	var m pipelineS3Manifest
	ok := json.Unmarshal(encoded, &m) == nil && m.ProjectID == w.pipeline.ProjectID && m.PipelineID == w.pipeline.ID &&
		m.RunID == w.id && m.WindowStartMS == w.start && m.WindowEndMS == w.end
	return m, ok
}

// The conditional PUT is the only remote commit. A retry settles the existing
// result only if it can read back the exact bytes in its durable checkpoint.
func commitPipelineManifest(ctx context.Context, store *exportstore.Store, key string, encoded []byte, conflict error) error {
	remote, err := store.ReadManifest(ctx, key)
	if errors.Is(err, exportstore.ErrNotFound) {
		if err = store.CommitManifest(ctx, key, encoded); err != nil && !errors.Is(err, exportstore.ErrExists) {
			return err
		}
		remote, err = store.ReadManifest(ctx, key)
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(remote, encoded) {
		return conflict
	}
	return nil
}

func pipelineManifestBytes(m pipelineS3Manifest) int64 {
	var n int64
	for _, f := range m.Files {
		n += f.Bytes
	}
	return n
}
