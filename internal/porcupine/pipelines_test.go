package porcupine

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/output"
)

func TestExistingPipelinesRequireSameKeyAfterRestart(t *testing.T) {
	t.Parallel()
	c := Config{DBPath: filepath.Join(t.TempDir(), "pipeline.db"), BootstrapToken: "test-pipeline-bootstrap", Pipelines: &output.PipelineConfig{Key: bytes.Repeat([]byte{1}, 32)}}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	created := request(t, s, "POST", "/api/v1/projects", c.BootstrapToken, `{"name":"pipeline fixture"}`, 201)
	projectID := created["project"].(map[string]any)["id"].(string)
	result, err := s.db.Exec(`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms)
SELECT 'd',$1,id,'webhook','test','paused','{}',X'00',0 FROM access_keys LIMIT 1`, projectID)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatal("pipeline fixture was not created", rows, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	key := c.Pipelines
	for _, cfg := range []*output.PipelineConfig{nil, {Key: bytes.Repeat([]byte{2}, 32)}} {
		c.Pipelines = cfg
		if bad, err := Open(c); err == nil {
			bad.Close()
			t.Fatal("startup accepted missing or mismatched key")
		}
	}
	c.Pipelines = key
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}

func TestBatchQueriesShareOneSlot(t *testing.T) {
	t.Parallel()
	s := &Service{querySlots: make(chan struct{}, 2), batchSlot: make(chan struct{}, 1)}
	ctx := context.Background()
	release, ok := s.acquirePipelineExport(ctx)
	if !ok {
		t.Fatal("idle batch refused")
	}
	if _, ok = s.acquirePipelineExport(ctx); ok {
		t.Fatal("concurrent heavy export admitted")
	}
	select {
	case s.querySlots <- struct{}{}:
	default:
		t.Fatal("interactive query starved")
	}
	release()
	if _, ok = s.acquirePipelineExport(ctx); ok {
		t.Fatal("batch started during interactive use")
	}
	<-s.querySlots
	release, ok = s.acquirePipelineExport(ctx)
	if !ok {
		t.Fatal("batch slot leaked")
	}
	release()
}

func TestClosedWindowWaitsForEarlierProjectAdmissions(t *testing.T) {
	t.Parallel()
	rt, err := newTestPipelineConfig(&output.PipelineConfig{Key: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	s := duckLakeJourneyService(t)
	attachTestPipelines(s, rt)
	s.clock = func() time.Time { return time.UnixMilli(59999) }
	at, done := s.pipelines.BeginReceipt("p")
	if at.UnixMilli() != 59999 || !s.pipelines.WindowHasAdmissions("p", 0, 60000) {
		t.Fatal("lost pre-boundary admission")
	}
	if s.pipelines.WindowHasAdmissions("p", 60000, 120000) || s.pipelines.WindowHasAdmissions("another", 0, 60000) {
		t.Fatal("unrelated window blocked")
	}
	done()
	if s.pipelines.WindowHasAdmissions("p", 0, 60000) {
		t.Fatal("completed capture held the watermark")
	}
}
