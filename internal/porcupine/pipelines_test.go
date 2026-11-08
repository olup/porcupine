package porcupine

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestPipelineSecretsBoundToDestination(t *testing.T) {
	rt, err := newPipelineRuntime(&PipelineConfig{Key: bytes.Repeat([]byte{0x37}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{pipelines: rt}
	secret := []byte("Bearer unit-test-only")
	blob, err := s.sealPipelineSecret("p", "d", secret)
	if err != nil || bytes.Contains(blob, secret) {
		t.Fatalf("unexpected seal result: %v", err)
	}
	second, err := s.sealPipelineSecret("p", "d", secret)
	if err != nil || bytes.Equal(blob, second) {
		t.Fatal("nonce reused")
	}
	decoded, err := s.openPipelineSecret("p", "d", blob)
	if err != nil || !bytes.Equal(decoded, secret) {
		t.Fatalf("open: %v", err)
	}
	for _, ids := range [][2]string{{"other", "d"}, {"p", "other"}} {
		if _, err = s.openPipelineSecret(ids[0], ids[1], blob); err == nil {
			t.Fatal("accepted credential moved across destinations")
		}
	}
	blob[len(blob)-1] ^= 1
	if _, err = s.openPipelineSecret("p", "d", blob); err == nil {
		t.Fatal("accepted corrupt credential")
	}
}
func TestPipelineEndpointAllowlist(t *testing.T) {
	rt, err := newPipelineRuntime(&PipelineConfig{Key: make([]byte, 32), AllowedS3Endpoints: []string{"https://STORAGE.example.org/"}})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{pipelines: rt}
	for _, url := range []string{"", "https://storage.example.org"} {
		if err = s.validatePipelineS3Endpoint(url); err != nil {
			t.Fatal(err)
		}
	}
	for _, url := range []string{"https://api.storage.example.org", "http://storage.example.org", "https://storage.example.org/private", "https://storage.example.org:443", "https://storage.example.org?", "https://user@storage.example.org", "https://storage.example.org#ignored"} {
		if err = s.validatePipelineS3Endpoint(url); err == nil {
			t.Fatalf("allowed %q", url)
		}
	}
}
func TestExistingPipelinesRequireSameKeyAfterRestart(t *testing.T) {
	c := Config{DBPath: filepath.Join(t.TempDir(), "pipeline.db"), BootstrapToken: "test-pipeline-bootstrap", Pipelines: &PipelineConfig{Key: bytes.Repeat([]byte{1}, 32)}}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO projects(id,name,created_at) VALUES('p','p','now');
INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms)
SELECT 'd','p',id,'webhook','test','paused','{}',X'00',0 FROM access_keys LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	key := c.Pipelines
	for _, cfg := range []*PipelineConfig{nil, {Key: bytes.Repeat([]byte{2}, 32)}} {
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
	rt, err := newPipelineRuntime(&PipelineConfig{Key: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{pipelines: rt, ch: &clickHouse{}, clock: func() time.Time { return time.UnixMilli(59999) }}
	at, done := s.beginCaptureReceipt("p")
	if at.UnixMilli() != 59999 || !s.pipelineWindowHasAdmissions("p", 0, 60000) {
		t.Fatal("lost pre-boundary admission")
	}
	if s.pipelineWindowHasAdmissions("p", 60000, 120000) || s.pipelineWindowHasAdmissions("another", 0, 60000) {
		t.Fatal("unrelated window blocked")
	}
	done()
	if s.pipelineWindowHasAdmissions("p", 0, 60000) {
		t.Fatal("completed capture held the watermark")
	}
}
