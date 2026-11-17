package porcupine

import "github.com/olup/porcupine/internal/output"

import "testing"

func TestExportDeploymentBudgets(t *testing.T) {
	t.Parallel()
	c, err := output.NormalizeExportConfig(output.ExportConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if c.BandwidthBytesPerSecond != 5<<20 || c.FileBytes != 256<<20 || c.MaxQueuedJobs != 100 {
		t.Fatalf("unsafe defaults: %+v", c)
	}
	cases := []output.ExportConfig{
		{MaxExecutionSeconds: 604801}, {MaxExecutionSeconds: -1}, {MaxOutputBytes: -1},
		{FileBytes: 1}, {MaxRowBytes: 65 << 20}, {BandwidthBytesPerSecond: 101 << 20},
		{QueueThreshold: -1}, {MaxQueuedJobs: 101}, {RetentionHours: 721},
	}
	for _, bad := range cases {
		if _, err := output.NormalizeExportConfig(bad); err == nil {
			t.Fatalf("accepted unbounded export config: %+v", bad)
		}
	}
}

func TestExportsRequireAnalyticalStorage(t *testing.T) {
	t.Parallel()
	if s, err := Open(Config{DBPath: ":memory:", BootstrapToken: "test-bootstrap-token", Exports: &output.ExportConfig{}}); err == nil {
		s.Close()
		t.Fatal("exports enabled without analytical storage")
	}
}
