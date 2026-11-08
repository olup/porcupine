package porcupine

import "testing"

func TestExportDeploymentBudgets(t *testing.T) {
	c, err := normalizeExportConfig(ExportConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if c.MemoryBytes != 512<<20 || c.BandwidthBytesPerSecond != 5<<20 || c.FileBytes != 256<<20 || c.MaxQueuedJobs != 100 {
		t.Fatalf("unsafe defaults: %+v", c)
	}
	cases := []ExportConfig{
		{MaxExecutionSeconds: 604801}, {MaxExecutionSeconds: -1}, {MaxReadBytes: -1}, {MaxOutputBytes: -1},
		{FileBytes: 1}, {MaxRowBytes: 65 << 20}, {MemoryBytes: 3 << 30}, {BandwidthBytesPerSecond: 101 << 20},
		{TemporaryBytes: 11 << 30}, {QueueThreshold: -1}, {MaxQueuedJobs: 101}, {RetentionHours: 721},
	}
	for _, bad := range cases {
		if _, err := normalizeExportConfig(bad); err == nil {
			t.Fatalf("accepted unbounded export config: %+v", bad)
		}
	}
}

func TestExportsRequireClickHouse(t *testing.T) {
	if s, err := Open(Config{DBPath: ":memory:", BootstrapToken: "test-bootstrap-token", Exports: &ExportConfig{}}); err == nil {
		s.Close()
		t.Fatal("exports enabled without ClickHouse")
	}
}
