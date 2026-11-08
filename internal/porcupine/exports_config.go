package porcupine

import "errors"

func normalizeExportConfig(c ExportConfig) (ExportConfig, error) {
	if c.MaxExecutionSeconds == 0 {
		c.MaxExecutionSeconds = 86400
	}
	if c.MaxReadBytes == 0 {
		c.MaxReadBytes = 1 << 40
	}
	if c.MaxOutputBytes == 0 {
		c.MaxOutputBytes = 1 << 40
	}
	if c.FileBytes == 0 {
		c.FileBytes = 256 << 20
	}
	if c.MaxRowBytes == 0 {
		c.MaxRowBytes = 8 << 20
	}
	if c.MemoryBytes == 0 {
		c.MemoryBytes = 512 << 20
	}
	if c.BandwidthBytesPerSecond == 0 {
		c.BandwidthBytesPerSecond = 5 << 20
	}
	if c.TemporaryBytes == 0 {
		c.TemporaryBytes = 1 << 30
	}
	if c.QueueThreshold == 0 {
		c.QueueThreshold = 1000
	}
	if c.MaxQueuedJobs == 0 {
		c.MaxQueuedJobs = 100
	}
	if c.RetentionHours == 0 {
		c.RetentionHours = 168
	}
	if c.MaxExecutionSeconds < 1 || c.MaxExecutionSeconds > 604800 ||
		c.MaxReadBytes < 1 || c.MaxReadBytes > 1<<50 ||
		c.MaxOutputBytes < 1 || c.MaxOutputBytes > 1<<50 ||
		c.FileBytes < 1024 || c.FileBytes > 1<<30 ||
		c.MaxRowBytes < 1024 || c.MaxRowBytes > 64<<20 ||
		c.MemoryBytes < 64<<20 || c.MemoryBytes > 2<<30 ||
		c.BandwidthBytesPerSecond < 64<<10 || c.BandwidthBytesPerSecond > 100<<20 ||
		c.TemporaryBytes < 1<<20 || c.TemporaryBytes > 10<<30 ||
		c.QueueThreshold < 1 || c.QueueThreshold > 100000 ||
		c.MaxQueuedJobs < 1 || c.MaxQueuedJobs > 100 || c.RetentionHours < 1 || c.RetentionHours > 720 {
		return c, errors.New("invalid deployment export budgets")
	}
	return c, nil
}
