package lake

import "time"

type NativeBackupConfig struct {
	Directory string
	S3        *DuckLakeS3Config
	Interval  time.Duration
	Timeout   time.Duration
	Retain    int
}
