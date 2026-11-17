package main

import (
	"fmt"
	"github.com/olup/porcupine/internal/usage"
	"strconv"
)

func (s settings) usageConfiguration() (*usage.Config, error) {
	c := usage.Config{}
	for key, p := range map[string]*int{"PORCUPINE_USAGE_RETENTION_DAYS": &c.RetentionDays, "PORCUPINE_USAGE_MAX_SNAPSHOTS": &c.MaxSnapshots} {
		if raw := s.get(key); raw != "" {
			n, e := strconv.Atoi(raw)
			if e != nil || n < 1 {
				return nil, fmt.Errorf("%s must be a positive integer", key)
			}
			*p = n
		}
	}
	if raw := s.get("PORCUPINE_USAGE_MAX_SNAPSHOT_BYTES"); raw != "" {
		n, e := strconv.ParseInt(raw, 10, 64)
		if e != nil || n < 1 {
			return nil, fmt.Errorf("PORCUPINE_USAGE_MAX_SNAPSHOT_BYTES must be a positive integer")
		}
		c.MaxSnapshotBytes = n
	}
	normalized, e := usage.Normalize(&c)
	return &normalized, e
}
