package main

import "testing"

func TestUsageConfiguration(t *testing.T) {
	t.Parallel()
	file := FileConfig{}
	days, capacity, bytes := 90, 4, int64(8<<20)
	file.Usage.RetentionDays = &days
	file.Usage.MaxSnapshots = &capacity
	file.Usage.MaxSnapshotBytes = &bytes
	c, e := fileSettings(file).usageConfiguration()
	if e != nil || c.RetentionDays != days || c.MaxSnapshots != capacity || c.MaxSnapshotBytes != bytes {
		t.Fatal(c, e)
	}
	for _, v := range []string{"0", "-1", "x", "999999999999999999999999"} {
		if _, e = (settings{"PORCUPINE_USAGE_RETENTION_DAYS": v}).usageConfiguration(); e == nil {
			t.Fatal(v)
		}
	}
}
