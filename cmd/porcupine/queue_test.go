package main

import (
	"testing"

	"github.com/olup/porcupine/internal/porcupine"
)

func TestQueueByteEnvironment(t *testing.T) {
	for _, test := range []struct {
		raw   string
		want  int64
		valid bool
	}{
		{"", porcupine.DefaultQueueMaxBytes, true}, {"1", 1, true}, {"268435456", 268435456, true},
		{"0", 0, false}, {"-1", 0, false}, {"256MiB", 0, false}, {"9223372036854775808", 0, false},
	} {
		t.Setenv("PORCUPINE_QUEUE_MAX_BYTES", test.raw)
		got, err := queueMaxBytesFromEnvironment()
		if (err == nil) != test.valid || test.valid && got != test.want {
			t.Errorf("queue bytes %q: value=%d error=%v", test.raw, got, err)
		}
	}
}
