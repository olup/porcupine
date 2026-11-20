package main

import (
	"strings"
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
		got, err := (settings{}).queueMaxBytesFromEnvironment()
		if (err == nil) != test.valid || test.valid && got != test.want {
			t.Errorf("queue bytes %q: value=%d error=%v", test.raw, got, err)
		}
	}
}

func TestProjectQueueSettings(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		file       string
		wantEvents int
		wantBytes  int64
		wantError  string
	}{
		{file: `{}`},
		{file: `{"queue":{"project_max_events":0,"project_max_bytes":0}}`},
		{file: `{"queue":{"project_max_events":123,"project_max_bytes":4294967296}}`, wantEvents: 123, wantBytes: 4294967296},
		{file: `{"queue":{"project_max_events":-1}}`, wantError: "PORCUPINE_QUEUE_PROJECT_MAX_EVENTS"},
		{file: `{"queue":{"project_max_bytes":-1}}`, wantError: "PORCUPINE_QUEUE_PROJECT_MAX_BYTES"},
	} {
		s, err := readSettings(strings.NewReader(test.file))
		if err != nil {
			t.Fatal(err)
		}
		_, c, err := s.serviceConfig()
		if test.wantError != "" {
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Errorf("config %s: expected %s error, got %v", test.file, test.wantError, err)
			}
		} else if err != nil || c.QueueProjectMaxEvents != test.wantEvents || c.QueueProjectMaxBytes != test.wantBytes {
			t.Errorf("config %s: got %d events, %d bytes (%v)", test.file, c.QueueProjectMaxEvents, c.QueueProjectMaxBytes, err)
		}
	}
}
