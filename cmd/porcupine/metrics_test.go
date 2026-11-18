package main

import (
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
)

func TestMetricsListenSettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		json, address string
		invalid       bool
	}{
		{json: `{}`},
		{json: `{"metrics":{"enabled":false,"listen":"0.0.0.0:9000"}}`},
		{json: `{"metrics":{"enabled":true}}`, address: "127.0.0.1:9090"},
		{json: `{"metrics":{"enabled":true,"listen":"10.0.0.1:9000"}}`, address: "10.0.0.1:9000"},
		{json: `{"metrics":{"enabled":true,"listen":"[::1]:9000"}}`, address: "[::1]:9000"},
		{json: `{"metrics":{"enabled":true,"listen":"[fd00::1]:9000"}}`, address: "[fd00::1]:9000"},
		{json: `{"metrics":{"enabled":true,"listen":"[::ffff:192.168.0.8]:9000"}}`, address: "[::ffff:192.168.0.8]:9000"},
		{json: `{"metrics":{"enabled":true,"listen":"0.0.0.0:9000"}}`, invalid: true},
		{json: `{"metrics":{"enabled":true,"listen":"[::]:9000"}}`, invalid: true},
		{json: `{"metrics":{"enabled":true,"listen":"8.8.8.8:9000"}}`, invalid: true},
		{json: `{"metrics":{"enabled":true,"listen":"my-interface:9000"}}`, invalid: true},
		{json: `{"metrics":{"enabled":true,"listen":"127.0.0.1:65536"}}`, invalid: true},
	}
	for _, tt := range tests {
		t.Run(tt.json, func(t *testing.T) {
			t.Parallel()
			s, err := readSettings(strings.NewReader(tt.json))
			if err != nil {
				t.Fatal(err)
			}
			address, err := s.metricsListenAddress()
			if tt.invalid != (err != nil) || address != tt.address {
				t.Errorf("address = %q, err = %v; want %q, invalid = %v", address, err, tt.address, tt.invalid)
			}
		})
	}
}

func TestServerFailureReasonsOmitRawError(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		err    error
		reason string
	}{
		{fmt.Errorf("private address: %w", syscall.EADDRINUSE), "address_in_use"},
		{fmt.Errorf("private file: %w", syscall.EACCES), "permission_denied"},
		{errors.New("PORCUPINE_BOOTSTRAP_TOKEN must contain at least 16 characters on first shared initialization"), "bootstrap_token_required"},
		{errors.New("private SQL and credentials"), "unclassified"},
	} {
		if reason := serverFailureReason(tt.err); reason != tt.reason {
			t.Errorf("reason = %q, want %q", reason, tt.reason)
		}
	}
}
