package lake

import (
	"github.com/olup/porcupine/internal/event"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestNativePostgresMetadataTimeoutPolicy(t *testing.T) {
	t.Parallel()
	base := "postgres://operator:test-secret@localhost/lake?sslmode=require&connect_timeout=0&keepalives=0&tcp_user_timeout=0"
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	q := parsed.Query()
	q.Set("options", `-c application_name=agent\ with\ spaces --statement-timeout=0 -clock_timeout=0 -c search_path=public`)
	parsed.RawQuery = q.Encode()
	protected, err := nativePostgresMetadataURL(parsed.String(), 10)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := url.Parse(protected)
	if err != nil {
		t.Fatal(err)
	}
	got := normalized.Query()
	for key, want := range map[string]string{"connect_timeout": "10", "keepalives": "1", "keepalives_idle": "10", "keepalives_interval": "1", "keepalives_count": "3", "tcp_user_timeout": "10000", "sslmode": "require"} {
		if got.Get(key) != want {
			t.Fatalf("native metadata setting %s was not enforced", key)
		}
	}
	args, err := splitPostgresOptions(got.Get("options"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-c", "application_name=agent with spaces", "-c", "search_path=public", "-c", "statement_timeout=10000", "-c", "lock_timeout=5000"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("native startup option boundaries changed: %v", args)
	}
	if parsed.Query().Get("connect_timeout") != "0" {
		t.Fatal("mutated operator URI settings")
	}
	rawOptions := `-c application_name=agent\ -c\ statement_timeout=0`
	options, err := boundedPostgresOptions(rawOptions, 2)
	if err != nil {
		t.Fatal(err)
	}
	args, err = splitPostgresOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	if args[1] != "application_name=agent -c statement_timeout=0" || args[len(args)-3] != "statement_timeout=2000" {
		t.Fatal("an escaped option value swallowed enforced settings")
	}
}

func TestNativePostgresMetadataRejectsAmbiguousOptions(t *testing.T) {
	t.Parallel()
	base := "postgres://operator:test-secret@localhost/lake"
	for _, raw := range []string{
		base + "?host=first,second", base + "?hostaddr=127.0.0.1%2C127.0.0.2", base + "?port=5432,5433",
		"postgres://first,second/lake", base + "?host=first&host=second", base + "?options=-c+statement_timeout%3D0&options=-c+lock_timeout%3D0",
		base + "?options=-c", base + "?options=-d+5", base + "?options=-c+%3Dvalue", base + "?options=-c+application_name%3Dbad%5C",
	} {
		_, err := nativePostgresMetadataURL(raw, 10)
		if err == nil {
			t.Fatal("accepted ambiguous native PostgreSQL URL")
		}
		if strings.Contains(err.Error(), "test-secret") {
			t.Fatal("native metadata validation exposed a credential")
		}
	}
	for _, seconds := range []int{-1, 0, 1, 61} {
		if _, err := nativePostgresMetadataURL(base, seconds); err == nil {
			t.Fatal("accepted invalid metadata timeout", seconds)
		}
	}
	config := DuckLakeConfig{PostgresURL: base, SharedSourceID: event.NewID(), S3: &DuckLakeS3Config{}}
	if err := ValidateSharedNativeConfig(&config); err != nil || config.MetadataTimeoutSeconds != 10 {
		t.Fatal("native metadata default was not normalized", err)
	}
	if err := ValidateSharedNativeConfig(&DuckLakeConfig{MetadataTimeoutSeconds: 10}); err == nil {
		t.Fatal("local mode accepted a remote metadata setting")
	}
}
