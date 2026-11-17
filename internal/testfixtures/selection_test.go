package testfixtures

import (
	"strings"
	"testing"
)

func TestCheckSelection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		fixture Fixture
		valid   bool
	}{
		{"TestDuckLakeS3Storage", S3, true},
		{"TestDuckLakeS3SharedPostgresStorage", S3, true},
		{"TestDuckLakeS3SharedPostgresStorage", Postgres, true},
		{"TestSharedUsage", Postgres, true},
		{"TestSharedS3Storage", S3, true},
		{"TestSharedUsage/nested/case", Postgres, true},
		{"TestTenantS3Storage", S3, false},
		{"TestUsagePostgres", Postgres, false},
		// A dual-fixture test selected only by the S3 suite must not skip PG.
		{"TestDuckLakeS3NeedsPostgres", S3, true},
		{"TestDuckLakeS3NeedsPostgres", Postgres, false},
		// Matching a child name does not select its unselected parent in go test.
		{"TestTenant/TestSharedUsage", Postgres, false},
		{"TestSharedUsage", Fixture("unknown"), false},
	} {
		t.Run(tc.name+"/"+string(tc.fixture), func(t *testing.T) {
			t.Parallel()
			err := CheckSelection(tc.name, tc.fixture)
			if (err == nil) != tc.valid {
				t.Fatalf("CheckSelection(%q, %q) = %v, valid=%t", tc.name, tc.fixture, err, tc.valid)
			}
			if err != nil && (!strings.Contains(err.Error(), strings.Split(tc.name, "/")[0]) || !strings.Contains(err.Error(), string(tc.fixture))) {
				t.Fatalf("selection error omits test or fixture: %v", err)
			}
		})
	}
}

func TestRunnerPatterns(t *testing.T) {
	t.Parallel()
	for _, suite := range suites {
		pattern, err := Pattern(suite.name)
		if err != nil || pattern != suite.pattern.String() {
			t.Fatalf("runner pattern for %q = %q, %v", suite.name, pattern, err)
		}
	}
	if _, err := Pattern("typo"); err == nil {
		t.Fatal("unknown suite accepted")
	}
}
