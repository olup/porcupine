// Package testfixtures keeps integration runner selections and their required
// services together. Tests check this selection before skipping a missing fixture.
package testfixtures

import (
	"fmt"
	"regexp"
	"strings"
)

type Fixture string

const (
	S3       Fixture = "S3"
	Postgres Fixture = "PostgreSQL"
	NativeS3         = "s3"
	Shared           = "shared"
)

var suites = [...]struct {
	name     string
	pattern  *regexp.Regexp
	fixtures []Fixture
}{
	{NativeS3, regexp.MustCompile("^TestDuckLakeS3"), []Fixture{S3}},
	{Shared, regexp.MustCompile("^(TestShared|TestDuckLakeS3SharedPostgres)"), []Fixture{S3, Postgres}},
}

func Pattern(suite string) (string, error) {
	for _, candidate := range suites {
		if candidate.name == suite {
			return candidate.pattern.String(), nil
		}
	}
	return "", fmt.Errorf("unknown integration suite %q", suite)
}

// CheckSelection rejects tests that would skip without a fixture but would never
// run in a CI suite providing it. Call only when the fixture is unavailable;
// explicitly launched child processes can have their own test names.
func CheckSelection(testName string, fixture Fixture) error {
	root, _, _ := strings.Cut(testName, "/")
	var providers []string
	for _, suite := range suites {
		for _, provided := range suite.fixtures {
			if provided != fixture {
				continue
			}
			if suite.pattern.MatchString(root) {
				return nil
			}
			providers = append(providers, suite.name+"="+suite.pattern.String())
		}
	}
	return fmt.Errorf("%s requires %s but no CI fixture suite selects it; selections: %s", root, fixture, strings.Join(providers, ", "))
}
