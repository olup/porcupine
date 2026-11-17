package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func fixtureReports(count int) []report {
	inventory := []string{"ExampleQuery", "FuzzCapture", "TestIngest", "TestNewFeature", "TestQuery", "TestRevoke"}
	var reports []report
	for index := 0; index < count; index++ {
		r := report{Index: index, Count: count, Inventory: slices.Clone(inventory), Results: map[string]string{}}
		for _, name := range selectTests(inventory, index, count) {
			r.Results[name] = "pass"
		}
		reports = append(reports, r)
	}
	return reports
}

func TestEveryDiscoveredTestHasExactlyOneShard(t *testing.T) {
	t.Parallel()
	inventory := discover("TestIngest\nFuzzCapture\nExampleQuery\nBenchmarkIgnored\nok example 0.1s\nTestIngest\n")
	if !slices.Equal(inventory, []string{"ExampleQuery", "FuzzCapture", "TestIngest"}) {
		t.Fatal(inventory)
	}
	for count := 1; count <= 8; count++ {
		seen := map[string]int{}
		for index := 0; index < count; index++ {
			for _, name := range selectTests(inventory, index, count) {
				seen[name]++
			}
		}
		for _, name := range inventory {
			if seen[name] != 1 {
				t.Fatalf("%s assigned %d times with %d shards", name, seen[name], count)
			}
		}
	}
}

func TestCoverageGateRejectsMissingChangedOrFailedResults(t *testing.T) {
	t.Parallel()
	if err := verifyReports(fixtureReports(2)); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func([]report) []report{
		func(rs []report) []report { return rs[:1] },
		func(rs []report) []report { rs[1] = rs[0]; return rs },
		func(rs []report) []report {
			rs[1].Inventory = append(rs[1].Inventory, "TestUnrunNewFeature")
			return rs
		},
		func(rs []report) []report { delete(rs[0].Results, selectTests(rs[0].Inventory, 0, 2)[0]); return rs },
		func(rs []report) []report { rs[0].Results[selectTests(rs[0].Inventory, 0, 2)[0]] = "fail"; return rs },
		func(rs []report) []report { rs[0].Results["TestUnexpected"] = "pass"; return rs },
	} {
		if err := verifyReports(change(fixtureReports(2))); err == nil {
			t.Fatal("accepted incomplete or invalid test execution")
		}
	}
	if err := verifyReports(nil); err == nil {
		t.Fatal("accepted no reports")
	}
}

func TestCollectTopLevelFuzzExamplesAndSkippedTests(t *testing.T) {
	t.Parallel()
	stream := `{"Action":"run","Test":"FuzzCapture"}
{"Action":"pass","Test":"FuzzCapture/seed#0"}
{"Action":"pass","Test":"FuzzCapture"}
{"Action":"pass","Test":"ExampleQuery"}
{"Action":"skip","Test":"TestNeedsS3"}
{"Action":"pass"}
`
	results, err := collect(strings.NewReader(stream))
	if err != nil || len(results) != 4 || results["FuzzCapture/seed#0"] != "pass" || results["FuzzCapture"] != "pass" || results["ExampleQuery"] != "pass" || results["TestNeedsS3"] != "skip" {
		t.Fatalf("%v %v", results, err)
	}
	if _, err := collect(strings.NewReader(stream + `{"Action":"pass","Test":"FuzzCapture"}`)); err == nil {
		t.Fatal("accepted duplicate execution")
	}
	if _, err := collect(strings.NewReader("invalid json")); err == nil {
		t.Fatal("accepted invalid stream")
	}
}

func TestVerifyReportFiles(t *testing.T) {
	t.Parallel()
	var paths []string
	for _, r := range fixtureReports(2) {
		path := filepath.Join(t.TempDir(), fmt.Sprint(r.Index)+".json")
		data, _ := json.Marshal(r)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	if err := verifyFiles(paths); err != nil {
		t.Fatal(err)
	}
	if err := verifyFiles(append(paths, "missing-report.json")); err == nil {
		t.Fatal("accepted missing file")
	}
}
