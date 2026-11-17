// Command test-shard runs an automatically discovered portion of the backend
// race suite and verifies that the combined reports cover every test once.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"time"
)

const backend = "./internal/porcupine"

type report struct {
	Index     int               `json:"index"`
	Count     int               `json:"count"`
	Inventory []string          `json:"inventory"`
	Results   map[string]string `json:"results"`
	Seconds   float64           `json:"seconds"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "verify" {
		return verifyFiles(args[1:])
	}
	flags := flag.NewFlagSet("test-shard", flag.ContinueOnError)
	index := flags.Int("index", 0, "zero-based shard index")
	count := flags.Int("count", 2, "number of shards")
	output := flags.String("report", "", "write the inventory and terminal test results here")
	log := flags.String("log", "", "write the original Go test JSON stream here")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *count < 1 || *index < 0 || *index >= *count || *output == "" || *log == "" || flags.NArg() != 0 {
		return errors.New("require 0 <= index < count, -report and -log")
	}
	start := time.Now()
	listing := goCommand("test", "-race", "-list", ".", "-run", "^$", backend)
	listing.Stderr = os.Stderr
	data, err := listing.Output()
	if err != nil {
		return fmt.Errorf("discover backend tests: %w", err)
	}
	inventory := discover(string(data))
	selected := selectTests(inventory, *index, *count)
	if len(selected) == 0 {
		return errors.New("empty test shard")
	}
	pattern := make([]string, len(selected))
	for i, name := range selected {
		pattern[i] = regexp.QuoteMeta(name)
	}
	command := goCommand("test", "-race", "-json", backend, "-run", "^("+strings.Join(pattern, "|")+")$", "-count=1", "-timeout=600s")
	command.Stderr = os.Stderr
	stream, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	file, err := os.Create(*log)
	if err != nil {
		return err
	}
	defer file.Close()
	fmt.Printf("Backend race shard %d/%d: %d of %d discovered tests\n", *index+1, *count, len(selected), len(inventory))
	if err := command.Start(); err != nil {
		return err
	}
	results, parseErr := collect(io.TeeReader(stream, file))
	if parseErr != nil {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if err := errors.Join(parseErr, waitErr); err != nil {
		return fmt.Errorf("backend race shard failed; see %s: %w", *log, err)
	}
	r := report{Index: *index, Count: *count, Inventory: inventory, Results: results, Seconds: time.Since(start).Seconds()}
	if err := validateResults(r); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, append(encoded, '\n'), 0600); err != nil {
		return err
	}
	fmt.Printf("Verified %d terminal results in %.2fs, including discovery/compilation\n", len(results), r.Seconds)
	return nil
}

func goCommand(args ...string) *exec.Cmd {
	command := exec.Command("go", args...)
	command.Env = append(os.Environ(), "CGO_ENABLED=1")
	return command
}

func discover(output string) []string {
	var tests []string
	for _, name := range strings.Fields(output) {
		if strings.HasPrefix(name, "Test") || strings.HasPrefix(name, "Example") || strings.HasPrefix(name, "Fuzz") {
			tests = append(tests, name)
		}
	}
	slices.Sort(tests)
	return slices.Compact(tests)
}

func selectTests(inventory []string, index, count int) []string {
	var selected []string
	for _, name := range inventory {
		hash := fnv.New32a()
		_, _ = hash.Write([]byte(name))
		if int(uint64(hash.Sum32())%uint64(count)) == index {
			selected = append(selected, name)
		}
	}
	return selected
}

func collect(stream io.Reader) (map[string]string, error) {
	results := map[string]string{}
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	for scanner.Scan() {
		var event struct {
			Action, Test, Output string
			Elapsed              float64
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("invalid Go test event: %w", err)
		}
		if event.Action == "output" && strings.Contains(event.Output, "--- FAIL:") {
			fmt.Fprint(os.Stderr, event.Output)
		}
		if event.Test == "" {
			continue
		}
		if event.Action == "pass" || event.Action == "skip" || event.Action == "fail" {
			if _, duplicate := results[event.Test]; duplicate {
				return nil, fmt.Errorf("duplicate terminal result for %s", event.Test)
			}
			results[event.Test] = event.Action
			if event.Elapsed >= 1 {
				fmt.Printf("%s %s %.2fs\n", event.Action, event.Test, event.Elapsed)
			}
		}
	}
	return results, scanner.Err()
}

func validateResults(r report) error {
	if r.Count < 1 || r.Index < 0 || r.Index >= r.Count || len(r.Inventory) == 0 || !slices.IsSorted(r.Inventory) || len(slices.Compact(slices.Clone(r.Inventory))) != len(r.Inventory) {
		return errors.New("invalid shard inventory")
	}
	selected := selectTests(r.Inventory, r.Index, r.Count)
	if len(selected) == 0 {
		return errors.New("empty test shard")
	}
	for name, result := range r.Results {
		parent, _, _ := strings.Cut(name, "/")
		if !slices.Contains(selected, parent) || (result != "pass" && result != "skip") {
			return fmt.Errorf("unexpected or unsuccessful result for %s: %q", name, result)
		}
	}
	for _, name := range selected {
		if result := r.Results[name]; result != "pass" && result != "skip" {
			return fmt.Errorf("missing or unsuccessful test %s: %q", name, result)
		}
	}
	return nil
}

func verifyFiles(paths []string) error {
	var reports []report
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var r report
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&r); err != nil {
			return err
		}
		reports = append(reports, r)
	}
	if err := verifyReports(reports); err != nil {
		return err
	}
	fmt.Printf("All %d shards cover %d Test/Example/Fuzz entries exactly once\n", len(reports), len(reports[0].Inventory))
	return nil
}

func verifyReports(reports []report) error {
	if len(reports) == 0 || reports[0].Count != len(reports) {
		return errors.New("missing shard reports")
	}
	seen := map[int]bool{}
	for _, r := range reports {
		if r.Count != len(reports) || seen[r.Index] || !slices.Equal(r.Inventory, reports[0].Inventory) {
			return errors.New("duplicate shard or inconsistent inventory")
		}
		if err := validateResults(r); err != nil {
			return err
		}
		seen[r.Index] = true
	}
	return nil
}
