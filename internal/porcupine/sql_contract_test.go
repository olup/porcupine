package porcupine

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io/fs"
	"os"
	"regexp"
	"testing"
)

const (
	currentSQLContract  = "testdata/sql_contract.json"
	releasedSQLContract = "testdata/sql_contract_v0.24.0.json"
)

var updateSQLContract = flag.Bool("update-sql-contract", false, "regenerate the current SQL contract from schema discovery")
var sqlDeprecationRelease = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

type sqlContract struct {
	Tables     []AnalyticsTable  `json:"tables"`
	SQL        AnalyticsSQL      `json:"sql"`
	Deprecated map[string]string `json:"deprecated,omitempty"`
}

func TestSQLContractDiscovery(t *testing.T) {
	t.Parallel()
	current, err := os.ReadFile(currentSQLContract)
	var contract sqlContract
	switch {
	case err == nil:
		if err := json.Unmarshal(current, &contract); err != nil {
			t.Fatal(err)
		}
	case !*updateSQLContract || !errors.Is(err, fs.ErrNotExist):
		t.Fatal(err)
	}

	s := service(t, 20)
	id, _ := project(t, s)
	_, token := key(t, s, []string{"events:read"}, []string{id})
	response := request(t, s, "GET", "/api/v1/projects/"+id+"/schema", token, "", 200)
	discovered := map[string]any{"tables": response["tables"], "sql": response["sql"]}
	if len(contract.Deprecated) > 0 {
		discovered["deprecated"] = contract.Deprecated
	}
	generated, err := json.MarshalIndent(discovered, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	generated = append(generated, '\n')
	if bytes.Equal(current, generated) {
		return
	}
	if !*updateSQLContract {
		t.Fatalf("schema discovery differs from %s. Run make sql-contract-generate and review the diff", currentSQLContract)
	}
	if err := os.WriteFile(currentSQLContract, generated, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestSQLContractAdditive(t *testing.T) {
	t.Parallel()
	if releasedSQLContract == "" {
		t.Skip("no released SQL contract yet; the next release records the first baseline")
	}
	released := readSQLContract(t, releasedSQLContract)
	current := readSQLContract(t, currentSQLContract)
	releasedEntries := sqlContractEntries(t, released)
	currentEntries := sqlContractEntries(t, current)
	if current.SQL.Dialect != released.SQL.Dialect {
		t.Errorf("SQL dialect changed from %q to %q", released.SQL.Dialect, current.SQL.Dialect)
	}
	for entry, release := range current.Deprecated {
		_, active := currentEntries[entry]
		_, previouslyActive := releasedEntries[entry]
		if !active && !previouslyActive && released.Deprecated[entry] == "" {
			t.Errorf("SQL deprecation for %q has no matching current or released entry", entry)
		}
		if !sqlDeprecationRelease.MatchString(release) {
			t.Errorf("SQL deprecation for %q requires a stable release tag, got %q", entry, release)
		}
	}
	for entry, releasedType := range releasedEntries {
		if current.Deprecated[entry] != "" {
			continue
		}
		currentType, exists := currentEntries[entry]
		if !exists {
			t.Errorf("%s was removed without a release-tagged deprecation", entry)
		} else if currentType != releasedType {
			t.Errorf("%s changed from %s to %s without a release-tagged deprecation", entry, releasedType, currentType)
		}
	}
}

func readSQLContract(t *testing.T, path string) sqlContract {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var contract sqlContract
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return contract
}

func sqlContractEntries(t *testing.T, contract sqlContract) map[string]string {
	t.Helper()
	entries := make(map[string]string)
	add := func(name, typ string) {
		if _, exists := entries[name]; exists {
			t.Fatalf("duplicate SQL contract entry %q", name)
		}
		entries[name] = typ
	}
	for _, table := range contract.Tables {
		for _, column := range table.Columns {
			add(table.Name+"."+column.Name, column.Type)
		}
	}
	for _, function := range contract.SQL.Functions {
		add("function."+function, "")
	}
	for _, feature := range contract.SQL.Features {
		add("feature."+feature, "")
	}
	return entries
}
