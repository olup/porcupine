package lake

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/nativequery"
)

type lakeSQLContract struct {
	Tables []struct {
		Name    string                  `json:"name"`
		Columns []AnalyticsResultColumn `json:"columns"`
	} `json:"tables"`
}

func readLakeSQLContract(t *testing.T) lakeSQLContract {
	t.Helper()
	data, err := os.ReadFile("../porcupine/testdata/sql_contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract lakeSQLContract
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	return contract
}

func TestSQLContractDuckLakeEvents(t *testing.T) {
	t.Parallel()
	contract := readLakeSQLContract(t)
	if len(contract.Tables) != 1 || contract.Tables[0].Name != "events" || len(contract.Tables[0].Columns) == 0 {
		t.Fatalf("expected one events table with columns in the contract, got %+v", contract.Tables)
	}
	want := contract.Tables[0].Columns

	store := openLakeStorageTest(t, t.TempDir(), 1)
	ctx := t.Context()
	const project = "sql-contract"
	event := lakeStorageProjection(t, project, "one", 1)
	if err := store.Apply(ctx, []EventProjection{event}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.CaptureExportSnapshot(ctx, project)
	if err != nil {
		t.Fatal(err)
	}

	assertSQLContractColumns(t, "DuckLake events view", describeSQLContractEvents(t, store, project), want)

	const query = "SELECT * FROM events"
	interactive, err := store.Query(ctx, project, query, nil, DefaultQueryExecutionLimits())
	if err != nil || interactive.Rows != 1 {
		t.Fatalf("interactive query returned %d rows, error %v", interactive.Rows, err)
	}
	assertSQLContractColumns(t, "interactive events", interactive.Meta, want)

	var exportColumns []AnalyticsResultColumn
	callbacks := ExportStreamCallbacks{
		Columns: func(columns []AnalyticsResultColumn) error {
			exportColumns = columns
			return nil
		},
		Row: func([]json.RawMessage) error { return nil },
	}
	current, err := store.Export(ctx, project, query, nil, nil, 4096, callbacks)
	if err != nil || current.Rows != 1 {
		t.Fatalf("live export returned %d rows, error %v", current.Rows, err)
	}
	assertSQLContractColumns(t, "live export events", exportColumns, want)
	exportColumns = nil
	pinned, err := store.ExportAtSnapshot(ctx, project, query, nil, snapshot, 4096, callbacks)
	if err != nil || pinned.Rows != 1 {
		t.Fatalf("snapshot export returned %d rows, error %v", pinned.Rows, err)
	}
	assertSQLContractColumns(t, "snapshot export events", exportColumns, want)

	received, err := time.Parse(time.RFC3339Nano, event.ReceivedAt)
	if err != nil {
		t.Fatal(err)
	}
	window := AnalyticalWindow{StartMS: received.UnixMilli(), EndMS: received.Add(time.Second).UnixMilli()}
	exportColumns = nil
	scheduled, err := store.ExportWindowAtSnapshot(ctx, project, query, nil, window, snapshot, 4096, callbacks)
	if err != nil || scheduled.Rows != 1 {
		t.Fatalf("scheduled snapshot export returned %d rows, error %v", scheduled.Rows, err)
	}
	assertSQLContractColumns(t, "scheduled snapshot export events", exportColumns, want)
}

func describeSQLContractEvents(t *testing.T, store *DuckLakeStore, project string) []AnalyticsResultColumn {
	t.Helper()
	ctx := t.Context()
	projectLake, release, err := store.LeaseProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	rows, err := projectLake.DB.QueryContext(ctx, `SELECT column_name, data_type FROM information_schema.columns
		WHERE table_catalog = current_database() AND table_schema = 'main' AND table_name = 'events'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []AnalyticsResultColumn
	for rows.Next() {
		var column AnalyticsResultColumn
		if err := rows.Scan(&column.Name, &column.Type); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return columns
}

func assertSQLContractColumns(t *testing.T, source string, got, want []AnalyticsResultColumn) {
	t.Helper()
	actual := make(map[string]string, len(got))
	for _, column := range got {
		if _, exists := actual[column.Name]; exists {
			t.Errorf("%s returned column %q more than once", source, column.Name)
		}
		// The Go driver uses the short spelling for this DuckDB type.
		if column.Type == "TIMESTAMPTZ" {
			column.Type = "TIMESTAMP WITH TIME ZONE"
		}
		actual[column.Name] = column.Type
	}
	for _, column := range want {
		typ, exists := actual[column.Name]
		if !exists {
			t.Errorf("%s is missing column %s (%s)", source, column.Name, column.Type)
		} else if typ != column.Type {
			t.Errorf("%s column %s has type %s, want %s", source, column.Name, typ, column.Type)
		}
		delete(actual, column.Name)
	}
	for name, typ := range actual {
		t.Errorf("%s returned undocumented column %s (%s)", source, name, typ)
	}
}

func TestSQLContractDuckLakeFunctions(t *testing.T) {
	t.Parallel()
	functions := nativequery.Functions()
	if len(functions) == 0 {
		t.Fatal("the SQL contract lists no functions")
	}
	store := openLakeStorageTest(t, t.TempDir(), 1)
	ctx := t.Context()
	projectLake, release, err := store.LeaseProject(ctx, "sql-contract")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	rows, err := projectLake.DB.QueryContext(ctx, `SELECT DISTINCT function_name FROM duckdb_functions()
		WHERE function_type IN ('scalar', 'aggregate', 'macro', 'window')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	catalog := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		catalog[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	// DuckDB's parser implements these forms using other catalog names or AST nodes.
	parserSyntax := map[string]bool{
		"ifnull": true, "coalesce": true, "extract": true, "current_timestamp": true,
		"->": true, "!~": true,
	}
	for _, name := range functions {
		if !catalog[name] && !parserSyntax[name] {
			t.Errorf("advertised SQL function %q is missing from duckdb_functions()", name)
		}
		expression, exists := sqlContractExamples[name]
		if !exists {
			t.Errorf("advertised SQL entry %q is missing a runnable example", name)
			continue
		}
		assertSQLContractExample(t, projectLake.DB, name, expression)
	}
	assertSQLContractExample(t, projectLake.DB, "LIKE ESCAPE", `'a_b' LIKE 'a$_b' ESCAPE '$'`)
	assertSQLContractExample(t, projectLake.DB, "NOT LIKE ESCAPE", `NOT ('a_b' NOT LIKE 'a$_b' ESCAPE '$')`)
}

func assertSQLContractExample(t *testing.T, db *sql.DB, name, expression string) {
	t.Helper()
	query := "SELECT " + expression
	canonical, args, err := nativequery.Prepare(t.Context(), db, query, nil)
	if err != nil {
		t.Errorf("SQL entry %q failed policy preparation for %q: %v", name, query, err)
		return
	}
	var passed bool
	if err := db.QueryRowContext(t.Context(), canonical, args...).Scan(&passed); err != nil || !passed {
		t.Errorf("SQL entry %q failed its DuckDB example %q, result %t, error %v", name, canonical, passed, err)
	}
}
