// Command sqlite-identities reads synthetic identity state for SDK contract tests.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"

	_ "modernc.org/sqlite"
)

type control struct {
	ID    int64  `json:"id"`
	UUID  string `json:"uuid"`
	Event string `json:"event"`
}

func main() {
	if len(os.Args) < 3 || len(os.Args) > 4 {
		fmt.Fprintln(os.Stderr, "usage: sqlite-identities database project [controls-json]")
		os.Exit(1)
	}
	controls := []control{}
	if len(os.Args) == 4 {
		if err := json.Unmarshal([]byte(os.Args[3]), &controls); err != nil {
			fail(err)
		}
	}
	result, err := snapshot(os.Args[1], os.Args[2], controls)
	if err != nil {
		fail(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
func snapshot(path, project string, controls []control) (map[string]any, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	uri := url.URL{Scheme: "file", Path: absolute, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	result := map[string]any{}
	for _, table := range []string{"identity_profiles", "identity_links"} {
		records, err := readRows(db, table, project)
		if err != nil {
			return nil, err
		}
		result[table] = records
	}
	found := []control{}
	for _, item := range controls {
		err := db.QueryRow(`SELECT id FROM events WHERE project_id=? AND uuid=? UNION ALL SELECT event_id FROM event_receipts WHERE project_id=? AND uuid=? LIMIT 1`, project, item.UUID, project, item.UUID).Scan(&item.ID)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, err
		}
		found = append(found, item)
	}
	sort.Slice(found, func(i, j int) bool { return found[i].ID < found[j].ID })
	result["control_events"] = found
	return result, nil
}
func readRows(db *sql.DB, table, project string) ([]map[string]any, error) {
	rows, err := db.Query("SELECT * FROM "+table+" WHERE project_id=?", project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		record := map[string]any{}
		for i, column := range columns {
			value := values[i]
			if data, ok := value.([]byte); ok {
				value = string(data)
			}
			if column == "traits" {
				var traits any
				if err := json.Unmarshal([]byte(fmt.Sprint(value)), &traits); err != nil {
					return nil, err
				}
				value = traits
			}
			record[column] = value
		}
		result = append(result, record)
	}
	return result, rows.Err()
}
