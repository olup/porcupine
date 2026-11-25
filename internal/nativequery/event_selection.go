package nativequery

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
)

// PrepareEventSelection parses one expression and places it in a fixed SELECT's
// WHERE clause before applying the same policy and bindings as an analytics query.
func PrepareEventSelection(ctx context.Context, db *sql.DB, predicate string, parameters map[string]string) (string, []any, Statement, error) {
	return prepareQuery(ctx, db, predicate, parameters, "", inspectEventSelection)
}

const selectionPrefix = "SELECT uuid, revision, id, received_at, is_imported, "
const selectionSuffix = "\n, FALSE FROM events"

func inspectEventSelection(ctx context.Context, db *sql.DB, predicate string) (Statement, error) {
	parsed, _, err := parseSelect(ctx, db, selectionPrefix+predicate+selectionSuffix)
	if err != nil {
		return Statement{}, err
	}
	template, _, err := parseSelect(ctx, db, selectionPrefix+"TRUE"+selectionSuffix)
	if err != nil {
		return Statement{}, err
	}
	node, _ := parsed.Statements[0]["node"].(map[string]any)
	trusted, _ := template.Statements[0]["node"].(map[string]any)
	columns, _ := node["select_list"].([]any)
	trustedColumns, _ := trusted["select_list"].([]any)
	if len(columns) != 7 || len(trustedColumns) != 7 {
		return Statement{}, invalidEventPredicate()
	}
	predicateNode, ok := columns[5].(map[string]any)
	if !ok || predicateNode["alias"] != "" {
		return Statement{}, invalidEventPredicate()
	}
	columns[5] = trustedColumns[5]
	// Replacing the expression must leave the entire parser-produced SELECT
	// unchanged, except for source offsets shifted by the input's length.
	stripQueryLocations(node)
	stripQueryLocations(trusted)
	if !reflect.DeepEqual(node, trusted) {
		return Statement{}, invalidEventPredicate()
	}
	node["select_list"] = columns[:5]
	node["where_clause"] = predicateNode
	serialized, err := json.Marshal(parsed)
	if err != nil {
		return Statement{}, err
	}
	return inspectSelect(parsed, string(serialized))
}

func stripQueryLocations(value any) {
	switch node := value.(type) {
	case map[string]any:
		delete(node, "query_location")
		for _, child := range node {
			stripQueryLocations(child)
		}
	case []any:
		for _, child := range node {
			stripQueryLocations(child)
		}
	}
}

func invalidEventPredicate() error {
	return policyError("invalid_predicate", "Expected one SQL predicate for where", "Supply a Boolean expression over events, without an outer SQL clause or statement.", "where")
}
