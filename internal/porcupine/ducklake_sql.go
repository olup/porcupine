//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

var duckLakeParameterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

var duckLakeFunctionSet = func() map[string]bool {
	functions := make(map[string]bool, len(duckLakeFunctions))
	for _, name := range duckLakeFunctions {
		functions[name] = true
	}
	return functions
}()

type duckLakeSQLGuard struct {
	parameters map[string]bool
	nodes      int
}

// validateDuckLakeSQL uses the engine's parsed tree, never string matching, to
// authorize every source and expression before the statement can be executed.
func validateDuckLakeSQL(ctx context.Context, db *sql.DB, raw string, parameters map[string]string) (string, []any, error) {
	if len(raw) == 0 || len(raw) > maxSQLBytes || len(parameters) > 32 {
		return "", nil, errors.New("invalid DuckDB query")
	}
	for name, value := range parameters {
		if !duckLakeParameterName.MatchString(name) || len(value) > 4096 {
			return "", nil, errors.New("invalid DuckDB parameters")
		}
	}
	var serialized string
	if err := db.QueryRowContext(ctx, "SELECT CAST(json_serialize_sql(?) AS VARCHAR)", raw).Scan(&serialized); err != nil {
		return "", nil, err
	}
	if len(serialized) > 4<<20 {
		return "", nil, errors.New("DuckDB query tree too large")
	}
	var parsed struct {
		Error      bool             `json:"error"`
		Statements []map[string]any `json:"statements"`
	}
	if json.Unmarshal([]byte(serialized), &parsed) != nil || parsed.Error || len(parsed.Statements) != 1 {
		return "", nil, errors.New("expected one DuckDB SELECT")
	}
	guard := duckLakeSQLGuard{parameters: map[string]bool{}}
	if err := guard.walk(parsed.Statements[0], map[string]bool{"events": true}, 0); err != nil {
		return "", nil, err
	}
	if len(guard.parameters) != len(parameters) {
		return "", nil, errors.New("DuckDB parameter names do not match")
	}
	arguments := make([]any, 0, len(parameters))
	for name := range guard.parameters {
		value, ok := parameters[name]
		if !ok {
			return "", nil, errors.New("missing DuckDB parameter")
		}
		arguments = append(arguments, sql.Named(name, value))
	}
	var canonical string
	if err := db.QueryRowContext(ctx, "SELECT json_deserialize_sql(?)", serialized).Scan(&canonical); err != nil {
		return "", nil, err
	}
	return canonical, arguments, nil
}

func (g *duckLakeSQLGuard) walk(value any, sources map[string]bool, depth int) error {
	g.nodes++
	if depth > 128 || g.nodes > 20000 {
		return errors.New("DuckDB query tree exceeds limits")
	}
	switch node := value.(type) {
	case []any:
		for _, child := range node {
			if err := g.walk(child, sources, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		kind, _ := node["type"].(string)
		class, _ := node["class"].(string)
		if kind == "SELECT_NODE" || kind == "SET_OPERATION_NODE" {
			return g.queryNode(node, sources, depth)
		}
		if class != "" {
			if err := g.expression(node, class); err != nil {
				return err
			}
		} else {
			switch kind {
			case "":
			case "BASE_TABLE":
				name, _ := node["table_name"].(string)
				if !sources[strings.ToLower(name)] || !duckLakeUnqualified(node, "schema_name", "catalog_name") || node["at_clause"] != nil || node["sample"] != nil {
					return errors.New("unsupported DuckDB source")
				}
			case "JOIN", "SUBQUERY", "EMPTY", "EMPTY_FROM", "CROSS_PRODUCT":
				if node["sample"] != nil {
					return errors.New("unsupported DuckDB sampling")
				}
			case "ORDER_MODIFIER", "LIMIT_MODIFIER", "DISTINCT_MODIFIER", "ORDER_DEFAULT", "ASCENDING", "DESCENDING":
			default:
				return errors.New("unsupported DuckDB syntax")
			}
		}
		for _, child := range node {
			if err := g.walk(child, sources, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *duckLakeSQLGuard) queryNode(node map[string]any, inherited map[string]bool, depth int) error {
	sources := make(map[string]bool, len(inherited))
	for name, allowed := range inherited {
		sources[name] = allowed
	}
	if node["sample"] != nil {
		return errors.New("unsupported DuckDB sampling")
	}
	ctes, _ := node["cte_map"].(map[string]any)
	entries, _ := ctes["map"].([]any)
	for _, entry := range entries {
		cte, ok := entry.(map[string]any)
		if !ok {
			return errors.New("invalid DuckDB CTE")
		}
		name, _ := cte["key"].(string)
		name = strings.ToLower(name)
		if !duckLakeParameterName.MatchString(name) || sources[name] {
			return errors.New("invalid or shadowed DuckDB CTE")
		}
		definition, ok := cte["value"].(map[string]any)
		if !ok {
			return errors.New("invalid DuckDB CTE")
		}
		for key, child := range definition {
			if key == "query_node" {
				if err := g.walk(child, sources, depth+1); err != nil {
					return err
				}
			} else if err := g.walk(child, sources, depth+1); err != nil {
				return err
			}
		}
		sources[name] = true
	}
	for key, child := range node {
		if key != "cte_map" {
			if err := g.walk(child, sources, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *duckLakeSQLGuard) expression(node map[string]any, class string) error {
	kind, _ := node["type"].(string)
	switch class {
	case "COLUMN_REF":
		columns, ok := node["column_names"].([]any)
		if !ok || len(columns) < 1 || len(columns) > 2 {
			return errors.New("unsupported DuckDB column")
		}
		name, _ := columns[len(columns)-1].(string)
		switch strings.ToLower(name) {
		case "current_user", "session_user", "user", "current_role", "current_catalog", "current_schema", "current_database", "original_snapshot", "identity_snapshot", "payload", "is_deleted":
			return errors.New("private DuckDB column")
		}
	case "FUNCTION", "WINDOW":
		name, _ := node["function_name"].(string)
		if !duckLakeFunctionSet[strings.ToLower(name)] || !duckLakeUnqualified(node, "schema", "catalog") || node["export_state"] == true {
			return errors.New("unsupported DuckDB function")
		}
	case "PARAMETER":
		name, _ := node["identifier"].(string)
		if !duckLakeParameterName.MatchString(name) {
			return errors.New("unsupported DuckDB parameter")
		}
		g.parameters[name] = true
	case "STAR":
		if node["columns"] == true || node["expr"] != nil || duckLakeNonemptyList(node["replace_list"]) || duckLakeNonemptyList(node["rename_list"]) {
			return errors.New("unsupported DuckDB star expression")
		}
	case "TYPE":
		name, _ := node["type_name"].(string)
		if !duckLakeUnqualified(node, "schema", "catalog") {
			return errors.New("unsupported DuckDB cast type")
		}
		switch strings.ToUpper(name) {
		case "BOOLEAN", "BOOL", "TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT", "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC", "VARCHAR", "TEXT", "STRING", "JSON", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_TZ", "INTERVAL":
		default:
			return errors.New("unsupported DuckDB cast type")
		}
	case "CONSTANT", "CAST", "CASE", "BETWEEN", "SUBQUERY":
	case "COMPARISON":
		switch kind {
		case "COMPARE_EQUAL", "COMPARE_NOTEQUAL", "COMPARE_LESSTHAN", "COMPARE_GREATERTHAN", "COMPARE_LESSTHANOREQUALTO", "COMPARE_GREATERTHANOREQUALTO", "COMPARE_DISTINCT_FROM", "COMPARE_NOT_DISTINCT_FROM":
		default:
			return errors.New("unsupported DuckDB comparison")
		}
	case "CONJUNCTION":
		if kind != "CONJUNCTION_AND" && kind != "CONJUNCTION_OR" {
			return errors.New("unsupported DuckDB conjunction")
		}
	case "OPERATOR":
		switch kind {
		case "OPERATOR_NOT", "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "COMPARE_IN", "COMPARE_NOT_IN", "OPERATOR_COALESCE", "OPERATOR_NULLIF":
		default:
			return errors.New("unsupported DuckDB operator")
		}
	default:
		return errors.New("unsupported DuckDB expression")
	}
	return nil
}

func duckLakeUnqualified(node map[string]any, fields ...string) bool {
	for _, field := range fields {
		if value, ok := node[field].(string); ok && value != "" {
			return false
		}
	}
	if qualified, ok := node["qualified_name"].(map[string]any); ok {
		if path, ok := qualified["path"].([]any); !ok || len(path) != 1 {
			return false
		}
	}
	return true
}

func duckLakeNonemptyList(value any) bool {
	list, _ := value.([]any)
	return len(list) != 0
}
