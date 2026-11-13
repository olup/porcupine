//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
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
	parameters   map[string]bool
	eventSources int
	nodes        int
}

// validateDuckLakeSQL uses the engine's parsed tree, never string matching, to
// authorize every source and expression before the statement can be executed.
func validateDuckLakeSQL(ctx context.Context, db *sql.DB, raw string, parameters map[string]string) (string, []any, error) {
	if len(raw) == 0 || len(raw) > maxSQLBytes || len(parameters) > 32 {
		return "", nil, analyticsError(400, "invalid_sql", "Invalid analytics query size or parameter count", "Send one SELECT of at most 16384 bytes and at most 32 parameters.", "")
	}
	for name, value := range parameters {
		if !duckLakeParameterName.MatchString(name) || len(value) > 4096 {
			return "", nil, analyticsError(400, "invalid_parameter", "Invalid analytics parameter", "Use identifier keys of at most 64 ASCII characters and values of at most 4096 bytes.", "")
		}
	}
	serialized, declared, _, err := parseDuckLakeSQL(ctx, db, raw)
	if err != nil {
		return "", nil, err
	}
	arguments := make([]any, 0, len(parameters))
	names := make([]string, 0, len(declared))
	for name := range declared {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value, ok := parameters[name]
		if !ok {
			return "", nil, analyticsError(400, "missing_parameter", "Missing bound query parameter", "Supply this key in parameters; use $name placeholders for DuckDB.", name)
		}
		arguments = append(arguments, sql.Named(name, value))
	}
	names = names[:0]
	for name := range parameters {
		if !declared[name] {
			names = append(names, name)
		}
	}
	if len(names) > 0 {
		sort.Strings(names)
		return "", nil, analyticsError(400, "unused_parameter", "Unused bound query parameter", "Remove this key or reference it with a $name placeholder.", names[0])
	}
	var canonical string
	if err := db.QueryRowContext(ctx, "SELECT json_deserialize_sql(?)", serialized).Scan(&canonical); err != nil {
		return "", nil, err
	}
	return canonical, arguments, nil
}

func parseDuckLakeSQL(ctx context.Context, db *sql.DB, raw string) (string, map[string]bool, bool, error) {
	if len(raw) == 0 || len(raw) > maxSQLBytes {
		return "", nil, false, errors.New("invalid DuckDB query")
	}
	var serialized string
	if err := db.QueryRowContext(ctx, "SELECT CAST(json_serialize_sql(?) AS VARCHAR)", raw).Scan(&serialized); err != nil {
		return "", nil, false, err
	}
	if len(serialized) > 4<<20 {
		return "", nil, false, errors.New("DuckDB query tree too large")
	}
	var parsed struct {
		Error      bool             `json:"error"`
		Statements []map[string]any `json:"statements"`
	}
	if json.Unmarshal([]byte(serialized), &parsed) != nil || parsed.Error || len(parsed.Statements) != 1 {
		return "", nil, false, analyticsError(400, "invalid_sql", "Expected one valid DuckDB SELECT", "Use DuckDB syntax and $name parameters. Read get-event-schema for the supported dialect and functions.", "")
	}
	guard := duckLakeSQLGuard{parameters: map[string]bool{}}
	if err := guard.walk(parsed.Statements[0], map[string]bool{"events": true}, 0); err != nil {
		return "", nil, false, err
	}
	return serialized, guard.parameters, guard.eventSources > 0, nil
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
				if strings.EqualFold(name, "events") {
					g.eventSources++
				}
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
		if !duckLakeUnqualified(node, "schema", "catalog") || node["export_state"] == true {
			return analyticsError(400, "unsupported_sql", "Qualified functions or exported aggregate state are not allowed", "Call an allowed unqualified function from get-event-schema.", "")
		}
		if !duckLakeFunctionSet[strings.ToLower(name)] {
			field := ""
			if duckLakeParameterName.MatchString(name) {
				field = name
			}
			return analyticsError(400, "unsupported_function", "Function is not allowed in analytics queries", "Read get-event-schema for allowed DuckDB functions. ClickHouse function names are not interchangeable.", field)
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
