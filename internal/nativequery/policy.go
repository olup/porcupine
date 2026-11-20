// Package nativequery validates the analytical SQL policy on an already scoped
// DuckDB connection. It owns no engine, project routing or HTTP behavior.
package nativequery

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

var parameterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

var functionSet = func() map[string]bool {
	allowed := make(map[string]bool, len(functions))
	for _, name := range functions {
		allowed[name] = true
	}
	return allowed
}()

type sqlGuard struct {
	parameters      map[string]bool
	eventSources    int
	nodes           int
	rowNames        map[string]bool
	referencedNames []string
	needsPerson     bool
}

// Prepare returns canonical SQL and its checked parameter bindings.
func Prepare(ctx context.Context, db *sql.DB, raw string, parameters map[string]string) (string, []any, error) {
	query, err := PrepareQuery(ctx, db, raw, parameters)
	return query.SQL, query.Args, err
}

// Prepared contains canonical SQL, its bindings, and the inspection result.
type Prepared struct {
	SQL  string
	Args []any
	Statement
}

// PrepareQuery authorizes every source and expression using DuckDB's parsed tree.
func PrepareQuery(ctx context.Context, db *sql.DB, raw string, parameters map[string]string) (Prepared, error) {
	if len(raw) == 0 || len(raw) > MaxSQLBytes || len(parameters) > 32 {
		return Prepared{}, policyError("invalid_sql", "Invalid analytics query size or parameter count", "Send one SELECT of at most 16384 bytes and at most 32 parameters.", "")
	}
	for name, value := range parameters {
		if !parameterName.MatchString(name) || len(value) > 4096 {
			return Prepared{}, policyError("invalid_parameter", "Invalid analytics parameter", "Use identifier keys of at most 64 ASCII characters and values of at most 4096 bytes.", "")
		}
	}
	statement, err := Inspect(ctx, db, raw)
	if err != nil {
		return Prepared{}, err
	}
	arguments := make([]any, 0, len(parameters))
	names := make([]string, 0, len(statement.parameters))
	for name := range statement.parameters {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value, ok := parameters[name]
		if !ok {
			return Prepared{}, policyError("missing_parameter", "Missing bound query parameter", "Supply this key in parameters; use $name placeholders for DuckDB.", name)
		}
		arguments = append(arguments, sql.Named(name, value))
	}
	names = names[:0]
	for name := range parameters {
		if !statement.parameters[name] {
			names = append(names, name)
		}
	}
	if len(names) > 0 {
		sort.Strings(names)
		return Prepared{}, policyError("unused_parameter", "Unused bound query parameter", "Remove this key or reference it with a $name placeholder.", names[0])
	}
	var canonical string
	if err := db.QueryRowContext(ctx, "SELECT json_deserialize_sql(?)", statement.serialized).Scan(&canonical); err != nil {
		return Prepared{}, err
	}
	if err := validateSQLText(canonical); err != nil {
		return Prepared{}, err
	}
	if len(canonical) == 0 || len(canonical) > MaxSQLBytes {
		return Prepared{}, policyError("invalid_sql", "Canonical analytics query exceeds the supported size", "Use a shorter SELECT of at most 16384 bytes after formatting.", "")
	}
	return Prepared{SQL: canonical, Args: arguments, Statement: statement}, nil
}

// MaxSQLBytes is the maximum accepted analytical statement size.
const MaxSQLBytes = 16 << 10

// Error describes a policy rejection without exposing SQL or bound values.
// Transport adapters decide how to present it to their callers.
type Error struct {
	Code    string
	Message string
	Hint    string
	Field   string
}

func (e *Error) Error() string { return e.Message }

func policyError(code, message, hint, field string) error {
	return &Error{Code: code, Message: message, Hint: hint, Field: field}
}

// DuckDB's SQL serializer can accept NUL as a token but produce SQL that its
// parser cannot read back. Reject invalid text before crossing that boundary.
func validateSQLText(raw string) error {
	if !utf8.ValidString(raw) || strings.ContainsRune(raw, '\x00') {
		return policyError("invalid_sql", "Invalid analytics SQL text", "Send UTF-8 SQL without NUL bytes.", "")
	}
	return nil
}

func unsupportedSQL() error {
	return policyError("unsupported_sql", "Unsupported DuckDB SQL", "Use one SELECT against events. Read get-event-schema for supported sources, expressions and functions.", "")
}

// Statement holds a checked AST. Its SQL and mutable parameter inventory stay
// private so callers cannot change the policy decision before binding.
type Statement struct {
	serialized  string
	parameters  map[string]bool
	usesEvents  bool
	needsPerson bool
}

func (s Statement) UsesEvents() bool              { return s.usesEvents }
func (s Statement) HasParameter(name string) bool { return s.parameters[name] }
func (s Statement) NeedsPerson() bool             { return s.needsPerson }

// Inspect checks every AST node, including CTEs and nested expressions. The
// supplied database must already expose only this project's analytical views.
func Inspect(ctx context.Context, db *sql.DB, raw string) (Statement, error) {
	if len(raw) == 0 || len(raw) > MaxSQLBytes {
		return Statement{}, unsupportedSQL()
	}
	if err := validateSQLText(raw); err != nil {
		return Statement{}, err
	}
	var serialized string
	if err := db.QueryRowContext(ctx, "SELECT CAST(json_serialize_sql(?) AS VARCHAR)", raw).Scan(&serialized); err != nil {
		return Statement{}, err
	}
	if len(serialized) > 4<<20 {
		return Statement{}, unsupportedSQL()
	}
	var parsed struct {
		Error      bool             `json:"error"`
		Statements []map[string]any `json:"statements"`
	}
	if json.Unmarshal([]byte(serialized), &parsed) != nil || parsed.Error || len(parsed.Statements) != 1 {
		return Statement{}, policyError("invalid_sql", "Expected one valid DuckDB SELECT", "Use DuckDB syntax and $name parameters. Read get-event-schema for the supported dialect and functions.", "")
	}
	guard := sqlGuard{parameters: map[string]bool{}, rowNames: map[string]bool{"events": true}}
	if err := guard.walk(parsed.Statements[0], map[string]bool{"events": true}, 0); err != nil {
		return Statement{}, err
	}
	// DuckDB can bind a table name or alias as a whole row, even in a subquery.
	for _, name := range guard.referencedNames {
		guard.needsPerson = guard.needsPerson || guard.rowNames[name]
	}
	return Statement{serialized: serialized, parameters: guard.parameters, usesEvents: guard.eventSources > 0, needsPerson: guard.needsPerson}, nil
}

func (g *sqlGuard) walk(value any, sources map[string]bool, depth int) error {
	g.nodes++
	if depth > 128 || g.nodes > 20000 {
		return unsupportedSQL()
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
			if alias, _ := node["alias"].(string); alias != "" {
				g.rowNames[strings.ToLower(alias)] = true
			}
			switch kind {
			case "":
			case "BASE_TABLE":
				name, _ := node["table_name"].(string)
				if strings.EqualFold(name, "events") {
					g.eventSources++
				}
				if !sources[strings.ToLower(name)] || !unqualified(node, "schema_name", "catalog_name") || node["at_clause"] != nil || node["sample"] != nil {
					return unsupportedSQL()
				}
				g.rowNames[strings.ToLower(name)] = true
				g.needsPerson = g.needsPerson || nonemptyList(node["column_name_alias"])
			case "JOIN", "SUBQUERY", "EMPTY", "EMPTY_FROM", "CROSS_PRODUCT":
				if node["sample"] != nil {
					return unsupportedSQL()
				}
				g.needsPerson = g.needsPerson || node["ref_type"] == "NATURAL" || node["ref_type"] == "POSITIONAL"
				columns, _ := node["using_columns"].([]any)
				for _, column := range columns {
					name, _ := column.(string)
					g.needsPerson = g.needsPerson || isPersonColumn(strings.ToLower(name))
				}
			case "ORDER_MODIFIER", "LIMIT_MODIFIER", "DISTINCT_MODIFIER", "ORDER_DEFAULT", "ASCENDING", "DESCENDING":
			default:
				return unsupportedSQL()
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

func (g *sqlGuard) queryNode(node map[string]any, inherited map[string]bool, depth int) error {
	sources := make(map[string]bool, len(inherited))
	for name, allowed := range inherited {
		sources[name] = allowed
	}
	if node["sample"] != nil {
		return unsupportedSQL()
	}
	ctes, _ := node["cte_map"].(map[string]any)
	entries, _ := ctes["map"].([]any)
	for _, entry := range entries {
		cte, ok := entry.(map[string]any)
		if !ok {
			return unsupportedSQL()
		}
		name, _ := cte["key"].(string)
		name = strings.ToLower(name)
		if !parameterName.MatchString(name) || sources[name] {
			return unsupportedSQL()
		}
		definition, ok := cte["value"].(map[string]any)
		if !ok {
			return unsupportedSQL()
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

func (g *sqlGuard) expression(node map[string]any, class string) error {
	kind, _ := node["type"].(string)
	switch class {
	case "COLUMN_REF":
		columns, ok := node["column_names"].([]any)
		if !ok || len(columns) < 1 || len(columns) > 2 {
			return unsupportedSQL()
		}
		name, _ := columns[len(columns)-1].(string)
		name = strings.ToLower(name)
		switch name {
		case "current_user", "session_user", "user", "current_role", "current_catalog", "current_schema", "current_database", "original_snapshot", "identity_snapshot", "payload", "is_deleted":
			return unsupportedSQL()
		}
		g.needsPerson = g.needsPerson || isPersonColumn(name)
		g.referencedNames = append(g.referencedNames, name)
	case "FUNCTION", "WINDOW":
		name, _ := node["function_name"].(string)
		if !unqualified(node, "schema", "catalog") || node["export_state"] == true {
			return policyError("unsupported_sql", "Qualified functions or exported aggregate state are not allowed", "Call an allowed unqualified function from get-event-schema.", "")
		}
		if !functionSet[strings.ToLower(name)] {
			field := ""
			if parameterName.MatchString(name) {
				field = name
			}
			return policyError("unsupported_function", "Function is not allowed in analytics queries", "Read get-event-schema for allowed DuckDB functions.", field)
		}
	case "PARAMETER":
		name, _ := node["identifier"].(string)
		if !parameterName.MatchString(name) {
			return unsupportedSQL()
		}
		g.parameters[name] = true
	case "STAR":
		g.needsPerson = true
		if node["columns"] == true || node["expr"] != nil || nonemptyList(node["replace_list"]) || nonemptyList(node["rename_list"]) {
			return unsupportedSQL()
		}
	case "TYPE":
		name, _ := node["type_name"].(string)
		if !unqualified(node, "schema", "catalog") {
			return unsupportedSQL()
		}
		switch strings.ToUpper(name) {
		case "BOOLEAN", "BOOL", "TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT", "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC", "VARCHAR", "TEXT", "STRING", "JSON", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_TZ", "INTERVAL":
		default:
			return unsupportedSQL()
		}
	case "CONSTANT", "CAST", "CASE", "BETWEEN", "SUBQUERY":
	case "COMPARISON":
		switch kind {
		case "COMPARE_EQUAL", "COMPARE_NOTEQUAL", "COMPARE_LESSTHAN", "COMPARE_GREATERTHAN", "COMPARE_LESSTHANOREQUALTO", "COMPARE_GREATERTHANOREQUALTO", "COMPARE_DISTINCT_FROM", "COMPARE_NOT_DISTINCT_FROM":
		default:
			return unsupportedSQL()
		}
	case "CONJUNCTION":
		if kind != "CONJUNCTION_AND" && kind != "CONJUNCTION_OR" {
			return unsupportedSQL()
		}
	case "OPERATOR":
		switch kind {
		case "OPERATOR_NOT", "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "COMPARE_IN", "COMPARE_NOT_IN", "OPERATOR_COALESCE", "OPERATOR_NULLIF":
		default:
			return unsupportedSQL()
		}
	default:
		return unsupportedSQL()
	}
	return nil
}

func isPersonColumn(name string) bool { return name == "person_id" || name == "person_properties" }

func unqualified(node map[string]any, fields ...string) bool {
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

func nonemptyList(value any) bool {
	list, _ := value.([]any)
	return len(list) != 0
}
