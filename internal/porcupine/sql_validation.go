package porcupine

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	chsql "github.com/AfterShip/clickhouse-sql-parser/parser"
)

const maxSQLBytes = 16 << 10

var sqlFunctionNames = strings.Fields(`
		count countIf sum sumIf avg avgIf min minIf max maxIf any anyLast
		uniq uniqIf uniqExact uniqExactIf uniqCombined uniqCombined64
		argMin argMax median medianExact quantile quantiles quantileExact quantileExactIf
		stddevPop stddevSamp varPop varSamp groupArray groupUniqArray windowFunnel retention sequenceMatch sequenceCount
		abs ceil ceiling floor round roundBankers trunc truncate exp exp2 exp10 log log2 log10 sqrt cbrt pow power
		intDiv intDivOrZero modulo positiveModulo negate greatest least clamp
		plus minus multiply divide divideOrNull divideOrZero equals notEquals less lessOrEquals greater greaterOrEquals
		and or not if multiIf nullIf ifNull coalesce assumeNotNull isNull isNotNull isFinite isInfinite isNaN
		toString toFixedString toInt8 toInt16 toInt32 toInt64 toUInt8 toUInt16 toUInt32 toUInt64 toFloat32 toFloat64
		toInt64OrNull toUInt64OrNull toFloat64OrNull toInt64OrZero toUInt64OrZero toFloat64OrZero
		length lengthUTF8 empty notEmpty lower upper lowerUTF8 upperUTF8 trim trimBoth trimLeft trimRight
		concat substring substringUTF8 left right startsWith endsWith position positionUTF8 replaceOne replaceAll
		splitByChar splitByString arrayStringConcat reverse repeat
		toDate toDate32 toDateTime toDateTime64 parseDateTimeBestEffort parseDateTimeBestEffortOrNull
		toStartOfDay toStartOfHour toStartOfMinute toStartOfWeek toStartOfMonth toStartOfQuarter toStartOfYear
		toStartOfInterval toYear toQuarter toMonth toWeek toDayOfMonth toDayOfWeek toDayOfYear toHour toMinute toSecond
		toYYYYMM toYYYYMMDD toUnixTimestamp toUnixTimestamp64Milli toUnixTimestamp64Micro toUnixTimestamp64Nano fromUnixTimestamp64Milli fromUnixTimestamp64Micro fromUnixTimestamp64Nano formatDateTime dateDiff dateAdd dateSub addDays addHours addMinutes addMonths addYears
		subtractDays subtractHours subtractMinutes subtractMonths subtractYears now now64 today yesterday
		JSONExtract JSONExtractString JSONExtractInt JSONExtractUInt JSONExtractFloat JSONExtractBool JSONExtractRaw
		JSONExtractArrayRaw JSONExtractKeys JSONExtractKeysAndValuesRaw JSONHas JSONLength JSONType isValidJSON
		array tuple arrayElement tupleElement has hasAny hasAll indexOf arrayEnumerate arrayDistinct arraySort arrayReverseSort
		arraySum arrayAvg arrayMin arrayMax arrayCompact arrayConcat arraySlice arrayFlatten
	`)

var sqlFunctions = func() map[string]bool {
	functions := make(map[string]bool)
	for _, name := range sqlFunctionNames {
		functions[strings.ToLower(name)] = true
	}
	return functions
}()

type sqlReferences struct {
	identityColumns *bool
	eventSources    map[*chsql.TableIdentifier]bool // true for a source with a table alias
	parameters      map[string]string               // parameter name to its declared ClickHouse type
}

type sqlScope struct {
	tables     map[string]bool
	qualifiers map[string]bool
	names      map[string]bool
	refs       sqlReferences
}

func (s sqlScope) clone() sqlScope {
	copyScope := sqlScope{refs: s.refs, tables: make(map[string]bool), qualifiers: make(map[string]bool), names: make(map[string]bool)}
	for name := range s.tables {
		copyScope.tables[name] = true
	}
	for name := range s.qualifiers {
		copyScope.qualifiers[name] = true
	}
	for name := range s.names {
		copyScope.names[name] = true
	}
	return copyScope
}

func validateSQL(raw string) (string, error) {
	return validateSQLWithReferences(raw, sqlReferences{})
}

func validateSQLWithReferences(raw string, refs sqlReferences) (string, error) {
	if len(raw) == 0 || len(raw) > maxSQLBytes {
		return "", errors.New("SQL must contain 1 to 16384 bytes")
	}
	if sqlHasOutputClause(raw) {
		return "", errors.New("FORMAT, SETTINGS and OUTFILE are forbidden")
	}
	statements, err := chsql.NewParser(raw).ParseStmts()
	if err != nil || len(statements) != 1 {
		return "", errors.New("expected one valid SELECT query")
	}
	query, ok := statements[0].(*chsql.SelectQuery)
	if !ok {
		return "", errors.New("only SELECT queries are allowed")
	}
	scope := sqlScope{refs: refs, tables: map[string]bool{"events": true}, qualifiers: map[string]bool{}, names: map[string]bool{}}
	for _, name := range strings.Fields("id project_id uuid event distinct_id person_id person_properties properties timestamp timestamp_ms event_time received_at received_at_ms revision") {
		scope.names[name] = true
	}
	if err := checkSQLExpr(query, scope, 0); err != nil {
		return "", err
	}
	return chsql.Format(query), nil
}

// The parser may discard a second trailing FORMAT clause. This lexical check
// rejects output/settings keywords even when they are absent from the AST.
// The AST validator below remains authoritative for sources and expressions.
func sqlHasOutputClause(s string) bool {
	for i := 0; i < len(s); {
		switch {
		case s[i] == '\'' || s[i] == '"' || s[i] == '`':
			quote := s[i]
			i++
			for i < len(s) {
				if s[i] == '\\' {
					i += 2
					continue
				}
				if s[i] == quote {
					i++
					if i < len(s) && s[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
		case strings.HasPrefix(s[i:], "--") || s[i] == '#':
			if end := strings.IndexByte(s[i:], '\n'); end >= 0 {
				i += end + 1
			} else {
				return false
			}
		case strings.HasPrefix(s[i:], "/*"):
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return true
			}
			i += end + 4
		case s[i] == '_' || s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z':
			start := i
			i++
			for i < len(s) && (s[i] == '_' || s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z' || s[i] >= '0' && s[i] <= '9') {
				i++
			}
			switch strings.ToUpper(s[start:i]) {
			case "FORMAT", "SETTINGS", "OUTFILE":
				return true
			}
		default:
			i++
		}
	}
	return false
}

func sqlName(id *chsql.Ident) bool {
	if id == nil || id.Name == "" {
		return false
	}
	for i, c := range id.Name {
		if c != '_' && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

// Every supported node explicitly lists its children. Unknown nodes fail closed;
// this does not depend on the parser's generic walker recognizing every child.
func checkSQLExpr(expr chsql.Expr, scope sqlScope, depth int) error {
	if depth > 128 {
		return errors.New("SQL nesting exceeds 128 levels")
	}
	children := []chsql.Expr{}
	switch n := expr.(type) {
	case *chsql.SelectQuery:
		if n == nil {
			return nil
		}
		if n.InnerQuery == nil && len(n.SelectItems) == 0 {
			return errors.New("SELECT requires a projection")
		}
		if n.Settings != nil || n.Format != nil || n.Window != nil {
			return errors.New("SETTINGS, FORMAT and window clauses are unsupported")
		}
		scope = scope.clone()
		if n.With != nil {
			for _, cte := range n.With.CTEs {
				if subquery, ok := cte.Alias.(*chsql.SelectQuery); ok {
					name, ok := cte.Expr.(*chsql.Ident)
					if !ok || !sqlName(name) || scope.tables[name.Name] {
						return errors.New("invalid or shadowed CTE name")
					}
					if err := checkSQLExpr(subquery, scope, depth+1); err != nil {
						return err
					}
					scope.tables[name.Name] = true
					sqlProjectedNames(subquery, scope.names)
				} else {
					name, ok := cte.Alias.(*chsql.Ident)
					if !ok || !sqlName(name) {
						return errors.New("unsupported WITH binding")
					}
					if err := checkSQLExpr(cte.Expr, scope, depth+1); err != nil {
						return err
					}
					scope.names[name.Name] = true
				}
			}
		}
		if n.From != nil {
			if err := checkSQLSource(n.From.Expr, scope, depth+1); err != nil {
				return err
			}
		}
		for _, item := range n.SelectItems {
			if err := checkSQLExpr(item, scope, depth+1); err != nil {
				return err
			}
		}
		children = append(children, n.InnerQuery, n.Top, n.DistinctOn, n.Prewhere, n.Where, n.GroupBy, n.Having, n.OrderBy, n.LimitBy, n.Limit, n.UnionAll, n.UnionDistinct, n.Except, n.Intersect)
	case *chsql.SubQuery:
		if n == nil {
			return nil
		}
		children = append(children, n.Select)
	case *chsql.SelectItem:
		if n == nil {
			return nil
		}
		if scope.refs.identityColumns != nil && sqlProjectionStar(n.Expr) {
			*scope.refs.identityColumns = true
		}
		if len(n.Modifiers) != 0 {
			return errors.New("SELECT modifiers are unsupported")
		}
		if err := checkSQLExpr(n.Expr, scope, depth+1); err != nil {
			return err
		}
		if n.Alias != nil {
			if !sqlName(n.Alias) {
				return errors.New("invalid column alias")
			}
			scope.names[n.Alias.Name] = true
		}
	case *chsql.FunctionExpr:
		if n == nil {
			return nil
		}
		if n.Name == nil || !sqlFunctions[strings.ToLower(n.Name.Name)] {
			return errors.New("unsupported SQL function")
		}
		children = append(children, n.Params)
	case *chsql.QueryParam:
		if n == nil {
			return nil
		}
		return scope.checkParameter(n.Name, n.Type)
	case *chsql.TypedPlaceholder:
		if n == nil {
			return nil
		}
		return scope.checkParameter(n.Name, n.Type)
	case *chsql.ParamExprList:
		if n == nil {
			return nil
		}
		children = append(children, n.Items, n.ColumnArgList)
	case *chsql.ColumnArgList:
		if n == nil {
			return nil
		}
		children = append(children, n.Items...)
	case *chsql.ColumnExprList:
		if n == nil {
			return nil
		}
		children = append(children, n.Items...)
	case *chsql.ColumnExpr:
		if n == nil {
			return nil
		}
		if err := checkSQLExpr(n.Expr, scope, depth+1); err != nil {
			return err
		}
		if n.Alias != nil {
			if !sqlName(n.Alias) {
				return errors.New("invalid column alias")
			}
			scope.names[n.Alias.Name] = true
		}
	case *chsql.Ident:
		if n == nil {
			return nil
		}
		scope.noteIdentity(n.Name)
		if n.Name != "*" && (!sqlName(n) || !scope.names[n.Name]) {
			return errors.New("unsupported identifier")
		}
	case *chsql.NestedIdentifier:
		if n == nil {
			return nil
		}
		if n.DotIdent != nil {
			scope.noteIdentity(n.DotIdent.Name)
		}
		if n.Ident == nil || !scope.qualifiers[n.Ident.Name] || n.DotIdent == nil || (n.DotIdent.Name != "*" && (!sqlName(n.DotIdent) || !scope.names[n.DotIdent.Name])) {
			return errors.New("only source aliases may qualify columns")
		}
	case *chsql.Path:
		if n == nil {
			return nil
		}
		if len(n.Fields) == 2 {
			scope.noteIdentity(n.Fields[1].Name)
		}
		if len(n.Fields) != 2 || !scope.qualifiers[n.Fields[0].Name] || n.Fields[1].Name != "*" && (!sqlName(n.Fields[1]) || !scope.names[n.Fields[1].Name]) {
			return errors.New("only source aliases may qualify columns")
		}
	case *chsql.NumberLiteral, *chsql.StringLiteral, *chsql.BoolLiteral, *chsql.NullLiteral:
	case *chsql.OperationExpr:
		if n == nil {
			return nil
		}
		if n.Kind != chsql.TokenKindMul {
			return errors.New("unsupported SQL operation")
		}
	case *chsql.BinaryOperation:
		if n == nil {
			return nil
		}
		switch strings.ToUpper(string(n.Operation)) {
		case "+", "-", "*", "/", "%", "=", "==", "!=", "<>", "<", ">", "<=", ">=", "AND", "OR", "LIKE", "ILIKE", "||":
		case "IN":
			switch n.RightExpr.(type) {
			case *chsql.SubQuery:
			case *chsql.ParamExprList, *chsql.ColumnExprList, *chsql.ArrayParamList:
				if !sqlConstantSet(n.RightExpr) {
					return errors.New("IN value lists must contain constants")
				}
			default:
				return fmt.Errorf("IN requires a value list or SELECT subquery, got %T", n.RightExpr)
			}
		default:
			return errors.New("unsupported SQL operator")
		}
		if n.HasGlobal {
			return errors.New("GLOBAL operations are unsupported")
		}
		children = append(children, n.LeftExpr, n.RightExpr)
	case *chsql.IndexOperation:
		if n == nil {
			return nil
		}
		children = append(children, n.Object, n.Index)
	case *chsql.TernaryOperation:
		if n == nil {
			return nil
		}
		children = append(children, n.Condition, n.TrueExpr, n.FalseExpr)
	case *chsql.NotExpr:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr)
	case *chsql.NegateExpr:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr)
	case *chsql.IsNullExpr:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr)
	case *chsql.IsNotNullExpr:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr)
	case *chsql.BetweenClause:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr, n.Between, n.And)
	case *chsql.ArrayParamList:
		if n == nil {
			return nil
		}
		children = append(children, n.Items)
	case *chsql.CaseExpr:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr, n.Else)
		for _, when := range n.Whens {
			children = append(children, when)
		}
	case *chsql.WhenClause:
		if n == nil {
			return nil
		}
		children = append(children, n.When, n.Then, n.Else)
	case *chsql.IntervalExpr:
		if n == nil {
			return nil
		}
		if n.Unit == nil {
			return errors.New("invalid interval")
		}
		switch strings.ToUpper(n.Unit.Name) {
		case "NANOSECOND", "MICROSECOND", "MILLISECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		default:
			return errors.New("unsupported interval unit")
		}
		children = append(children, n.Expr)
	case *chsql.TopClause:
		if n == nil {
			return nil
		}
		children = append(children, n.Number)
	case *chsql.DistinctOn:
		if n == nil {
			return nil
		}
		for _, ident := range n.Idents {
			children = append(children, ident)
		}
	case *chsql.PrewhereClause:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr)
	case *chsql.WhereClause:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr)
	case *chsql.HavingClause:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr)
	case *chsql.GroupByClause:
		if n == nil {
			return nil
		}
		children = append(children, n.Expr)
	case *chsql.OrderByClause:
		if n == nil {
			return nil
		}
		if n.Interpolate != nil {
			return errors.New("INTERPOLATE is unsupported")
		}
		children = append(children, n.Items...)
	case *chsql.OrderExpr:
		if n == nil {
			return nil
		}
		if n.Fill != nil {
			return errors.New("WITH FILL is unsupported")
		}
		children = append(children, n.Expr, n.Alias)
	case *chsql.LimitClause:
		if n == nil {
			return nil
		}
		children = append(children, n.Limit, n.Offset)
	case *chsql.LimitByClause:
		if n == nil {
			return nil
		}
		children = append(children, n.Limit, n.ByExpr)
	case nil:
		return nil
	default:
		return fmt.Errorf("unsupported SQL syntax %T", expr)
	}
	for _, child := range children {
		if err := checkSQLExpr(child, scope, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func checkSQLSource(expr chsql.Expr, scope sqlScope, depth int) error {
	if depth > 128 {
		return errors.New("SQL nesting exceeds 128 levels")
	}
	switch n := expr.(type) {
	case *chsql.TableExpr:
		if n.HasFinal || n.Alias != nil {
			return errors.New("unsupported table modifier")
		}
		return checkSQLSource(n.Expr, scope, depth+1)
	case *chsql.JoinTableExpr:
		if n.SampleRatio != nil || n.HasFinal {
			return errors.New("FINAL and SAMPLE are unsupported")
		}
		return checkSQLSource(n.Table, scope, depth+1)
	case *chsql.AliasExpr:
		alias, ok := n.Alias.(*chsql.Ident)
		if !ok || !sqlName(alias) {
			return errors.New("invalid table alias")
		}
		if err := checkSQLSource(n.Expr, scope, depth+1); err != nil {
			return err
		}
		if source, ok := n.Expr.(*chsql.TableIdentifier); ok && source.Table.Name == "events" && scope.refs.eventSources != nil {
			scope.refs.eventSources[source] = true
		}
		scope.qualifiers[alias.Name] = true
		return nil
	case *chsql.TableIdentifier:
		if n.Database != nil || !sqlName(n.Table) || !scope.tables[n.Table.Name] {
			return errors.New("only events and local CTEs may be queried")
		}
		if scope.refs.eventSources != nil && n.Table.Name == "events" {
			scope.refs.eventSources[n] = false
		}
		scope.qualifiers[n.Table.Name] = true
		return nil
	case *chsql.SubQuery:
		if err := checkSQLExpr(n, scope, depth+1); err != nil {
			return err
		}
		sqlProjectedNames(n.Select, scope.names)
		return nil
	case *chsql.JoinExpr:
		for _, modifier := range n.Modifiers {
			switch strings.ToUpper(modifier) {
			case "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "ANY", "ALL", "SEMI", "ANTI", "JOIN", ",":
			default:
				return errors.New("unsupported JOIN modifier")
			}
		}
		if err := checkSQLSource(n.Left, scope, depth+1); err != nil {
			return err
		}
		if n.Right != nil {
			if err := checkSQLSource(n.Right, scope, depth+1); err != nil {
				return err
			}
		}
		if n.Constraints != nil {
			switch constraint := n.Constraints.(type) {
			case *chsql.OnClause:
				return checkSQLExpr(constraint.On, scope, depth+1)
			case *chsql.UsingClause:
				return checkSQLExpr(constraint.Using, scope, depth+1)
			case *chsql.JoinConstraintClause:
				if err := checkSQLExpr(constraint.On, scope, depth+1); err != nil {
					return err
				}
				return checkSQLExpr(constraint.Using, scope, depth+1)
			default:
				return errors.New("unsupported JOIN constraint")
			}
		}
		return nil
	default:
		return errors.New("table functions and unsupported table sources are forbidden")
	}
}

// Parenthesized bare table identifiers can otherwise be interpreted as IN sets.
func sqlConstantSet(expr chsql.Expr) bool {
	switch n := expr.(type) {
	case nil:
		return true
	case *chsql.NumberLiteral, *chsql.StringLiteral, *chsql.BoolLiteral, *chsql.NullLiteral, *chsql.QueryParam, *chsql.TypedPlaceholder:
		return true
	case *chsql.ParamExprList:
		if n == nil {
			return true
		}
		return sqlConstantSet(n.Items) && sqlConstantSet(n.ColumnArgList)
	case *chsql.ColumnExprList:
		if n == nil {
			return true
		}
		for _, item := range n.Items {
			if !sqlConstantSet(item) {
				return false
			}
		}
		return true
	case *chsql.ColumnArgList:
		if n == nil {
			return true
		}
		for _, item := range n.Items {
			if !sqlConstantSet(item) {
				return false
			}
		}
		return true
	case *chsql.ArrayParamList:
		if n == nil {
			return true
		}
		return sqlConstantSet(n.Items)
	case *chsql.ColumnExpr:
		return n.Alias == nil && sqlConstantSet(n.Expr)
	case *chsql.FunctionExpr:
		return sqlConstantSet(n.Params)
	case *chsql.NegateExpr:
		return sqlConstantSet(n.Expr)
	case *chsql.BinaryOperation:
		return sqlConstantSet(n.LeftExpr) && sqlConstantSet(n.RightExpr)
	default:
		return false
	}
}

func sqlProjectedNames(query *chsql.SelectQuery, names map[string]bool) {
	if query == nil {
		return
	}
	for _, item := range query.SelectItems {
		if item.Alias != nil {
			names[item.Alias.Name] = true
		}
	}
	sqlProjectedNames(query.InnerQuery, names)
}

func sqlParameterType(columnType chsql.ColumnType) bool {
	switch n := columnType.(type) {
	case *chsql.ScalarType:
		if n == nil || n.Name == nil {
			return false
		}
		switch strings.ToLower(n.Name.Name) {
		case "string", "bool", "uuid", "date", "date32", "datetime", "datetime64",
			"int8", "int16", "int32", "int64", "int128", "int256",
			"uint8", "uint16", "uint32", "uint64", "uint128", "uint256", "float32", "float64":
			return true
		}
	case *chsql.TypeWithParams:
		if n == nil || n.Name == nil {
			return false
		}
		switch strings.ToLower(n.Name.Name) {
		case "datetime":
			if len(n.Params) != 1 {
				return false
			}
			_, ok := n.Params[0].(*chsql.StringLiteral)
			return ok
		case "datetime64":
			if len(n.Params) < 1 || len(n.Params) > 2 {
				return false
			}
			precision, ok := n.Params[0].(*chsql.NumberLiteral)
			if !ok {
				return false
			}
			number, err := strconv.Atoi(precision.Literal)
			if err != nil || number < 0 || number > 9 {
				return false
			}
			if len(n.Params) == 2 {
				_, ok := n.Params[1].(*chsql.StringLiteral)
				return ok
			}
			return true
		}
	}
	return false
}

// Check all supplied names before constructing HTTP param_* bindings. Values
// remain separate from SQL and are parsed by ClickHouse as their declared types.
func validateSQLParameters(raw string, parameters map[string]string) error {
	bindings := map[string]string{}
	if _, err := validateSQLWithReferences(raw, sqlReferences{parameters: bindings}); err != nil {
		return err
	}
	return matchSQLParameters(bindings, parameters)
}

func (s sqlScope) checkParameter(name *chsql.Ident, typ chsql.ColumnType) error {
	if !sqlName(name) || !sqlParameterType(typ) {
		return errors.New("query parameters require safe scalar types")
	}
	if s.refs.parameters != nil {
		declared := chsql.Format(typ)
		if previous, exists := s.refs.parameters[name.Name]; exists && previous != declared {
			return errors.New("parameter name has conflicting types")
		}
		s.refs.parameters[name.Name] = declared
	}
	return nil
}

func matchSQLParameters(bindings, parameters map[string]string) error {
	for name := range bindings {
		if _, exists := parameters[name]; !exists {
			return fmt.Errorf("missing SQL parameter %q", name)
		}
	}
	for name := range parameters {
		if _, exists := bindings[name]; !exists {
			return fmt.Errorf("unknown SQL parameter %q", name)
		}
	}
	return nil
}
