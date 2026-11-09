package porcupine

import (
	"encoding/hex"
	"errors"
	"sort"

	chsql "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// Identity resolution is opt-in at query execution. The ordinary events view
// never joins profile history; SELECT * requests the extended public schema.
func identitySQL(raw, namespace string) (string, error) {
	canonical, err := validateSQL(raw)
	if err != nil {
		return "", err
	}
	if !clickHouseIdentifier.MatchString(namespace) {
		return "", errors.New("invalid identity namespace")
	}
	sources := map[*chsql.TableIdentifier]bool{}
	identity := false
	if _, err = validateSQLWithReferences(canonical, sqlReferences{eventSources: sources, identityColumns: &identity}); err != nil {
		return "", err
	}
	if !identity {
		return canonical, nil
	}
	type edit struct {
		start, end int
		text       string
	}
	var edits []edit
	for table, aliased := range sources {
		text := namespace + ".events_with_persons"
		if !aliased {
			text += " AS events"
		}
		edits = append(edits, edit{int(table.Pos()), int(table.End()), text})
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, e := range edits {
		if e.start < 0 || e.end > len(canonical) || e.start >= e.end {
			return "", errors.New("invalid identity source")
		}
		canonical = canonical[:e.start] + e.text + canonical[e.end:]
	}
	return canonical, nil
}

func (s sqlScope) noteIdentity(name string) {
	if s.refs.identityColumns != nil && (name == "person_id" || name == "person_properties") {
		*s.refs.identityColumns = true
	}
}
func sqlProjectionStar(e chsql.Expr) bool {
	switch n := e.(type) {
	case *chsql.Ident:
		return n != nil && n.Name == "*"
	case *chsql.ColumnExpr:
		return n != nil && sqlProjectionStar(n.Expr)
	case *chsql.OperationExpr:
		return n != nil && n.Kind == chsql.TokenKindMul
	case *chsql.Path:
		return n != nil && len(n.Fields) == 2 && n.Fields[1].Name == "*"
	case *chsql.NestedIdentifier:
		return n != nil && n.DotIdent != nil && n.DotIdent.Name == "*"
	}
	return false
}

func identityViewSQL(database, namespace, projectID string) string {
	project := "project_id=unhex('" + hex.EncodeToString([]byte(projectID)) + "')"
	controls := database + "." + eventTable + " FINAL"
	links := `SELECT JSONExtractString(link,'anonymous_id') AS anonymous_id,
 argMax(JSONExtractString(link,'identified_id'),toUInt64OrZero(JSONExtractString(link,'version'))) AS identified_id
 FROM ` + controls + ` ARRAY JOIN JSONExtractArrayRaw(identity_snapshot,'links') AS link
 WHERE ` + project + ` AND identity_snapshot!='' AND JSONExtractString(link,'anonymous_id')!=''
 GROUP BY anonymous_id`
	profiles := `SELECT JSONExtractString(identity_snapshot,'identified_id') AS identified_id,
 argMax(JSONExtractRaw(identity_snapshot,'traits'),toUInt64OrZero(JSONExtractString(identity_snapshot,'version'))) AS traits
 FROM ` + controls + ` WHERE ` + project + ` AND identity_snapshot!='' AND JSONExtractString(identity_snapshot,'identified_id')!=''
 GROUP BY identified_id`
	person := `if(links.anonymous_id!='',links.identified_id,e.distinct_id)`
	return "CREATE OR REPLACE VIEW " + namespace + `.events_with_persons SQL SECURITY INVOKER AS
 SELECT e.id,e.event_id,e.is_imported,e.source_id,e.message_id,e.event_type,e.context,e.project_id,e.uuid,e.event,e.distinct_id,` + person + ` AS person_id,
 if(profiles.identified_id!='',profiles.traits,'{}') AS person_properties,
 e.properties,e.timestamp,e.timestamp_ms,e.revision,e.event_time,e.received_at,e.received_at_ms
 FROM ` + namespace + `.events AS e
 LEFT ANY JOIN (` + links + `) AS links ON links.anonymous_id=e.distinct_id
 LEFT ANY JOIN (` + profiles + `) AS profiles ON profiles.identified_id=` + person
}

// The physical allowlist intentionally excludes original_snapshot and the
// deletion marker. The new snapshot contains canonical, project-local identity
// state only; its raw field is never projected into either public view.
const analyticsPhysicalColumns = "event_id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,received_at,received_at_ms,revision,identity_snapshot,is_imported," + eventMetadataColumns

func identityViewNeeded(raw string) bool {
	identity := false
	_, err := validateSQLWithReferences(raw, sqlReferences{identityColumns: &identity})
	return err == nil && identity
}

func identitySourceName(raw string) string {
	if identityViewNeeded(raw) {
		return "events_with_persons"
	}
	return "events"
}
