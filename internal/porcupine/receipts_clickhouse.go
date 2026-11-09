package porcupine

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var receiptCleanupDisabled = regexp.MustCompile(`(?i)allow_experimental_replacing_merge_with_cleanup\s*=\s*0\b`)

// Only Porcupine's deployment MOVE TTL is recognized. Bare TTLs delete rows
// implicitly; unknown operator TTLs cannot establish authoritative absence.
var receiptMoveTTL = regexp.MustCompile(`(?i)^toDateTime\(fromUnixTimestamp64Milli\(received_at_ms,'UTC'\)\)\+(?:toIntervalDay\([0-9]+\)|INTERVAL[0-9]+DAY)TOVOLUME'[a-zA-Z_][a-zA-Z0-9_]*'$`)

func receiptDefinitionSafe(definition string) bool {
	if !receiptCleanupDisabled.MatchString(definition) {
		return false
	}
	if clause := tableTTLClause.FindStringSubmatch(definition); len(clause) > 0 {
		compact := strings.Join(strings.Fields(clause[1]), "")
		compact = strings.ReplaceAll(compact, "`received_at_ms`", "received_at_ms")
		return receiptMoveTTL.MatchString(compact)
	}
	return true
}

func receiptSortingKeySafe(key string) bool {
	key = strings.ReplaceAll(strings.Join(strings.Fields(key), ""), "`", "")
	return key == "project_id,uuid" || key == "(project_id,uuid)" || key == "tuple(project_id,uuid)"
}

// receiptFacts reads physical rows in every receipt partition, including
// tombstones. It never initializes or alters the source (online backup can hold
// the projection lock while captures continue). Unknown history fails closed.
func (c *clickHouse) receiptFacts(ctx context.Context, keys []receiptKey) (string, map[receiptKey]bool, error) {
	source, ids, err := c.receiptFactsWithIDs(ctx, keys)
	seen := map[receiptKey]bool{}
	for key := range ids {
		seen[key] = true
	}
	return source, seen, err
}

func (c *clickHouse) receiptFactsWithIDs(ctx context.Context, keys []receiptKey) (string, map[receiptKey]int64, error) {
	if c == nil || len(keys) > maxCaptureEvents {
		return "", nil, errReceiptHistory
	}
	requested := map[receiptKey]bool{}
	projects := map[string][]string{}
	literal := func(v string) string { return "unhex('" + hex.EncodeToString([]byte(v)) + "')" }
	for _, key := range keys {
		if !requested[key] {
			projects[key.ProjectID] = append(projects[key.ProjectID], literal(key.UUID))
		}
		requested[key] = true
	}
	projectNames := make([]string, 0, len(projects))
	for project := range projects {
		projectNames = append(projectNames, project)
	}
	sort.Strings(projectNames)
	predicates := make([]string, 0, len(projectNames))
	for _, project := range projectNames {
		predicates = append(predicates, "(project_id="+literal(project)+" AND uuid IN ("+strings.Join(projects[project], ",")+"))")
	}
	tableFilter := `database=` + literal(c.config.Database) + ` AND name='events_v2'`
	policyFilter := `database=` + literal(c.config.Database) + ` AND table='events_v2'`
	facts := `CAST([], 'Array(Tuple(String,String,String))')`
	if len(predicates) > 0 {
		facts = `(SELECT groupArray(tuple(project_id,uuid,toString(event_id))) FROM (SELECT project_id,uuid,min(event_id) AS event_id FROM ` + c.config.Database + `.` + eventTable + ` PREWHERE ` + strings.Join(predicates, " OR ") + ` GROUP BY project_id,uuid))`
	}
	// A complete JSON envelope, not streaming JSONEachRow, is essential: a source
	// header followed by a truncated/broken facts stream must never prove absence.
	query := `/* porcupine:receipts */ SELECT (SELECT toString(uuid) FROM system.tables WHERE ` + tableFilter + `) AS source_uuid, (SELECT engine FROM system.tables WHERE ` + tableFilter + `) AS source_engine, (SELECT sorting_key FROM system.tables WHERE ` + tableFilter + `) AS sorting_key, (SELECT create_table_query FROM system.tables WHERE ` + tableFilter + `) AS definition, (SELECT count() FROM system.row_policies WHERE ` + policyFilter + `) AS policy_count, (SELECT countIf(is_restrictive) FROM system.row_policies WHERE ` + policyFilter + `) AS restrictive_count, (SELECT countIf(trimBoth(select_filter) IN ('1','true') AND NOT has(apply_to_except,` + literal(c.config.User) + `) AND (apply_to_all OR has(apply_to_list,` + literal(c.config.User) + `))) FROM system.row_policies WHERE ` + policyFilter + `) AS allow_count, ` + facts + ` AS facts SETTINGS final=0,use_query_cache=0,max_rows_to_group_by=0,group_by_overflow_mode='throw',max_rows_in_set=0,max_bytes_in_set=0,set_overflow_mode='throw',max_threads=1,max_execution_time=5,timeout_overflow_mode='throw',max_memory_usage=67108864,max_rows_to_read=0,max_bytes_to_read=1073741824,read_overflow_mode='throw',output_format_json_quote_64bit_integers=1,enable_named_columns_in_function_tuple=0,max_result_rows=1,max_result_bytes=2097152,result_overflow_mode='throw' FORMAT JSON`
	data, err := c.request(ctx, c.config.User, c.config.Password, "", nil, strings.NewReader(query), 2<<20)
	if err != nil {
		return "", nil, errReceiptHistory
	}
	var result struct {
		Data []struct {
			Source      string      `json:"source_uuid"`
			Engine      string      `json:"source_engine"`
			SortingKey  string      `json:"sorting_key"`
			Definition  string      `json:"definition"`
			Policies    *uint64     `json:"policy_count,string"`
			Restrictive *uint64     `json:"restrictive_count,string"`
			Allow       *uint64     `json:"allow_count,string"`
			Facts       *[][]string `json:"facts"`
		} `json:"data"`
		Rows       *uint64          `json:"rows"`
		Statistics *json.RawMessage `json:"statistics"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(&result) != nil || result.Rows == nil || *result.Rows != 1 || result.Statistics == nil || len(result.Data) != 1 {
		return "", nil, errReceiptHistory
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return "", nil, errReceiptHistory
	}
	row := result.Data[0]
	if row.Source == "" || row.Source == "00000000-0000-0000-0000-000000000000" || row.Engine != "ReplacingMergeTree" || !receiptSortingKeySafe(row.SortingKey) || !receiptDefinitionSafe(row.Definition) || row.Policies == nil || row.Restrictive == nil || row.Allow == nil || *row.Restrictive != 0 || (*row.Policies > 0 && *row.Allow == 0) || row.Facts == nil || len(*row.Facts) > len(requested) {
		return "", nil, errReceiptHistory
	}
	seen := map[receiptKey]int64{}
	for _, tuple := range *row.Facts {
		if len(tuple) != 2 && len(tuple) != 3 {
			return "", nil, errReceiptHistory
		}
		key := receiptKey{tuple[0], tuple[1]}
		_, duplicate := seen[key]
		if !requested[key] || duplicate {
			return "", nil, errReceiptHistory
		}
		id := int64(0)
		if len(tuple) == 3 {
			var parseErr error
			id, parseErr = strconv.ParseInt(tuple[2], 10, 64)
			if parseErr != nil || id < 0 {
				return "", nil, errReceiptHistory
			}
		}
		seen[key] = id
	}
	return row.Source, seen, nil
}
