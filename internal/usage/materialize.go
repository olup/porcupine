package usage

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type projectAnchor struct {
	tenant, id, first      string
	present, tenantPresent bool
}
type aggregate struct {
	c          Counters
	unresolved int64
	observed   string
	sample     *StorageSample
}
type bucket struct{ start, end time.Time }

func buckets(q Query) []bucket {
	start, _ := time.Parse(time.RFC3339, q.Start)
	end, _ := time.Parse(time.RFC3339, q.End)
	out := []bucket{}
	for start.Before(end) {
		var next time.Time
		if q.Granularity == "month" {
			next = time.Date(start.Year(), start.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		} else {
			next = start.AddDate(0, 0, 1)
		}
		next = minTime(next, end)
		out = append(out, bucket{start, next})
		start = next
	}
	return out
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func maxString(a, b string) string {
	if a > b {
		return a
	}
	return b
}

func (s *Store) materialize(ctx context.Context, q Query, budget int64) ([]UsageRow, error) {
	options := &sql.TxOptions{ReadOnly: true}
	if s.Postgres {
		options.Isolation = sql.LevelRepeatableRead
	}
	tx, e := s.DB.BeginTx(ctx, options)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	var available string
	if e = tx.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='usage_available_since'`).Scan(&available); e != nil {
		return nil, e
	}
	// Include numerical tombstones without carrying project names or tenant PII.
	rows, e := tx.QueryContext(ctx, `SELECT k.tenant_id,k.project_id,COALESCE(p.created_at,MIN(k.first_day)),CASE WHEN p.id IS NULL THEN 0 ELSE 1 END,CASE WHEN t.id IS NULL THEN 0 ELSE 1 END FROM (
SELECT tenant_id,id AS project_id,created_at AS first_day FROM projects WHERE ($1='' OR tenant_id=$1)
UNION SELECT tenant_id,project_id,MIN(day_start) AS first_day FROM usage_rollups WHERE day_start<$2 AND ($1='' OR tenant_id=$1) GROUP BY tenant_id,project_id
UNION SELECT tenant_id,project_id,MIN(day_start) AS first_day FROM usage_storage_samples WHERE day_start<$2 AND ($1='' OR tenant_id=$1) GROUP BY tenant_id,project_id
) k LEFT JOIN projects p ON p.id=k.project_id AND p.tenant_id=k.tenant_id LEFT JOIN tenants t ON t.id=k.tenant_id GROUP BY k.tenant_id,k.project_id,p.id,p.created_at,t.id ORDER BY k.tenant_id,k.project_id LIMIT 100001`, q.TenantID, q.End)
	if e != nil {
		return nil, e
	}
	// Reserve a conservative working allowance for each project/bucket before
	// growing maps or building wire strings. IDs are bounded opaque ASCII and all
	// other fields are bounded timestamps/numerics. Actual encoded pages are
	// measured too; this deliberately may reject before the 100k absolute ceiling.
	bs := buckets(q)
	const rowWorkingBytes = int64(2048)
	anchorLimit := min(int64(MaxRows/max(1, len(bs))), budget/rowWorkingBytes/int64(max(1, len(bs))))
	anchors := map[string]projectAnchor{}
	for rows.Next() {
		var p projectAnchor
		var present, tenant int
		if e = rows.Scan(&p.tenant, &p.id, &p.first, &present, &tenant); e != nil {
			rows.Close()
			return nil, e
		}
		p.present = present == 1
		p.tenantPresent = tenant == 1
		if !validID(p.tenant) || !validID(p.id) {
			rows.Close()
			return nil, fmt.Errorf("invalid usage project anchor")
		}
		key := p.tenant + "\x00" + p.id
		if _, exists := anchors[key]; !exists && int64(len(anchors)) >= anchorLimit {
			rows.Close()
			return nil, ErrTooLarge
		}
		if old, ok := anchors[key]; !ok || p.first < old.first {
			anchors[key] = p
		}
		if len(anchors) > MaxRows {
			rows.Close()
			return nil, ErrTooLarge
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	if len(anchors) > MaxRows/max(1, len(bs)) {
		return nil, ErrTooLarge
	}
	projects := make([]projectAnchor, 0, len(anchors))
	for _, p := range anchors {
		projects = append(projects, p)
	}
	sort.Slice(projects, func(i, j int) bool {
		if projects[i].tenant == projects[j].tenant {
			return projects[i].id < projects[j].id
		}
		return projects[i].tenant < projects[j].tenant
	})
	totals := map[string][]aggregate{}
	for _, p := range projects {
		totals[p.tenant+"\x00"+p.id] = make([]aggregate, len(bs))
	}
	rows, e = tx.QueryContext(ctx, `SELECT tenant_id,project_id,day_start,live_events,import_events,query_count,query_micros,export_bytes,pipeline_bytes,webhook_success,unresolved_queries,last_observed_at FROM usage_rollups WHERE day_start>=$1 AND day_start<$2 AND ($3='' OR tenant_id=$3)`, q.Start, q.End, q.TenantID)
	if e != nil {
		return nil, e
	}
	for rows.Next() {
		var tenant, project, day, observed string
		var c Counters
		var unresolved int64
		if e = rows.Scan(&tenant, &project, &day, &c.Live, &c.Import, &c.Queries, &c.QueryMicros, &c.ExportBytes, &c.PipelineBytes, &c.Webhooks, &unresolved, &observed); e != nil {
			rows.Close()
			return nil, e
		}
		dayTime, _ := time.Parse(time.RFC3339, day)
		values := totals[tenant+"\x00"+project]
		for i, b := range bs {
			if !dayTime.Before(b.start) && dayTime.Before(b.end) {
				if e = values[i].c.Add(c); e != nil {
					rows.Close()
					return nil, e
				}
				values[i].unresolved, e = checkedUnresolved(values[i].unresolved, unresolved)
				if e != nil {
					rows.Close()
					return nil, e
				}
				values[i].observed = maxString(values[i].observed, observed)
				break
			}
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	// One latest sample per UTC day is retained. Earlier samples can cover a
	// cutoff until they become stale; absent measurements never turn into zero.
	rows, e = tx.QueryContext(ctx, `SELECT tenant_id,project_id,day_start,observed_at,bytes FROM usage_storage_samples WHERE day_start<$1 AND ($2='' OR tenant_id=$2) ORDER BY tenant_id,project_id,observed_at`, q.End, q.TenantID)
	if e != nil {
		return nil, e
	}
	for rows.Next() {
		v := new(StorageSample)
		if e = rows.Scan(&v.TenantID, &v.ProjectID, &v.Day, &v.ObservedAt, &v.Bytes); e != nil {
			rows.Close()
			return nil, e
		}
		at, _ := time.Parse(time.RFC3339Nano, v.ObservedAt)
		values := totals[v.TenantID+"\x00"+v.ProjectID]
		for i, b := range bs {
			cutoff := minTime(b.end, s.Clock())
			if !at.After(cutoff) {
				values[i].sample = v
			}
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	out := []UsageRow{}
	for _, p := range projects {
		for i, b := range bs {
			if !b.end.After(parseTime(p.first)) {
				continue
			}
			v := totals[p.tenant+"\x00"+p.id][i]
			r := UsageRow{TenantID: p.tenant, ProjectID: p.id, TenantDeleted: !p.tenantPresent, ProjectDeleted: !p.present, PeriodStart: b.start.Format(time.RFC3339), PeriodEnd: b.end.Format(time.RFC3339), AvailableSince: available, Partial: Stamp(b.start) < available, AcceptanceBasis: "journal_commit", Counters: v.c.Wire(), QueryDuration: &UsageQueryDuration{Quality: "observed", Unresolved: strconv.FormatInt(v.unresolved, 10), LastObserved: v.observed}, Storage: storageFor(v.sample, minTime(b.end, s.Clock()))}
			if v.unresolved > 0 {
				r.QueryDuration.Quality = "lower_bound"
			}
			if Stamp(b.end) <= available {
				r.Counters = nil
				r.QueryDuration = nil
			}
			out = append(out, r)
		}
	}
	if q.GroupBy == "tenant" {
		return combineTenants(out)
	}
	return out, nil
}
func parseTime(v string) time.Time { t, _ := time.Parse(time.RFC3339Nano, v); return t }
func storageFor(v *StorageSample, cutoff time.Time) UsageStorage {
	out := UsageStorage{Status: "not_measured", ExpectedProjects: 1}
	if v == nil {
		return out
	}
	age := max(int64(0), int64(cutoff.Sub(parseTime(v.ObservedAt))/time.Second))
	bytes := strconv.FormatInt(v.Bytes, 10)
	out.Bytes = &bytes
	out.ObservedAt = v.ObservedAt
	out.OldestObservedAt = v.ObservedAt
	out.AgeSeconds = &age
	out.MeasuredProjects = 1
	out.Status = "sampled"
	if age > int64(SampleStale/time.Second) {
		out.Status = "stale"
	}
	return out
}
func combineTenants(rows []UsageRow) ([]UsageRow, error) {
	out := []UsageRow{}
	indexes := map[string]int{}
	totals := []Counters{}
	for _, r := range rows {
		key := r.TenantID + "\x00" + r.PeriodStart
		i, ok := indexes[key]
		if !ok {
			i = len(out)
			indexes[key] = i
			v := r
			v.ProjectID = ""
			v.ProjectDeleted = false
			v.Counters = nil
			v.Storage = UsageStorage{Status: "not_measured"}
			v.QueryDuration = nil
			out = append(out, v)
			totals = append(totals, Counters{})
		}
		a := &out[i]
		if r.Counters != nil {
			c, e := wireCounters(r.Counters)
			if e != nil {
				return nil, e
			}
			if e = totals[i].Add(c); e != nil {
				return nil, e
			}
			a.Counters = totals[i].Wire()
		}
		if r.QueryDuration != nil {
			if a.QueryDuration == nil {
				a.QueryDuration = &UsageQueryDuration{Quality: "observed", Unresolved: "0"}
			}
			unresolved, e := strconv.ParseInt(a.QueryDuration.Unresolved, 10, 64)
			if e != nil {
				return nil, e
			}
			extra, _ := strconv.ParseInt(r.QueryDuration.Unresolved, 10, 64)
			unresolved, e = checkedUnresolved(unresolved, extra)
			if e != nil {
				return nil, e
			}
			a.QueryDuration.Unresolved = strconv.FormatInt(unresolved, 10)
			if unresolved > 0 {
				a.QueryDuration.Quality = "lower_bound"
			}
			a.QueryDuration.LastObserved = maxString(a.QueryDuration.LastObserved, r.QueryDuration.LastObserved)
		}
		a.Storage.ExpectedProjects++
		if r.Storage.Bytes != nil {
			value, _ := strconv.ParseInt(*r.Storage.Bytes, 10, 64)
			current := int64(0)
			if a.Storage.Bytes != nil {
				current, _ = strconv.ParseInt(*a.Storage.Bytes, 10, 64)
			}
			if value > int64(^uint64(0)>>1)-current {
				return nil, fmt.Errorf("usage storage overflow")
			}
			sum := strconv.FormatInt(current+value, 10)
			a.Storage.Bytes = &sum
			a.Storage.MeasuredProjects++
			a.Storage.ObservedAt = maxString(a.Storage.ObservedAt, r.Storage.ObservedAt)
			if a.Storage.OldestObservedAt == "" || r.Storage.ObservedAt < a.Storage.OldestObservedAt {
				a.Storage.OldestObservedAt = r.Storage.ObservedAt
			}
			if a.Storage.AgeSeconds == nil || *r.Storage.AgeSeconds > *a.Storage.AgeSeconds {
				age := *r.Storage.AgeSeconds
				a.Storage.AgeSeconds = &age
			}
			if r.Storage.Status == "stale" {
				a.Storage.Status = "stale"
			}
		}
	}
	for i := range out {
		v := &out[i].Storage
		if v.MeasuredProjects < v.ExpectedProjects {
			v.Bytes = nil
			if v.MeasuredProjects > 0 {
				v.Status = "partial"
			} else {
				v.Status = "not_measured"
			}
		} else if v.Status != "stale" {
			v.Status = "sampled"
		}
	}
	return out, nil
}
func wireCounters(w *UsageCounters) (Counters, error) {
	var c Counters
	values := []string{w.Live, w.Import, w.Queries, w.QuerySeconds, w.ExportBytes, w.PipelineBytes, w.Webhooks}
	target := []*int64{&c.Live, &c.Import, &c.Queries, &c.QueryMicros, &c.ExportBytes, &c.PipelineBytes, &c.Webhooks}
	for i, v := range values {
		if i == 3 {
			seconds, fraction, ok := strings.Cut(v, ".")
			whole, e := strconv.ParseInt(seconds, 10, 64)
			micros, f := strconv.ParseInt(fraction, 10, 64)
			if !ok || e != nil || f != nil || len(fraction) != 6 || whole < 0 || micros < 0 || micros > 999999 || whole > (math.MaxInt64-micros)/1000000 {
				return c, fmt.Errorf("invalid usage duration")
			}
			*target[i] = whole*1000000 + micros
		} else {
			n, e := strconv.ParseInt(v, 10, 64)
			if e != nil {
				return c, e
			}
			*target[i] = n
		}
	}
	return c, nil
}
