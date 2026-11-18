package porcupine

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/olup/porcupine/internal/usage"
)

// stopHostedUsageLoops leaves observation, sampling and recovery to the test.
func stopHostedUsageLoops(nodes ...*sharedJourneyNode) {
	for _, node := range nodes {
		node.service.usage.cancel()
		<-node.service.usage.done
	}
}

func hostedUsageRows(t *testing.T, node *sharedJourneyNode, tenant string) map[string]map[string]any {
	t.Helper()
	at := node.service.clock()
	path := "/api/v1/usage?start=" + usage.Day(at) + "&end=" + usage.Day(at.AddDate(0, 0, 1)) + "&tenant_id=" + tenant
	page := sharedJourneyHTTP(t, node.host.URL, http.MethodGet, path, adminToken, nil, nil, http.StatusOK)
	rows := map[string]map[string]any{}
	for _, row := range page["rows"].([]any) {
		row := row.(map[string]any)
		rows[row["project_id"].(string)] = row
	}
	return rows
}

func TestSharedHostedUsageStorageSamplesCurrentParquet(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	a, b := sharedNativeJourneyPair(t, fixture)
	stopHostedUsageLoops(a, b)
	ctx := t.Context()
	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Sampled")
	other, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Neighbor")
	hostedCleanupWrite(t, a.service, project, "first")
	hostedCleanupWrite(t, a.service, project, "second")
	hostedCleanupWrite(t, a.service, other, "neighbor")

	if next, err := b.service.sampleHostedUsageStorage(ctx, ""); err != nil || next != "" {
		t.Fatalf("hosted sampling pass = %q, %v", next, err)
	}
	rows := hostedUsageRows(t, a, defaultTenantID)
	storage := rows[project]["storage"].(map[string]any)
	if storage["status"] != "sampled" || storage["current_parquet_bytes"] == nil {
		t.Fatalf("hosted storage was not measured: %v", storage)
	}
	measured, err := strconv.ParseInt(storage["current_parquet_bytes"].(string), 10, 64)
	if err != nil || measured <= 0 {
		t.Fatalf("hosted current Parquet bytes = %v, %v", storage["current_parquet_bytes"], err)
	}
	// Two appends and no maintenance leave every written Parquet object current.
	var objects int64
	prefix := fixture.projectPrefix(t, b.service.hosted.native, project)
	pager := s3.NewListObjectsV2Paginator(fixture.client, &s3.ListObjectsV2Input{Bucket: aws.String(fixture.storage.Bucket), Prefix: aws.String(prefix)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, object := range page.Contents {
			if strings.HasSuffix(aws.ToString(object.Key), ".parquet") {
				objects += aws.ToInt64(object.Size)
			}
			t.Log("DEBUG s3", aws.ToString(object.Key), aws.ToInt64(object.Size))
		}
		schema := "pc_lake_" + strings.ReplaceAll(lakeapiCatalog(t, b, project), "-", "")
		dbg, _ := a.service.control.QueryContext(ctx, `SELECT d.table_id,t.table_name,d.path,d.file_size_bytes,d.begin_snapshot,coalesce(d.end_snapshot,-1) FROM "`+schema+`".ducklake_data_file d JOIN "`+schema+`".ducklake_table t USING(table_id) UNION ALL SELECT f.table_id,'DELETE',f.path,f.file_size_bytes,f.begin_snapshot,coalesce(f.end_snapshot,-1) FROM "`+schema+`".ducklake_delete_file f`)
		for dbg != nil && dbg.Next() {
			var id, size, begin, end int64
			var name, path string
			_ = dbg.Scan(&id, &name, &path, &size, &begin, &end)
			t.Log("DEBUG meta", id, name, path, size, begin, end)
		}
	}
	if measured != objects {
		t.Fatalf("metadata measured %d bytes, the project's S3 Parquet objects hold %d", measured, objects)
	}
	if neighbor := rows[other]["storage"].(map[string]any); neighbor["status"] != "sampled" || neighbor["current_parquet_bytes"] == storage["current_parquet_bytes"] {
		t.Fatalf("neighbor project was not measured separately: %v", neighbor)
	}

	// A fresh sample is shared: another worker's pass does not re-read it.
	var observed string
	if err := a.service.control.QueryRowContext(ctx, `SELECT observed_at FROM usage_storage_samples WHERE project_id=$1`, project).Scan(&observed); err != nil {
		t.Fatal(err)
	}
	if next, err := a.service.sampleHostedUsageStorage(ctx, ""); err != nil || next != "" {
		t.Fatalf("second hosted sampling pass = %q, %v", next, err)
	}
	var again string
	if err := a.service.control.QueryRowContext(ctx, `SELECT observed_at FROM usage_storage_samples WHERE project_id=$1`, project).Scan(&again); err != nil || again != observed {
		t.Fatalf("fresh sample was measured again: %q -> %q, %v", observed, again, err)
	}
}

func TestSharedHostedTenantUsageIsTheEnforcedCounter(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	a, b := sharedNativeJourneyPair(t, fixture)
	ctx := t.Context()
	created := sharedJourneyHTTP(t, a.host.URL, http.MethodPost, "/api/v1/tenants", adminToken, []byte(`{"name":"Metered"}`), nil, http.StatusCreated)
	tenant := created["tenant"].(map[string]any)["id"].(string)
	budget := func(events int) {
		t.Helper()
		body := `{"ingestion_monthly_events":` + strconv.Itoa(events) + `,"hot_query_max_concurrent":1,"hot_query_timeout_ms":1000,"hot_query_max_rows":100,"hot_query_max_result_bytes":65536}`
		sharedJourneyHTTP(t, b.host.URL, http.MethodPut, "/api/v1/tenants/"+tenant+"/budgets", adminToken, []byte(body), nil, http.StatusOK)
	}
	budget(3)
	project, ingest := sharedJourneyProject(t, a.host.URL, tenant, "Metered events")
	neighbor, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Unmetered events")
	for i := range 3 {
		hostedCleanupWrite(t, []*Service{a.service, b.service}[i%2], project, "accepted")
	}
	hostedCleanupWrite(t, a.service, neighbor, "other tenant")

	reported := sharedJourneyHTTP(t, b.host.URL, http.MethodGet, "/api/v1/tenants/"+tenant+"/usage", adminToken, nil, nil, http.StatusOK)
	if reported["accepted_events"] != "3" || reported["remaining_events"] != "0" || reported["ingestion_allowed"] != false {
		t.Fatalf("get-tenant-usage after three hosted acceptances: %v", reported)
	}
	for _, node := range []*sharedJourneyNode{a, b} {
		policy := &node.service.hosted.flusher.policy
		policy.mu.Lock()
		policy.refreshed = time.Time{}
		policy.mu.Unlock()
		var denied *tenantIngestionError
		if err := policy.admit(ctx, tenant); !errors.As(err, &denied) || denied.code != "ingestion_budget_exceeded" {
			t.Fatalf("enforcement admitted past the reported budget: %v", err)
		}
		policy.mu.Lock()
		used := policy.tenants[tenant].used
		policy.mu.Unlock()
		if strconv.FormatInt(used, 10) != reported["accepted_events"] {
			t.Fatalf("enforcement counted %d, get-tenant-usage reported %v", used, reported["accepted_events"])
		}
		if err := policy.admit(ctx, defaultTenantID); err != nil {
			t.Fatal("another tenant's budget was charged", err)
		}
	}
	sharedJourneyNative(t, a.host.URL, ingest, `{"type":"track","message_id":"over-budget","actor_id":"u","name":"message"}`, http.StatusTooManyRequests)

	budget(4)
	reported = sharedJourneyHTTP(t, a.host.URL, http.MethodGet, "/api/v1/tenants/"+tenant+"/usage", adminToken, nil, nil, http.StatusOK)
	if reported["accepted_events"] != "3" || reported["remaining_events"] != "1" || reported["ingestion_allowed"] != true {
		t.Fatalf("raising the budget changed consumption: %v", reported)
	}
	policy := &b.service.hosted.flusher.policy
	policy.mu.Lock()
	policy.refreshed = time.Time{}
	policy.mu.Unlock()
	if err := policy.admit(ctx, tenant); err != nil {
		t.Fatal("enforcement rejected an allowance get-tenant-usage reports", err)
	}
}

func TestSharedHostedQueryUsageRecoversOnlyAbandonedExecutions(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	a, b := sharedNativeJourneyPair(t, fixture)
	stopHostedUsageLoops(a, b)
	ctx := t.Context()
	control := a.service.control
	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Queried")

	// A worker that crashed after admitting a query: its record was last
	// observed long ago and no process holds it any more.
	crashed := usage.NewRecorder(control, hostedQueryUsageSource, func() time.Time { return time.Now().Add(-time.Hour) })
	if _, err := crashed.StartExecution(ctx, defaultTenantID, project, "sql"); err != nil {
		t.Fatal(err)
	}
	// A live worker's query that started long ago keeps being observed.
	live, err := a.service.hostedQueries.recorder.StartExecution(ctx, defaultTenantID, project, "sql")
	if err != nil {
		t.Fatal(err)
	}
	live.Begin()
	if _, err = control.ExecContext(ctx, `UPDATE usage_executions SET last_observed_at=$1 WHERE closed=0`, usage.Stamp(time.Now().Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err = a.service.synchronizeUsage(ctx); err != nil {
		t.Fatal(err)
	}

	type record struct{ closed, lost int }
	records := func() map[string]record {
		t.Helper()
		rows, err := control.QueryContext(ctx, `SELECT id,closed,lost FROM usage_executions WHERE project_id=$1`, project)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]record{}
		for rows.Next() {
			var id string
			var r record
			if err = rows.Scan(&id, &r.closed, &r.lost); err != nil {
				t.Fatal(err)
			}
			out[id] = r
		}
		return out
	}
	liveID := func() string {
		t.Helper()
		var id string
		if err := control.QueryRowContext(ctx, `SELECT id FROM usage_executions WHERE project_id=$1 AND last_observed_at>$2`, project, usage.Stamp(time.Now().Add(-time.Minute))).Scan(&id); err != nil {
			t.Fatal("live execution was not observed", err)
		}
		return id
	}()

	if n, err := b.service.recoverHostedQueryUsage(ctx); err != nil || n != 1 {
		t.Fatalf("recovery on another worker marked %d executions: %v", n, err)
	}
	for id, r := range records() {
		want := record{closed: 0, lost: 1}
		if id == liveID {
			want = record{}
		}
		if r != want {
			t.Fatalf("execution %s = %+v, want %+v", id, r, want)
		}
	}
	if n, err := a.service.recoverHostedQueryUsage(ctx); err != nil || n != 0 {
		t.Fatalf("repeated recovery marked %d executions: %v", n, err)
	}

	live.Finish()
	if err = a.service.synchronizeUsage(ctx); err != nil {
		t.Fatal(err)
	}
	if r := records()[liveID]; r != (record{closed: 1}) {
		t.Fatalf("live execution did not finish normally: %+v", r)
	}
	// Recovery invents no finish: the abandoned admission stays a lower bound.
	var admitted, unresolved int64
	if err = control.QueryRowContext(ctx, `SELECT query_count,unresolved_queries FROM usage_rollups WHERE source_id=$1 AND project_id=$2`, hostedQueryUsageSource, project).Scan(&admitted, &unresolved); err != nil || admitted != 2 || unresolved != 1 {
		t.Fatalf("query rollup = %d admitted, %d unresolved: %v", admitted, unresolved, err)
	}
	if row := hostedUsageRows(t, b, defaultTenantID)[project]; row["query_duration"].(map[string]any)["quality"] != "lower_bound" {
		t.Fatalf("abandoned execution was reported as observed: %v", row)
	}
}

func lakeapiCatalog(t *testing.T, b *sharedJourneyNode, project string) string {
	var id string
	if err := b.service.control.QueryRow(`SELECT catalog_id FROM pc_native_projects WHERE project_id=$1`, project).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}
