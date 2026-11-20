package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/query"
)

func TestDuckLakeS3SharedPostgresHostedQueryRoutesAndMetering(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	a, b := sharedNativeJourneyPair(t, fixture)
	for _, node := range []*sharedJourneyNode{a, b} {
		node.service.usage.cancel()
		<-node.service.usage.done
	}
	control := a.service.control
	ctx := t.Context()
	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Hosted query")
	uninitialized := eventapi.NewID()
	if _, err := control.ExecContext(ctx, `INSERT INTO projects(id,tenant_id,name,created_at) VALUES($1,$2,'Uninitialized query',$3)`, uninitialized, defaultTenantID, now()); err != nil {
		t.Fatal(err)
	}
	_, reader := sharedJourneyKey(t, a.host.URL, project, []string{"events:read"})
	_, other := sharedJourneyKey(t, a.host.URL, uninitialized, []string{"events:read"})
	sb := b.service.lake
	if _, err := control.ExecContext(ctx, `CREATE TABLE hosted_query_test_activity (project TEXT NOT NULL, id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	register := func(ctx context.Context, tx *sql.Tx, project, id string) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO hosted_query_test_activity(project,id) VALUES($1,$2)`, project, id)
		return err
	}
	var failFinish atomic.Bool
	finish := func(ctx context.Context, db *sql.DB, project, id string) error {
		if failFinish.Load() {
			return errors.New("injected activity acknowledgement failure")
		}
		_, err := db.ExecContext(ctx, `DELETE FROM hosted_query_test_activity WHERE project=$1 AND id=$2`, project, id)
		return err
	}
	for _, node := range []*sharedJourneyNode{a, b} {
		if err := node.service.initializeHostedQueries(node.service.lake, register, finish); err != nil {
			t.Fatal(err)
		}
	}

	base := "/api/v1/projects/" + project
	failFinish.Store(true)
	for _, route := range []string{"/schema", "/catalog", "/event-profile?event=sample"} {
		response := sharedJourneyHTTP(t, b.host.URL, http.MethodGet, base+route, reader, nil, nil, http.StatusOK)
		delivery := response["delivery"].(map[string]any)
		if delivery["pending_unknown"] != true {
			t.Fatal("unexpected hosted delivery status", delivery)
		}
	}
	response := sharedJourneyQuery(t, a.host.URL, project, reader, "SELECT count(*) AS total FROM events", http.StatusOK)
	rows := response["result"].(map[string]any)["data"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["total"] != "0" {
		t.Fatal("unexpected hosted analytical result", rows)
	}
	var admitted, unresolved int64
	if err := control.QueryRowContext(ctx, `SELECT query_count,unresolved_queries FROM usage_rollups WHERE source_id=$1 AND project_id=$2`, query.HostedUsageSource, project).Scan(&admitted, &unresolved); err != nil || admitted != 3 || unresolved != 3 {
		t.Fatal("admission must be visible in PostgreSQL before observation", admitted, unresolved, err)
	}
	if a.service.db != nil || b.service.db != nil {
		t.Fatal("hosted query workers opened a local journal")
	}
	var permits, activities int
	if err := control.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM shared_query_permits WHERE project_id=$1), (SELECT count(*) FROM hosted_query_test_activity WHERE project=$1)`, project).Scan(&permits, &activities); err != nil || permits != 0 || activities != 3 {
		t.Fatal("permit release and failed activity acknowledgement were conflated", permits, activities, err)
	}
	failFinish.Store(false)
	for _, node := range []*sharedJourneyNode{a, b} {
		for range 2 {
			if err := node.service.synchronizeUsage(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	var micros int64
	if err := control.QueryRowContext(ctx, `SELECT query_count,query_micros,unresolved_queries,(SELECT count(*) FROM hosted_query_test_activity) FROM usage_rollups WHERE source_id=$1 AND project_id=$2`, query.HostedUsageSource, project).Scan(&admitted, &micros, &unresolved, &activities); err != nil || admitted != 3 || micros < 1 || unresolved != 0 || activities != 0 {
		t.Fatal("observations must converge without double counting", admitted, micros, unresolved, activities, err)
	}

	sharedJourneyQuery(t, b.host.URL, project, other, "SELECT count(*) FROM events", http.StatusForbidden)
	for _, route := range []string{"/catalog", "/event-profile?event=sample"} {
		sharedJourneyHTTP(t, b.host.URL, http.MethodGet, "/api/v1/projects/"+uninitialized+route, other, nil, nil, http.StatusServiceUnavailable)
	}
	sharedJourneyQuery(t, b.host.URL, uninitialized, other, "SELECT count(*) FROM events", http.StatusServiceUnavailable)
	var registered int
	if err := sb.SharedCatalog.QueryRowContext(ctx, `SELECT count(*) FROM pc_native_projects WHERE source_id=$1 AND project_id=$2`, sb.Config.SharedSourceID, uninitialized).Scan(&registered); err != nil || registered != 0 {
		t.Fatal("read registered an uninitialized catalog", registered, err)
	}
}
