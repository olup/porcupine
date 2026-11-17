package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestTenantInteractiveAdmissionSharesProjectsAndPreservesHeavySlot(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	a, _ := project(t, s)
	b, _ := project(t, s)
	if _, err := s.control.Exec(`UPDATE tenants SET hot_query_max_concurrent=1 WHERE id=?`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	_, _, release, err := s.beginInteractiveQuery(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{a, b} {
		if _, _, _, err = s.beginInteractiveQuery(context.Background(), p); queryStatus(err) != 429 {
			t.Fatal("projects bypass shared tenant slots", err)
		}
	}
	other, err := s.insertTenant(context.Background(), "Independent", s.defaultTenantBudgets())
	if err != nil {
		t.Fatal(err)
	}
	c, _ := project(t, s)
	if _, err = s.control.Exec(`UPDATE projects SET tenant_id=? WHERE id=?`, other.ID, c); err != nil {
		t.Fatal(err)
	}
	_, _, otherRelease, err := s.beginInteractiveQuery(context.Background(), c)
	if err != nil {
		t.Fatal("independent tenant blocked", err)
	}
	otherRelease()
	release()
	release()
	if len(s.querySlots) != 0 || len(s.tenantQueries.active) != 0 {
		t.Fatal("query admission leaked")
	}
	// Failure to reserve global capacity must roll back the tenant reservation.
	for range cap(s.querySlots) {
		s.querySlots <- struct{}{}
	}
	if _, _, _, err = s.beginInteractiveQuery(context.Background(), a); queryStatus(err) != 429 {
		t.Fatal("global slots ignored", err)
	}
	for range cap(s.querySlots) {
		<-s.querySlots
	}
	if len(s.tenantQueries.active) != 0 {
		t.Fatal("global refusal leaked tenant slot")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err = s.beginInteractiveQuery(canceled, a); err == nil {
		t.Fatal("canceled request admitted")
	}
	if len(s.querySlots) != 0 || len(s.tenantQueries.active) != 0 {
		t.Fatal("canceled request leaked capacity")
	}
	// Heavy exports retain their existing global admission, independent of tenant slots.
	tenantRelease, ok := s.tenantQueries.acquire(defaultTenantID, 1)
	if !ok {
		t.Fatal("tenant slot not released")
	}
	heavyRelease, ok := s.acquirePipelineExport(context.Background())
	if !ok {
		t.Fatal("tenant slots affected background export")
	}
	_, _, release, err = s.beginInteractiveQuery(context.Background(), c)
	if err != nil {
		t.Fatal("heavy export consumed all interactive capacity", err)
	}
	release()
	heavyRelease()
	tenantRelease()
	// Existing work is not canceled when an operator lowers concurrency, but no
	// further work is admitted until the active count is below the new ceiling.
	first, _ := s.tenantQueries.acquire("lower", 2)
	second, _ := s.tenantQueries.acquire("lower", 2)
	if _, ok = s.tenantQueries.acquire("lower", 1); ok {
		t.Fatal("lowered policy ignored")
	}
	first()
	if _, ok = s.tenantQueries.acquire("lower", 1); ok {
		t.Fatal("equal active count admitted")
	}
	second()
	third, ok := s.tenantQueries.acquire("lower", 1)
	if !ok {
		t.Fatal("slots not recovered")
	}
	third()
}

func TestTenantInteractiveQueryRuntimeBudgetsAndDiscovery(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	id, _ := project(t, s)
	batch := make([]lakeapi.EventProjection, 40)
	for i := range batch {
		batch[i] = lakeStorageProjection(t, id, fmt.Sprintf("budget-%d", i), int64(i+1))
	}
	if err := s.lake.Apply(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	set := func(timeout, rows, bytes int) {
		t.Helper()
		if _, err := s.control.Exec(`UPDATE tenants SET hot_query_timeout_ms=?,hot_query_max_rows=?,hot_query_max_result_bytes=? WHERE id=?`, timeout, rows, bytes, defaultTenantID); err != nil {
			t.Fatal(err)
		}
	}
	query := func(sql string) *AnalyticsQueryOutput {
		t.Helper()
		out, err := s.analyticsQuery(context.Background(), queryInput(id, sql))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	set(5000, 1, 1<<20)
	if _, err := s.analyticsQuery(context.Background(), queryInput(id, `SELECT id FROM events LIMIT 2`)); queryStatus(err) != 429 {
		t.Fatal("row policy ignored", err)
	}
	query(`SELECT count(*) AS n FROM events`)
	set(5000, 1000, 256)
	for _, sql := range []string{`SELECT id FROM events LIMIT 1`, `SELECT 1 AS empty_result WHERE false`} {
		_, err := s.analyticsQuery(context.Background(), queryInput(id, sql))
		if queryStatus(err) != 429 {
			t.Fatal("byte policy ignored including empty result", err)
		}
	}
	if _, err := s.lakeEventCatalog(context.Background(), lakeapi.EventCatalogInput{ID: id, Limit: 20}); queryStatus(err) != 429 {
		t.Fatal("catalog byte policy ignored", err)
	}
	if _, err := s.eventProfile(context.Background(), &lakeapi.EventProfileInput{ID: id, Event: batch[0].Event.Event, SampleLimit: 5, PropertyLimit: 5, PropertyKind: "all"}); queryStatus(err) != 429 {
		t.Fatal("profile byte policy ignored", err)
	}
	set(20, 1000, 1<<20)
	started := time.Now()
	_, err := s.analyticsQuery(context.Background(), queryInput(id, `SELECT sum(a.timestamp_ms % (b.timestamp_ms+1) + c.timestamp_ms % (d.timestamp_ms+1)) FROM events a CROSS JOIN events b CROSS JOIN events c CROSS JOIN events d CROSS JOIN events e CROSS JOIN events f`))
	if queryStatus(err) != 429 || time.Since(started) > 3*time.Second {
		t.Fatal("execution was not canceled", err, time.Since(started))
	}
	set(5000, 1000, 1<<20)
	query(`SELECT count(*) AS n FROM events`)
	_, _, release, err := s.beginInteractiveQuery(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.control.Exec(`UPDATE tenants SET hot_query_max_concurrent=1 WHERE id=?`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.lakeEventCatalog(context.Background(), lakeapi.EventCatalogInput{ID: id, Limit: 20}); queryStatus(err) != 429 {
		t.Fatal("catalog bypassed tenant admission", err)
	}
	if _, err = s.eventProfile(context.Background(), &lakeapi.EventProfileInput{ID: id, Event: batch[0].Event.Event, SampleLimit: 5, PropertyLimit: 5, PropertyKind: "all"}); queryStatus(err) != 429 {
		t.Fatal("profile bypassed tenant admission", err)
	}
	release()
	// Tenant policy can be larger than deployment capacity; discovery gives both
	// the configured policy and the effective ceilings without aggregate usage.
	set(60000, 100000, 64<<20)
	schema, err := s.analyticsSchema(context.Background(), &AnalyticsSchemaInput{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	if schema.Body.Limits.MaxExecutionMS != 5000 || schema.Body.Limits.MaxRows != 1000 || schema.Body.Limits.MaxConcurrentQueries != 1 || schema.Body.TenantBudgets.HotQueryTimeoutMS != 60000 || schema.Body.DeploymentLimits.MaxRows != 1000 {
		t.Fatalf("inaccurate query policy discovery: %+v", schema.Body)
	}
	encoded, _ := json.Marshal(schema.Body)
	if strings.Contains(string(encoded), "ingested_events") || strings.Contains(string(encoded), "used_events") {
		t.Fatal("discovery leaked aggregate tenant usage")
	}
	if len(s.querySlots) != 0 || len(s.tenantQueries.active) != 0 {
		t.Fatal("failure leaked query capacity")
	}
}

type recordingQueryLimits struct {
	lakeapi.LakeStore

	received lakeapi.QueryExecutionLimits
}

func (s *recordingQueryLimits) Query(ctx context.Context, project, sql string, parameters map[string]string, limits lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error) {
	s.received = limits
	return s.LakeStore.Query(ctx, project, sql, parameters, limits)
}

func TestTenantInteractiveRaisedDeploymentBudgetsReachNativeQuery(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	s.interactiveConfig = InteractiveQueryConfig{MaxConcurrent: 2, TimeoutMS: 6000, MaxRows: 2000, MaxResultBytes: 4 << 20}
	spy := &recordingQueryLimits{LakeStore: s.lake}
	s.lake = spy
	id, _ := project(t, s)
	if _, err := s.control.Exec(`UPDATE tenants SET hot_query_timeout_ms=6000,hot_query_max_rows=2000,hot_query_max_result_bytes=? WHERE id=?`, 4<<20, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	batch := make([]lakeapi.EventProjection, 40)
	for i := range batch {
		batch[i] = lakeStorageProjection(t, id, fmt.Sprintf("raised-%d", i), int64(i+1))
	}
	if err := s.lake.Apply(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	out, err := s.analyticsQuery(context.Background(), queryInput(id, `SELECT a.id FROM events a CROSS JOIN events b LIMIT 1500`))
	if err != nil || out.Body.Result.Rows != 1500 {
		t.Fatal("hardcoded native row ceiling remains", err)
	}
	input := queryInput(id, `SELECT $value AS value FROM events a CROSS JOIN events b LIMIT 500`)
	input.Body.Parameters = map[string]string{"value": strings.Repeat("x", 4096)}
	out, err = s.analyticsQuery(context.Background(), input)
	if err != nil || out.Body.Result.Rows != 500 {
		t.Fatal("hardcoded native byte ceiling remains", err)
	}
	if spy.received.Timeout != 6*time.Second {
		t.Fatal("native timeout did not receive deployment ceiling", spy.received)
	}
	schema, err := s.analyticsSchema(context.Background(), &AnalyticsSchemaInput{ID: id})
	if err != nil || schema.Body.Limits.MaxExecutionMS != 6000 {
		t.Fatal("raised execution ceiling not discoverable", err)
	}
}

func TestInteractiveQueryConfigurationCeilings(t *testing.T) {
	t.Parallel()
	for _, c := range []InteractiveQueryConfig{{MaxConcurrent: 1}, {TimeoutMS: -1}, {TimeoutMS: 60001}, {MaxRows: 100001}, {MaxResultBytes: 255}} {
		if _, err := normalizeInteractiveQueries(&c); err == nil {
			t.Fatal("unsafe config accepted", c)
		}
	}
	c, err := normalizeInteractiveQueries(nil)
	if err != nil || c.MaxConcurrent != 2 || c.TimeoutMS != 5000 || c.MaxRows != 1000 || c.MaxResultBytes != 1<<20 {
		t.Fatal(c, err)
	}
}
