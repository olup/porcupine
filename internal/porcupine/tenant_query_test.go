package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

func TestTenantInteractiveAdmissionSharesProjectsAndPreservesHeavySlot(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	a, _ := project(t, s)
	b, _ := project(t, s)
	if _, err := s.control.Exec(`UPDATE tenants SET hot_query_max_concurrent=1 WHERE id=?`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	_, _, release, err := s.holdQuery(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{a, b} {
		if _, _, _, err = s.holdQuery(context.Background(), p); queryStatus(err) != 429 {
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
	_, _, otherRelease, err := s.holdQuery(context.Background(), c)
	if err != nil {
		t.Fatal("independent tenant blocked", err)
	}
	otherRelease()
	release()
	release()
	if s.queries.UsedSlots() != 0 {
		t.Fatal("query admission leaked")
	}
	// Fill worker capacity with both kinds of work. The global refusal
	// must not charge admission to the unrelated tenant.
	heavyRelease, ok := s.queries.TryBackground(t.Context())
	if !ok {
		t.Fatal("idle worker refused the export")
	}
	_, _, release, err = s.holdQuery(t.Context(), c)
	if err != nil {
		t.Fatal("heavy export consumed all interactive capacity", err)
	}
	if _, _, _, err = s.holdQuery(t.Context(), a); queryStatus(err) != 429 {
		t.Fatal("global slots ignored", err)
	}
	heavyRelease()
	if _, ok = s.queries.TryBackground(t.Context()); ok {
		t.Fatal("batch started during interactive use")
	}
	release()
	_, _, release, err = s.holdQuery(t.Context(), a)
	if err != nil {
		t.Fatal("global refusal leaked tenant slot", err)
	}
	release()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err = s.holdQuery(canceled, a); err == nil {
		t.Fatal("canceled request admitted")
	}
	if s.queries.UsedSlots() != 0 {
		t.Fatal("canceled request leaked capacity")
	}
	// Decreasing the limit leaves existing work alone but blocks new reads.
	_, _, first, err := s.holdQuery(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	_, _, second, err := s.holdQuery(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer second()
	if _, err := s.control.Exec(`UPDATE tenants SET hot_query_max_concurrent=1 WHERE id=$1`, other.ID); err != nil {
		t.Fatal(err)
	}
	first()
	if _, _, _, err = s.holdQuery(t.Context(), c); queryStatus(err) != 429 {
		t.Fatal("equal active count admitted", err)
	}
	second()
	_, _, third, err := s.holdQuery(t.Context(), c)
	if err != nil {
		t.Fatal("slots not recovered", err)
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
	if _, err := s.eventCatalog(context.Background(), &lakeapi.EventCatalogInput{ID: id, Limit: 20}); queryStatus(err) != 429 {
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
	_, _, release, err := s.holdQuery(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.control.Exec(`UPDATE tenants SET hot_query_max_concurrent=1 WHERE id=?`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.eventCatalog(context.Background(), &lakeapi.EventCatalogInput{ID: id, Limit: 20}); queryStatus(err) != 429 {
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
	if s.queries.UsedSlots() != 0 {
		t.Fatal("failure leaked query capacity")
	}
}

type recordingQueryLimits struct {
	query.Reader

	received lakeapi.QueryExecutionLimits
}

func (s *recordingQueryLimits) Query(ctx context.Context, project, sql string, parameters map[string]string, limits lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error) {
	s.received = limits
	return s.Reader.Query(ctx, project, sql, parameters, limits)
}

func TestTenantInteractiveRaisedDeploymentBudgetsReachNativeQuery(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	s.queries = query.New(query.InteractiveQueryConfig{MaxConcurrent: 2, TimeoutMS: 6000, MaxRows: 2000, MaxResultBytes: 4 << 20}, s.clock, nil)
	s.queries.Bind(s.control, "", 0)
	s.queries.SetLocalRecorder(s.usage.recorder)
	spy := &recordingQueryLimits{Reader: s.lake}
	s.standaloneReader = spy
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
	for _, c := range []query.InteractiveQueryConfig{{MaxConcurrent: 1}, {TimeoutMS: -1}, {TimeoutMS: 60001}, {MaxRows: 100001}, {MaxResultBytes: 255}} {
		if _, err := query.Normalize(&c); err == nil {
			t.Fatal("unsafe config accepted", c)
		}
	}
	c, err := query.Normalize(nil)
	if err != nil || c.MaxConcurrent != 2 || c.TimeoutMS != 5000 || c.MaxRows != 1000 || c.MaxResultBytes != 1<<20 {
		t.Fatal(c, err)
	}
}
