package lake

import (
	"errors"
	"testing"
)

func TestDuckLakeS3SharedPostgresHostedMaintenanceGuarded(t *testing.T) {
	t.Parallel()
	ctx, a, b, scope, _ := hostedMaintenanceExperiment(t)
	_, coordinator := nativeFenceControl(t)
	owner := acquireFenceSession(t, coordinator, scope.Key)
	fence := owner.Fence(scope.StreamID)
	if err := a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	p := a.Projects[scope.Key.ProjectID]
	for _, marker := range []string{"one", "two", "three"} {
		if err := fenceWitnessTx(ctx, p, fence, marker, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	before := maintenanceExperimentFiles(t, ctx, p)
	var retriesBefore, retriesAfter int
	if err := p.DB.QueryRowContext(ctx, `SELECT current_setting('ducklake_max_retry_count')`).Scan(&retriesBefore); err != nil {
		t.Fatal(err)
	}
	stats, err := b.MaintainHostedProject(ctx, owner)
	if err != nil || !stats.Completed || stats.FilesProcessed == 0 || stats.FilesCreated == 0 {
		t.Fatal(stats, err)
	}
	if after := maintenanceExperimentFiles(t, ctx, p); after >= before {
		t.Fatal("guarded maintenance did no work", before, after)
	}
	if err := p.DB.QueryRowContext(ctx, `SELECT current_setting('ducklake_max_retry_count')`).Scan(&retriesAfter); err != nil || retriesBefore != retriesAfter || retriesBefore == 0 {
		t.Fatal("ordinary engine retry policy changed", retriesBefore, retriesAfter, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal("dedicated engine or resident lease leaked", err)
	}
	if _, err := a.SealHostedProject(ctx, fence); err != nil {
		t.Fatal(err)
	}
	if stats, err := a.MaintainHostedProject(ctx, owner); !errors.Is(err, ErrProjectWriteFenceStale) || stats.Completed {
		t.Fatal("sealed maintenance accepted", stats, err)
	}
	if stats, err := a.MaintainHostedProject(ctx, nil); !errors.Is(err, ErrProjectWriteFenceStale) || stats.Completed {
		t.Fatal("missing session accepted", stats, err)
	}
	if err := p.MaintenanceCommand(ctx, "CALL ducklake_merge_adjacent_files('lake')", &DuckLakeMaintenanceStats{}); !errors.Is(err, ErrHostedNativeGuardRequired) {
		t.Fatal("legacy hosted path opened", err)
	}
}

func TestDuckLakeS3SharedPostgresHostedMaintenanceRetriesFreshGuard(t *testing.T) {
	t.Parallel()
	ctx, a, b, scope, pause := hostedMaintenanceExperiment(t)
	_, coordinator := nativeFenceControl(t)
	owner := acquireFenceSession(t, coordinator, scope.Key)
	fence := owner.Fence(scope.StreamID)
	if err := a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	p := a.Projects[scope.Key.ProjectID]
	for _, marker := range []string{"one", "two", "three"} {
		if err := fenceWitnessTx(ctx, p, fence, marker, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	pause.armed.Store(true)
	type outcome struct {
		stats DuckLakeMaintenanceStats
		err   error
	}
	result := make(chan outcome, 1)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		stats, err := b.MaintainHostedProject(ctx, owner)
		result <- outcome{stats, err}
	}()
	defer func() { pause.release(); <-returned }()
	select {
	case <-pause.entered:
	case result := <-result:
		t.Fatal("maintenance returned before native PUT pause", result)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := b.Close(); err == nil {
		t.Fatal("store closed while dedicated engine was active")
	}
	// Another native commit with this still-valid acquisition occupies snapshot+1.
	// Maintenance must retry the whole guarded transaction, without a new epoch.
	if err := fenceWitnessTx(ctx, p, fence, "intervening", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	pause.release()
	select {
	case result := <-result:
		if result.err != nil || !result.stats.Completed {
			t.Fatal("valid owner did not retry fresh snapshot", result)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var count int
	if err := p.DB.QueryRowContext(ctx, `SELECT count(*) FROM lake.heads`).Scan(&count); err != nil || count != 4 {
		t.Fatal("retry lost native rows", count, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal("maintenance leaked engine/lease", err)
	}
}
