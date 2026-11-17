package porcupine

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func sharedStatusRead(t *testing.T, s *Service) InstanceStatus {
	t.Helper()
	result, err := s.getInstanceStatus(instanceStatusReaderContext(t.Context()), &struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	return result.Body
}

func TestSharedInstanceStatusAnalyticalMetadataFailure(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	s := service(t, 20)
	native := instanceStatusNativeStore(t, s)
	metadata, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// The native SourceID probe checks the local registry. Its separate metadata
	// connection must also participate in health, independently of control storage.
	native.SharedCatalog = metadata
	if result := sharedStatusRead(t, s); result.Status != "ok" || result.Checks.DuckLake != dependencyOK {
		t.Fatalf("healthy analytical metadata looked unavailable: %+v", result)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	result := sharedStatusRead(t, s)
	if result.Status != "degraded" || result.Checks.DuckLake != dependencyUnavailable || result.Checks.SQLite != dependencyOK {
		t.Fatalf("analytical metadata failure disappeared behind local registry health: %+v", result)
	}
}

func TestSharedInstanceStatusControlRegistrationAndPrivacy(t *testing.T) {
	t.Parallel()
	a, _ := sharedJourneyPair(t, nil)
	healthy := sharedStatusRead(t, a.service)
	if healthy.Status != "ok" || healthy.Checks.SharedControl != dependencyOK || healthy.Checks.WorkerRegistration != dependencyOK || !healthy.Configuration.SharedControl || !healthy.Configuration.OwnerRouting {
		t.Fatalf("healthy shared dependencies were not reported: %+v", healthy)
	}
	raw, err := json.Marshal(healthy)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{a.service.shared.config.DatabaseURL, a.host.URL, sharedJourneySecret, a.service.shared.config.WorkerID, a.service.shared.routing.token, "porcupine-test-only"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("shared status disclosed a connection, route or credential")
		}
	}
	if _, err := a.service.control.Exec(`UPDATE shared_workers SET registration_token='superseded-registration' WHERE worker_id=$1`, a.service.shared.config.WorkerID); err != nil {
		t.Fatal(err)
	}
	stale := sharedStatusRead(t, a.service)
	if stale.Status != "degraded" || stale.Checks.SharedControl != dependencyOK || stale.Checks.WorkerRegistration != dependencyUnavailable || !stale.Configuration.OwnerRouting {
		t.Fatalf("superseded worker registration looked healthy: %+v", stale)
	}
}

func TestSharedInstanceStatusDetectsLostRegistrationSession(t *testing.T) {
	t.Parallel()
	a, _ := sharedJourneyPair(t, nil)
	if _, err := a.service.control.Exec(`SELECT pg_terminate_backend($1)`, a.service.shared.routing.backendPID); err != nil {
		t.Fatal(err)
	}
	result := sharedStatusRead(t, a.service)
	if result.Status != "degraded" || result.Checks.SharedControl != dependencyOK || result.Checks.WorkerRegistration != dependencyUnavailable {
		t.Fatalf("lost dedicated registration session looked healthy before heartbeat: %+v", result)
	}
}

func TestSharedInstanceStatusMandatoryControlFailure(t *testing.T) {
	t.Parallel()
	a, _ := sharedJourneyPair(t, nil)
	// Invoke the handler with an already authenticated reader context: when the
	// database fails after authentication, status must still report that failure.
	if err := a.service.control.Close(); err != nil {
		t.Fatal(err)
	}
	result := sharedStatusRead(t, a.service)
	if result.Status != "degraded" || result.Checks.SQLite != dependencyOK || result.Checks.SharedControl != dependencyUnavailable || result.Checks.WorkerRegistration != dependencyUnavailable || !result.Configuration.SharedControl {
		t.Fatalf("shared control failure disappeared behind local SQLite health: %+v", result)
	}
}

func TestSharedInstanceStatusControlProbeIsBounded(t *testing.T) {
	t.Parallel()
	a, _ := sharedJourneyPair(t, nil)
	// The dedicated registration session occupies the only connection. Pool
	// admission must honor the one-second status deadline rather than hanging.
	a.service.control.SetMaxOpenConns(1)
	a.service.control.SetMaxIdleConns(0)
	defer func() {
		a.service.control.SetMaxOpenConns(16)
		a.service.control.SetMaxIdleConns(4)
	}()
	started := time.Now()
	result := sharedStatusRead(t, a.service)
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("control probe exceeded its bound: %s", elapsed)
	}
	if result.Status != "degraded" || result.Checks.SharedControl != dependencyUnavailable || result.Checks.SQLite != dependencyOK {
		t.Fatalf("blocked control probe looked healthy: %+v", result)
	}
}
