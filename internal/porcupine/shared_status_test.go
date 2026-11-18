package porcupine

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/query"
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
	native := openSharedNativeMetadataTest(t, dsn)
	s.lake, s.lakeMemoryMB = native, 128
	// Native identity comes from the real PG registry, independently of the
	// service's local control database. It has no local registry fallback.
	metadata := native.SharedCatalog
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

func sharedStatusControlFixture(t *testing.T) (*Service, string) {
	t.Helper()
	dsn := sharedPostgresDSN(t)
	control, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { control.Close() })
	limits, _ := query.Normalize(nil)
	runtime := query.New(limits, time.Now, nil)
	runtime.Bind(control, "status-worker", 0)
	return &Service{control: control, shared: &sharedRuntime{workerID: "status-worker"}, queries: runtime, statusSlots: make(chan struct{}, 1)}, dsn
}

func TestSharedInstanceStatusControlPrivacy(t *testing.T) {
	t.Parallel()
	s, dsn := sharedStatusControlFixture(t)
	healthy := sharedStatusRead(t, s)
	if healthy.Status != "ok" || healthy.Checks.SharedControl != dependencyOK || !healthy.Configuration.SharedControl || healthy.Checks.SQLite != dependencyNotConfigured {
		t.Fatalf("healthy shared dependencies were not reported: %+v", healthy)
	}
	raw, err := json.Marshal(healthy)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{dsn, s.shared.workerID, adminToken} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("shared status disclosed a connection or credential")
		}
	}
}

func TestSharedInstanceStatusMandatoryControlFailure(t *testing.T) {
	t.Parallel()
	s, _ := sharedStatusControlFixture(t)
	// Invoke the handler with an already authenticated reader context so the
	// status probe still runs after the control database goes offline.
	if err := s.control.Close(); err != nil {
		t.Fatal(err)
	}
	result := sharedStatusRead(t, s)
	if result.Status != "degraded" || result.Checks.SQLite != dependencyNotConfigured || result.Checks.SharedControl != dependencyUnavailable || !result.Configuration.SharedControl {
		t.Fatalf("shared control failure was not reported: %+v", result)
	}
}

func TestSharedInstanceStatusControlProbeIsBounded(t *testing.T) {
	t.Parallel()
	s, _ := sharedStatusControlFixture(t)
	s.control.SetMaxOpenConns(1)
	conn, err := s.control.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	started := time.Now()
	result := sharedStatusRead(t, s)
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("control probe exceeded its bound: %s", elapsed)
	}
	if result.Status != "degraded" || result.Checks.SharedControl != dependencyUnavailable || result.Checks.SQLite != dependencyNotConfigured {
		t.Fatalf("blocked control probe looked healthy: %+v", result)
	}
}
