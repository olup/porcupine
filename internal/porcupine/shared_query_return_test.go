package porcupine

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

type queryReturnHook struct {
	query.HostedReader
	after func(context.Context) error
}

func (s *queryReturnHook) Query(ctx context.Context, project, sql string, parameters map[string]string, limits lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error) {
	result, err := s.HostedReader.Query(ctx, project, sql, parameters, limits)
	return result, errors.Join(err, s.after(ctx))
}

func (s *queryReturnHook) Catalog(ctx context.Context, input lakeapi.EventCatalogInput) (lakeapi.EventCatalog, error) {
	result, err := s.HostedReader.Catalog(ctx, input)
	return result, errors.Join(err, s.after(ctx))
}

func (s *queryReturnHook) Profile(ctx context.Context, project string, options lakeapi.EventProfileOptions, asOf time.Time) (lakeapi.EventProfileRead, error) {
	result, err := s.HostedReader.Profile(ctx, project, options, asOf)
	return result, errors.Join(err, s.after(ctx))
}

func TestSharedQueryNativeReturnPrecedesResponseProcessing(t *testing.T) {
	t.Parallel()
	for _, operation := range []struct{ name, method, path, body string }{
		{"query", "POST", "/query", `{"sql":"SELECT count(*) FROM events"}`},
		{"catalog", "GET", "/catalog", ""},
		{"profile", "GET", "/event-profile?event=missing", ""},
	} {
		t.Run(operation.name, func(t *testing.T) {
			t.Parallel()
			a, b := sharedNativeJourneyPair(t, nativeS3Fixture(t))
			project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Native return")
			hostedCleanupWrite(t, a.service, project, "described")
			locker, err := b.service.control.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer locker.Rollback()
			returned := make(chan struct{})
			hook := &queryReturnHook{HostedReader: a.service.hostedReader}
			hook.after = func(ctx context.Context) error {
				defer close(returned)
				var active bool
				if err := a.service.control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM shared_query_permits WHERE project_id=$1)`, project).Scan(&active); err != nil {
					return err
				}
				if !active {
					return errors.New("query lacked a permit while in native code")
				}
				if operation.name == "catalog" {
					// The handler loads descriptions after the native permit is released.
					_, err := locker.ExecContext(ctx, `LOCK TABLE catalog_descriptions IN ACCESS EXCLUSIVE MODE`)
					return err
				}
				return nil
			}
			a.service.hostedReader = hook
			request := func(ctx context.Context) (*httptest.ResponseRecorder, <-chan struct{}) {
				r := httptest.NewRequest(operation.method, "/api/v1/projects/"+project+operation.path, strings.NewReader(operation.body)).WithContext(ctx)
				r.Header.Set("Authorization", "Bearer "+adminToken)
				r.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				done := make(chan struct{})
				go func() {
					defer close(done)
					a.service.Handler().ServeHTTP(response, r)
				}()
				return response, done
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			response, done := request(ctx)
			select {
			case <-returned:
			case <-ctx.Done():
				t.Fatal("native call did not return")
			}
			if operation.name == "catalog" {
				// Confirm the production handler reached the blocked catalog description lookup.
				for {
					var waiting bool
					if err := b.service.control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='catalog_descriptions'::regclass AND NOT granted)`).Scan(&waiting); err != nil {
						t.Fatal(err)
					}
					if waiting {
						break
					}
					select {
					case <-done:
						t.Fatal("handler skipped catalog descriptions", response.Code, response.Body.String())
					case <-ctx.Done():
						t.Fatal("catalog description lookup did not block")
					case <-time.After(time.Millisecond):
					}
				}
			} else {
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("native call returned without a response")
				}
			}
			if state := a.service.queries.Status(); state.Active != 0 || state.Unavailable() || a.service.queries.UsedSlots() != 0 {
				t.Fatal("response processing retained native execution state", state)
			}
			var permits int
			if err := b.service.control.QueryRowContext(ctx, `SELECT count(*) FROM shared_query_permits WHERE project_id=$1`, project).Scan(&permits); err != nil || permits != 0 {
				t.Fatal("finished native work retained PostgreSQL capacity", permits, err)
			}
			sharedJourneyHTTP(t, a.host.URL, "GET", "/readyz", "", nil, nil, 200)
			if err := locker.Rollback(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("response did not resume with the original request deadline")
			}
			if response.Code != 200 {
				t.Fatal("native completion canceled response processing", response.Code, response.Body.String())
			}

			// A driver may return a nil error even after its execution was canceled.
			hook.after = func(ctx context.Context) error {
				if _, err := a.service.control.ExecContext(ctx, `DELETE FROM shared_query_permits WHERE project_id=$1`, project); err != nil {
					return err
				}
				// Simulate native I/O returning a successful result after
				// the keeper has detected loss of its permit.
				wait, stop := context.WithTimeout(t.Context(), 8*time.Second)
				defer stop()
				return waitQueryStatus(wait, a.service.queries, func(status query.QueryExecutionStatus) bool { return status.UncertainPermits == 1 })
			}
			wait, stop := context.WithTimeout(t.Context(), 10*time.Second)
			defer stop()
			canceledResponse, canceledDone := request(wait)
			select {
			case <-canceledDone:
			case <-wait.Done():
				t.Fatal("canceled native result did not finish")
			}
			if canceledResponse.Code != 503 || !strings.Contains(canceledResponse.Body.String(), `"code":"query_permit_uncertain"`) {
				t.Fatalf("nil driver result bypassed lease failure: %d %s", canceledResponse.Code, canceledResponse.Body.String())
			}
		})
	}
}
