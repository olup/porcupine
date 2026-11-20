package porcupine

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestProjectQueueLimitsIsolateLiveAdmission(t *testing.T) {
	t.Parallel()
	for _, limit := range []string{"events", "bytes"} {
		t.Run(limit, func(t *testing.T) {
			t.Parallel()
			s, err := Open(Config{
				DBPath: filepath.Join(t.TempDir(), "journal.db"), BootstrapToken: adminToken,
				QueueMaxEvents: 3, MetricsEnabled: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			fixedQueueClock(s)
			idA, ingestA := project(t, s)
			idB, ingestB := project(t, s)
			size := queueFixtureBytes(idA, "one")
			if limit == "events" {
				s.queueProjectMaxEvents = 2
			} else {
				s.queueProjectMaxBytes = 2 * size
			}

			capture := func(key, id string, status int) map[string]any {
				t.Helper()
				return request(t, s, "POST", "/capture/", "", queueBody(key, id), status)
			}
			capture(ingestA, "one", 200)
			batch := fmt.Sprintf(`{"api_key":%q,"batch":[%s,%s,%s]}`,
				ingestA, queueBody(ingestA, "one"), queueBody(ingestA, "two"), queueBody(ingestA, "new"))
			request(t, s, "POST", "/batch/", "", batch, 503)
			if count, bytes := queueUsage(t, s); count != 1 || bytes != size || eventCount(t, s) != 1 {
				t.Fatalf("project pressure left a partial batch: %d events, %d bytes", count, bytes)
			}
			capture(ingestA, "two", 200)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/capture/", strings.NewReader(queueBody(ingestA, "new"))))
			if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
				t.Fatalf("project pressure response: HTTP %d, headers %v", w.Code, w.Header())
			}
			if !strings.Contains(w.Body.String(), "project queue is full") {
				t.Fatalf("expected project pressure: %s", w.Body)
			}
			if retry := capture(ingestA, "one", 200); retry["duplicates"] != float64(1) || retry["accepted"] != float64(0) {
				t.Fatalf("saturated project retry: %v", retry)
			}
			// Identical producer IDs are independent across projects.
			if fresh := capture(ingestB, "one", 200); fresh["accepted"] != float64(1) {
				t.Fatalf("other project admission: %v", fresh)
			}
			capture(ingestB, "two", 503)
			if count, bytes := queueUsage(t, s); count != 3 || bytes != 2*size+queueFixtureBytes(idB, "one") {
				t.Fatalf("instance capacity: %d events, %d bytes", count, bytes)
			}
			requireMetric(t, scrape(t, s), `porcupine_ingestion_project_queue_rejections_total{profile="standalone"} 2`)

			runProjectPurge(t, s, idA, true)
			capture(ingestB, "two", 200)
			if count, _ := queueUsage(t, s); count != 2 {
				t.Fatalf("purge left %d pending events", count)
			}
		})
	}
}

func TestProjectQueueLimitAppliesToPendingEventsAfterRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "journal.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken, QueueMaxEvents: 4})
	if err != nil {
		t.Fatal(err)
	}
	idA, ingestA := project(t, s)
	_, ingestB := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "one"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "two"), 200)
	_, expectedBytes := queueUsage(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(Config{DBPath: path, QueueMaxEvents: 4, QueueProjectMaxEvents: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if count, bytes := queueUsage(t, s); count != 2 || bytes != expectedBytes {
		t.Fatalf("lowering the cap changed previously queued data: %d events, %d bytes", count, bytes)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "three"), 503)
	if retry := request(t, s, "POST", "/capture/", "", captureBody(ingestA, "one"), 200); retry["duplicates"] != float64(1) {
		t.Fatalf("restart did not deduplicate: %v", retry)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingestB, "one"), 200)
	// Cover the legacy delivered transition as well as the production removal.
	if _, err := s.db.Exec(`UPDATE events SET delivered=1 WHERE project_id=? AND uuid='one'`, idA); err != nil {
		t.Fatal(err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "three"), 503)
	if _, err := s.db.Exec(`DELETE FROM events WHERE project_id=? AND uuid='two'`, idA); err != nil {
		t.Fatal(err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "three"), 200)
}

func TestProjectQueueMigrationSeedsExistingPendingPayloads(t *testing.T) {
	t.Parallel()
	db, err := openLocalSQLite(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	v9 := journalSchema
	v9.steps = v9.steps[:9]
	if err := migrateSQLite(t.Context(), db, v9); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tenants VALUES ('t','tenant','now',1,NULL,2,1000,1000,1024);
INSERT INTO projects(id,tenant_id,name,created_at) VALUES ('a','t','A','now'),('b','t','B','now');
INSERT INTO queue_usage VALUES(1,0,0);
INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at,delivered,identity_snapshot,is_imported) VALUES
 ('a','one','event','actor','{"city":"東京"}','t','r',0,'',1),
 ('a','two','event','actor','{}','t','r',0,'{"city":"大阪"}',0),
 ('a','old','event','actor','{}','t','r',1,'',0),
 ('b','one','event','actor','{}','t','r',0,'',0);`); err != nil {
		t.Fatal(err)
	}
	check := func(id string, expected int64) {
		t.Helper()
		var count, bytes int64
		err := db.QueryRow(`SELECT pending_count,pending_bytes FROM project_queue_usage WHERE project_id=?`, id).Scan(&count, &bytes)
		if err != nil {
			t.Fatal(err)
		}
		var actualBytes int64
		if err := db.QueryRow(`SELECT COALESCE(sum(`+queueByteExpression("")+`),0) FROM events WHERE delivered=0 AND project_id=?`, id).Scan(&actualBytes); err != nil {
			t.Fatal(err)
		}
		if count != expected || bytes != actualBytes {
			t.Fatalf("project %q has %d events and %d bytes, want %d and %d", id, count, bytes, expected, actualBytes)
		}
	}
	for range 2 {
		if err := migrateSQLite(t.Context(), db, journalSchema); err != nil {
			t.Fatal(err)
		}
		check("a", 2)
		check("b", 1)
	}
	if _, err := db.Exec(`UPDATE events SET identity_snapshot='{"city":"神戸"}' WHERE project_id='a' AND uuid='one'`); err != nil {
		t.Fatal(err)
	}
	check("a", 2)
}
