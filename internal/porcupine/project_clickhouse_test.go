package porcupine

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestDeleteProjectWithDisconnectedClickHouse(t *testing.T) {
	s := service(t, 5)
	if err := s.deleteProjectClickHouse(t.Context(), "project"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("INSERT INTO metadata(key,value) VALUES ('query_master_key','previously-configured')"); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteProjectClickHouse(t.Context(), "project"); err == nil {
		t.Fatal("accepted deletion with disconnected ClickHouse")
	}
}

func TestDeleteProjectClickHouseLostAcknowledgment(t *testing.T) {
	s := service(t, 5)
	rows, active, submits := true, false, 0
	pid := "a'; DELETE WHERE 1; --"
	predicate := "project_id=unhex('" + hex.EncodeToString([]byte(pid)) + "')"
	ch := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		stmt := string(b)
		if strings.Contains(stmt, pid) {
			t.Error("raw project ID in ClickHouse statement")
		}
		switch {
		case strings.HasPrefix(stmt, "SELECT 1 FROM analytics."+eventTable):
			if !strings.Contains(stmt, predicate) || !strings.Contains(stmt, "final=0") {
				t.Error("raw project rows not checked")
			}
			if rows {
				io.WriteString(w, "1\n")
			}
		case strings.HasPrefix(stmt, "SELECT 1 FROM system.mutations"):
			if active {
				io.WriteString(w, "1\n")
			}
		case strings.HasPrefix(stmt, "ALTER TABLE"):
			if !strings.Contains(stmt, predicate) || !strings.HasSuffix(stmt, "mutations_sync=1") {
				t.Error("incorrect deletion scope or completion semantics")
			}
			submits++
			active = true
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	configureAnalyticsTest(s, ch)
	if err := s.deleteProjectClickHouse(t.Context(), pid); err == nil {
		t.Fatal("lost acknowledgment claimed success")
	}
	if err := s.deleteProjectClickHouse(t.Context(), pid); err == nil {
		t.Fatal("unfinished mutation claimed success")
	}
	rows, active = false, false
	if err := s.deleteProjectClickHouse(t.Context(), pid); err != nil || submits != 1 {
		t.Fatalf("retry failed or duplicated mutation: %v, submits=%d", err, submits)
	}
}

func TestClickHouseIntegrationDeleteProject(t *testing.T) {
	s := service(t, 10)
	ch, err := newClickHouse(integrationClickHouseConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, ch)
	ctx := context.Background()
	a, ak := project(t, s)
	b, bk := project(t, s)
	for _, key := range []string{ak, bk} {
		request(t, s, "POST", "/capture/", "", captureBody(key, "same-uuid"), 200)
	}
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	ns, acct, pwd, err := s.analyticsReader(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	nsB, acctB, pwdB, err := s.analyticsReader(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"_export", "_pipeline_export"} {
		if _, _, _, err = s.configureExportReader(ctx, a, contextTestExportConfig(), suffix); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		request(t, s, "DELETE", "/api/v1/projects/"+a, adminToken, "", 204)
	}
	if s.queryReady[a] || !s.queryReady[b] {
		t.Fatal("wrong project provisioning cache invalidated")
	}
	for _, stmt := range []string{"SELECT count() FROM system.users WHERE startsWith(name,'" + ns + "_')",
		"SELECT count() FROM system.row_policies WHERE startsWith(short_name,'" + ns + "_')",
		"SELECT count() FROM system.databases WHERE name='" + ns + "'",
		"SELECT count() FROM " + ch.config.Database + "." + eventTable + " WHERE project_id='" + a + "'"} {
		data, err := ch.admin(ctx, stmt+" FORMAT TabSeparated")
		if err != nil || string(data) != "0\n" {
			t.Fatalf("cleanup left a resource: %s got %q err %v", stmt, data, err)
		}
	}
	if _, err := ch.query(ctx, acct, pwd, ns, "SELECT count() AS total FROM events", nil); err == nil {
		t.Fatal("deleted reader credentials still work")
	}
	data, err := ch.query(ctx, acctB, pwdB, nsB, "SELECT count() AS total FROM events", nil)
	if err != nil || !strings.Contains(string(data), `"total": "1"`) {
		t.Fatalf("other project's data or reader changed: %s, %v", data, err)
	}
}
