package porcupine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
)

// Compare the extra batch meter in the same binary and journal schema. Background
// work is stopped, payloads are prebuilt, and both cases retain all existing
// allowance/receipt/identity work. No analytical engine or network is involved.
func BenchmarkUsageCaptureAcceptance(b *testing.B) {
	for _, size := range []int{1, 100} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("events_%d/meter_%t", size, enabled), func(b *testing.B) {
				if b.N > 1000 {
					b.Skip("use -benchtime=100x; journal benchmark is bounded")
				}
				at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
				s, e := Open(Config{DBPath: filepath.Join(b.TempDir(), "queue.db"), BootstrapToken: adminToken, QueueMaxEvents: 110000, Clock: func() time.Time { return at }})
				if e != nil {
					b.Fatal(e)
				}
				defer s.Close()
				s.stopUsage()
				s.usage.cancel = nil
				if !enabled {
					s.eventRules.usage = nil
				}
				id, token := "meter-project", "meter-ingestion-token"
				if _, e = s.control.Exec(`INSERT INTO projects(id,tenant_id,name,created_at) VALUES(?,?,?,?)`, id, defaultTenantID, "Meter benchmark", at.Format(time.RFC3339)); e != nil {
					b.Fatal(e)
				}
				if _, e = s.control.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES('meter-key','Meter',?,'["events:write"]','["meter-project"]',?)`, hash(token), at.Format(time.RFC3339)); e != nil {
					b.Fatal(e)
				}
				var operator string
				if e = s.control.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&operator); e != nil {
					b.Fatal(e)
				}
				if e = s.ensureProjectLocal(context.Background(), id, auth.Principal{ID: operator}); e != nil {
					b.Fatal(e)
				}
				payloads := make([][]byte, b.N)
				for i := range payloads {
					payloads[i] = architectureLivePayload(i*size, size, 0)
				}
				handler := s.Handler()
				b.ReportAllocs()
				b.ResetTimer()
				for _, payload := range payloads {
					nativeBenchmarkAccept(b, handler, token, payload)
				}
				b.StopTimer()
				var live int64
				if e = s.db.QueryRow(`SELECT COALESCE(SUM(live_events),0) FROM usage_rollups`).Scan(&live); e != nil {
					b.Fatal(e)
				}
				if enabled && live != int64(b.N*size) || !enabled && live != 0 {
					b.Fatal("unexpected meter count", live)
				}
			})
		}
	}
}
