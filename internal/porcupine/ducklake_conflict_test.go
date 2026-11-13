//go:build ducklake

package porcupine

import (
	"context"
	"runtime"
	"strings"
	"testing"
)

// Observe the pinned engine's own transaction behavior without Porcupine's
// application write mutex. A conflict may abort a transaction, never half-apply it.
func TestDuckLakeNativeWriteConflictSemantics(t *testing.T) {
	store := openLakeStorageTest(t, t.TempDir(), 1)
	p, release, err := store.leaseProject(context.Background(), "native-conflicts")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var version string
	if err := p.db.QueryRow("SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, statement string
		delta           int
		updated         bool
		insertValue     int
	}{
		{"delete_matched", "DELETE FROM lake.conflict_probe WHERE id=2", -1, false, 0},
		{"delete_unmatched", "CREATE OR REPLACE TEMP TABLE _pc_native_incoming AS SELECT 101 AS id; DELETE FROM lake.conflict_probe WHERE id IN (SELECT id FROM _pc_native_incoming)", 0, false, 0},
		{"insert_only", "INSERT INTO lake.conflict_probe VALUES(101,0)", 1, false, 0},
		{"merge_unmatched", "MERGE INTO lake.conflict_probe t USING (SELECT 101 AS id,1 AS value) s ON t.id=s.id WHEN MATCHED THEN UPDATE SET value=s.value WHEN NOT MATCHED THEN INSERT(id,value) VALUES(s.id,s.value)", 1, false, 1},
		{"merge_matched", "MERGE INTO lake.conflict_probe t USING (SELECT 2 AS id,1 AS value) s ON t.id=s.id WHEN MATCHED THEN UPDATE SET value=s.value WHEN NOT MATCHED THEN INSERT(id,value) VALUES(s.id,s.value)", 0, true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := p.db.Exec("DROP TABLE IF EXISTS lake.conflict_probe; CREATE TABLE lake.conflict_probe(id BIGINT,value BIGINT); INSERT INTO lake.conflict_probe SELECT (i+1)*2,0 FROM range(100) r(i)"); err != nil {
				t.Fatal(err)
			}
			tx, err := p.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.Exec(c.statement); err != nil {
				t.Fatal(err)
			}
			if _, err := p.db.Exec("INSERT INTO lake.conflict_probe VALUES(999,2)"); err != nil {
				t.Fatal(err)
			}
			commitErr := tx.Commit()
			if commitErr != nil && !strings.Contains(strings.ToLower(commitErr.Error()), "conflict") {
				t.Fatal(commitErr)
			}
			expected := map[int]int{999: 2}
			for i := 1; i <= 100; i++ {
				expected[i*2] = 0
			}
			if commitErr == nil {
				if c.delta < 0 {
					delete(expected, 2)
				}
				if c.delta > 0 {
					expected[101] = c.insertValue
				}
				if c.updated {
					expected[2] = 1
				}
			}
			result, err := p.db.Query("SELECT id,value FROM lake.conflict_probe")
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()
			rows := 0
			for result.Next() {
				var id, value int
				if err := result.Scan(&id, &value); err != nil {
					t.Fatal(err)
				}
				if want, ok := expected[id]; !ok || want != value {
					t.Fatalf("unexpected row after transaction: id=%d value=%d", id, value)
				}
				delete(expected, id)
				rows++
			}
			if err := result.Err(); err != nil {
				t.Fatal(err)
			}
			if len(expected) != 0 {
				t.Fatalf("missing rows after transaction: %v", expected)
			}
			architectureEmit("native_conflict", "ducklake", map[string]any{"operation": c.name, "committed": commitErr == nil, "rows": rows, "native_version": version, "platform": runtime.GOOS + "/" + runtime.GOARCH, "verdict": "VERIFIED"})
		})
	}
}
