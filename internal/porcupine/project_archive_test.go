package porcupine

import "testing"

func TestProjectArchiveCleanupRefusesMissingBoundStore(t *testing.T) {
	s := service(t, 5)
	if err := s.deleteProjectArchive(t.Context(), "queue-only"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO metadata(key,value) VALUES('ducklake_source_id','original-native-source')`); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteProjectArchive(t.Context(), "requires-original-archive"); err == nil {
		t.Fatal("missing native archive was treated as deleted")
	}
}
