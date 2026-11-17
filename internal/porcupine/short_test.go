package porcupine

import "testing"

// skipInShort keeps `make check` fast. `make check-full` and CI run every test.
func skipInShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("slow journey; run make check-full")
	}
}
