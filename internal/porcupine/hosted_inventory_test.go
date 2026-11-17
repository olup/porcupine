package porcupine

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestHostedJournalInventoryCoversVersionedTables(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "design", "hosted-architecture.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, inventory, ok := strings.Cut(string(data), "<!-- journal-inventory:start -->")
	if !ok {
		t.Fatal("missing hosted journal inventory")
	}
	inventory, _, ok = strings.Cut(inventory, "<!-- journal-inventory:end -->")
	if !ok {
		t.Fatal("unterminated hosted journal inventory")
	}
	tables := map[string]bool{}
	create := regexp.MustCompile(`(?i)\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_][a-z_0-9]*)\s*\(`)
	for _, step := range journalSchema.steps {
		for _, match := range create.FindAllStringSubmatch(step, -1) {
			tables[match[1]] = true
		}
	}
	row := regexp.MustCompile("(?m)^\\| `([a-z_][a-z_0-9]*)` \\| (PostgreSQL|DuckLake|Removed) \\| (Exactly once|At least once|Best effort) \\|")
	seen := map[string]bool{}
	for _, match := range row.FindAllStringSubmatch(inventory, -1) {
		name := match[1]
		if seen[name] || !tables[name] {
			t.Errorf("duplicate or unknown inventory table %s", name)
		}
		seen[name] = true
	}
	for name := range tables {
		if !seen[name] {
			t.Errorf("versioned journal table %s has no hosted disposition", name)
		}
	}
}
