package lake

import (
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/importer"
)

func TestStoredPersonUUIDKeepsLiveNamespaceSeparate(t *testing.T) {
	t.Parallel()
	person := importer.PostHogPersonUUIDPrefix + strings.Repeat("a", 128)
	if !ValidStoredEventUUID(person) || validLiveEventUUID(person) {
		t.Fatal("private person UUID lost or admitted live")
	}
	for _, invalid := range []string{person + "a", person[:len(person)-1], importer.PostHogPersonUUIDPrefix + strings.Repeat("A", 128), importer.PostHogPersonUUIDPrefix + strings.Repeat("z", 128)} {
		if ValidStoredEventUUID(invalid) {
			t.Fatal("invalid person digest accepted", invalid)
		}
	}
	for _, live := range []string{"ordinary", SourceEventUUIDPrefix + strings.Repeat("b", 128)} {
		if !validLiveEventUUID(live) || !ValidStoredEventUUID(live) {
			t.Fatal("live UUID contract changed", live)
		}
	}
	if ValidStoredEventUUID("") || validLiveEventUUID("") {
		t.Fatal("empty UUID accepted")
	}
}
