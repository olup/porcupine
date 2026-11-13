package porcupine

import (
	"strings"
	"testing"
)

func TestCatalogPropertyOptionValidation(t *testing.T) {
	for _, input := range []EventCatalogInput{
		{Event: "page", PropertyLimit: 101}, {Event: "page", PropertyLimit: -1},
		{Event: "page", PropertyKind: "unknown"}, {PropertyKind: "custom"},
		{PropertyPrefix: "thread"}, {PropertyCursor: "anything"},
		{Event: "page", PropertyPrefix: strings.Repeat("p", 201)},
		{Event: "page", PropertyCursor: strings.Repeat("x", 8193)},
		{Event: "page", PropertyCursor: "not-a-cursor"},
	} {
		if _, err := normalizeCatalogInput(input); queryStatus(err) != 400 {
			t.Fatalf("invalid property options accepted: %+v %v", input, err)
		}
	}
}

func TestCatalogPropertyCursorRoundTripEscapedNamesAndFilters(t *testing.T) {
	input, err := normalizeCatalogInput(EventCatalogInput{ID: "a", Event: strings.Repeat("<", 500), PropertyPrefix: strings.Repeat("<", 199), PropertyLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	key := input.PropertyPrefix + "a"
	result := EventCatalog{Properties: []EventCatalogProperty{{Key: key}, {Key: input.PropertyPrefix + "b"}}}
	finishCatalogProperties(&result, input)
	input.PropertyCursor = result.NextPropertyCursor
	decoded, err := normalizeCatalogInput(input)
	if err != nil || decoded.propertyAfter == nil || *decoded.propertyAfter != key {
		t.Fatalf("maximum escaped cursor did not round trip: %v", err)
	}
	for _, field := range []string{"project", "event", "prefix", "kind"} {
		changed := input
		switch field {
		case "project":
			changed.ID = "b"
		case "event":
			changed.Event = "other"
		case "prefix":
			changed.PropertyPrefix = "other"
		case "kind":
			changed.PropertyKind = "custom"
		}
		if _, err := normalizeCatalogInput(changed); queryStatus(err) != 400 {
			t.Fatalf("cursor filter binding ignored: %s", field)
		}
	}
}
