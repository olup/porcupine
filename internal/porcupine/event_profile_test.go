package porcupine

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func profileTestRow(id, uuid, raw string, ms int64) eventProfileRow {
	return eventProfileRow{ID: id, UUID: uuid, Timestamp: time.UnixMilli(ms).UTC().Format(time.RFC3339Nano), TimestampMS: ms, ReceivedAt: "2026-11-13T12:00:00Z", Properties: raw, PropertyBytes: len(raw), Context: "{}", ContextBytes: 2}
}

func profileTestRead(rows ...eventProfileRow) eventProfileRead {
	return eventProfileRead{activity: EventProfileActivity{Total: "7", Last24Hours: "2", Last7Days: "3", FutureEvents: "2"}, rows: rows}
}

func TestEventProfileSampleSignalsAndNumericPrecision(t *testing.T) {
	options := eventProfileOptions{event: "page", sampleLimit: 1000, exampleLimit: 3, propertyLimit: 100, propertyKind: "custom", includeValues: true}
	read := profileTestRead(
		profileTestRow("3", "latest", `{"a":null,"b":"fr","n":9007199254740993,"$sdk":false}`, 3000),
		profileTestRow("2", "middle", `{"a":1,"b":"fr","n":9007199254740992}`, 2000),
		profileTestRow("1", "oldest", `{"b":"en","n":9007199254740993}`, 1000),
	)
	result, err := summarizeEventProfile(read, options, time.UnixMilli(4000))
	if err != nil {
		t.Fatal(err)
	}
	if result.Sampling.ProfiledEvents != 3 || result.Sampling.SampledEvents != 3 || result.Sampling.HistoricalPropertiesComplete {
		t.Fatalf("bad sample scope: %+v", result.Sampling)
	}
	a := result.Properties[0]
	if a.Key != "a" || a.Present != 2 || a.Missing != 1 || a.Null != 1 || a.DistinctValues != 1 || !reflect.DeepEqual(a.Types, []string{"Int64", "Null"}) || a.LatestEventUUID != "latest" {
		t.Fatalf("bad null/absence distinction: %+v", a)
	}
	b := result.Properties[1]
	if b.Present != 3 || b.DistinctValues != 2 || len(b.TopValues) != 2 || string(b.TopValues[0].Value) != `"fr"` || b.TopValues[0].Count != 2 {
		t.Fatalf("bad frequent values: %+v", b)
	}
	n := result.Properties[2]
	if n.DistinctValues != 2 || string(n.TopValues[0].Value) != "9007199254740993" || n.TopValues[0].Count != 2 {
		t.Fatalf("lost numeric precision: %+v", n)
	}
	if len(result.Examples) != 3 || string(result.Examples[0].Properties) != read.rows[0].Properties {
		t.Fatalf("missing actual examples: %+v", result.Examples)
	}
	options.includeValues = false
	result, err = summarizeEventProfile(read, options, time.UnixMilli(4000))
	if err != nil {
		t.Fatal(err)
	}
	for _, property := range result.Properties {
		if len(property.TopValues) != 0 || property.ValuesOmitted {
			t.Fatalf("values disclosed without opt-in: %+v", property)
		}
	}
}

func TestEventProfilePayloadExclusionsDoNotBecomeMissingValues(t *testing.T) {
	options := eventProfileOptions{event: "page", sampleLimit: 10, exampleLimit: 3, propertyLimit: 1, propertyKind: "all"}
	oversized := profileTestRow("3", "oversized", "", 3000)
	oversized.PropertyBytes = eventProfileFieldBytes + 1
	budgeted := profileTestRow("2", "budgeted", "", 2000)
	budgeted.PropertyBytes = 100
	complete := profileTestRow("1", "complete", `{"z":false,"a":"last"}`, 1000)
	result, err := summarizeEventProfile(profileTestRead(oversized, budgeted, complete), options, time.UnixMilli(4000))
	if err != nil {
		t.Fatal(err)
	}
	if result.Sampling.ProfiledEvents != 1 || result.Sampling.OversizedEvents != 1 || result.Sampling.ByteBudgetExcludedEvents != 1 || !result.Sampling.PropertiesTruncated {
		t.Fatalf("wrong exclusion accounting: %+v", result.Sampling)
	}
	if len(result.Properties) != 1 || result.Properties[0].Key != "a" || result.Properties[0].Present != 1 || result.Properties[0].Missing != 0 {
		t.Fatalf("omitted payloads counted as absent properties: %+v", result.Properties)
	}
	for _, example := range result.Examples[:2] {
		if example.Properties != nil || !reflect.DeepEqual(example.TruncatedFields, []string{"properties"}) {
			t.Fatalf("silent example truncation: %+v", example)
		}
	}
}

func TestEventProfileValueLimitsKeepDistinctLongValues(t *testing.T) {
	one, _ := json.Marshal(strings.Repeat("x", eventProfileValueBytes) + "one")
	two, _ := json.Marshal(strings.Repeat("x", eventProfileValueBytes) + "two")
	options := eventProfileOptions{event: "page", sampleLimit: 10, exampleLimit: 0, propertyLimit: 100, propertyKind: "all", includeValues: true}
	read := profileTestRead(profileTestRow("2", "one", `{"long":`+string(one)+`,"object":{"a":1}}`, 2000), profileTestRow("1", "two", `{"long":`+string(two)+`,"object":{"a":2}}`, 1000))
	result, err := summarizeEventProfile(read, options, time.UnixMilli(3000))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Examples) != 0 {
		t.Fatal("zero example limit ignored")
	}
	for _, property := range result.Properties {
		if property.DistinctValues != 2 || !property.ValuesOmitted || len(property.TopValues) != 0 {
			t.Fatalf("long or complex values disclosed or collapsed: %+v", property)
		}
	}
}

func TestEventProfileNormalizationAndMalformedStorage(t *testing.T) {
	for _, input := range []EventProfileInput{{Event: ""}, {Event: strings.Repeat("é", 251)}, {Event: "ok", SampleLimit: 1001}, {Event: "ok", ExampleLimit: -1}, {Event: "ok", ExampleLimit: 6}, {Event: "ok", PropertyLimit: 101}, {Event: "ok", PropertyPrefix: strings.Repeat("é", 101)}, {Event: "ok", PropertyKind: "unknown"}} {
		if _, err := normalizeEventProfileInput(input); queryStatus(err) != 400 {
			t.Fatalf("accepted invalid options: %+v %v", input, err)
		}
	}
	options, err := normalizeEventProfileInput(EventProfileInput{Event: "literal' ; DROP TABLE events --", ExampleLimit: 0})
	if err != nil || options.exampleLimit != 0 || options.sampleLimit != 1000 || options.propertyLimit != 20 {
		t.Fatalf("bad normalized defaults or event altered: %+v %v", options, err)
	}
	for _, raw := range []string{`null`, `[]`, `invalid`} {
		row := profileTestRow("1", "stored", raw, 1000)
		if _, err := summarizeEventProfile(profileTestRead(row), options, time.UnixMilli(3000)); queryStatus(err) != 503 {
			t.Fatalf("bad stored properties accepted: %q %v", raw, err)
		}
	}
	row := profileTestRow("1", "stored", `{}`, 1000)
	row.TimestampMS = 1001
	if _, err := summarizeEventProfile(profileTestRead(row), options, time.UnixMilli(3000)); queryStatus(err) != 503 {
		t.Fatalf("bad stored occurrence accepted: %v", err)
	}
}
