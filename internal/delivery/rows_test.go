package delivery

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"testing"
)

func TestRowsShareCSVAndJSONLValueSemantics(t *testing.T) {
	t.Parallel()
	names := []string{"note<>&", "large", "nothing"}
	values := []json.RawMessage{json.RawMessage(`"<hello>,\n\"world\""`), json.RawMessage(`18446744073709551615`), json.RawMessage(`null`)}
	jsonl, err := NewRowEncoder("jsonl", names)
	if err != nil {
		t.Fatal(err)
	}
	row, err := jsonl.Encode(values)
	if err != nil {
		t.Fatal(err)
	}
	if want := "{\"note<>&\":\"<hello>,\\n\\\"world\\\"\",\"large\":18446744073709551615,\"nothing\":null}\n"; string(row) != want {
		t.Fatalf("JSONL changed values: %s", row)
	}
	if len(jsonl.Header()) != 0 {
		t.Fatal("unexpected JSONL header")
	}
	csvEncoder, err := NewRowEncoder("csv", names)
	if err != nil {
		t.Fatal(err)
	}
	row, err = csvEncoder.Encode(values)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(bytes.NewReader(append(append([]byte{}, csvEncoder.Header()...), row...))).ReadAll()
	if err != nil || len(records) != 2 || records[0][0] != names[0] || records[1][0] != "<hello>,\n\"world\"" || records[1][1] != "18446744073709551615" || records[1][2] != "" {
		t.Fatalf("CSV changed values: %#v %v", records, err)
	}
}

func TestRowsRejectAmbiguousOrWrongShape(t *testing.T) {
	t.Parallel()
	if _, err := NewRowEncoder("xml", nil); !errors.Is(err, ErrEncoding) {
		t.Fatal("unknown format accepted")
	}
	if _, err := NewRowEncoder("jsonl", []string{"same", "same"}); !errors.Is(err, ErrEncoding) {
		t.Fatal("duplicate JSONL names accepted")
	}
	encoder, err := NewRowEncoder("csv", []string{"same", "same"})
	if err != nil {
		t.Fatal("CSV duplicate names changed", err)
	}
	if _, err := encoder.Encode([]json.RawMessage{json.RawMessage(`1`)}); !errors.Is(err, ErrEncoding) {
		t.Fatal("row width mismatch accepted")
	}
	var uninitialized *RowEncoder
	if _, err := uninitialized.Encode(nil); !errors.Is(err, ErrEncoding) {
		t.Fatal("uninitialized encoder accepted")
	}
}
