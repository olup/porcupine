package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

const exportTestMeta = `"meta":[{"name":"n","type":"UInt64"}],"data":`

func TestExportStreamCompleteAndExtras(t *testing.T) {
	input := `{` + exportTestMeta + `[["18446744073709551615"],["2"]],"totals":["18446744073709551615"],"extremes":{"min":["2"],"max":["18446744073709551615"]},"rows":2,"rows_before_limit_at_least":12,"rows_before_aggregation":14,"statistics":{"elapsed":0.5,"rows_read":99,"bytes_read":999}}`
	var rows []string
	columnsCalled := false
	result, err := streamExportJSON(context.Background(), strings.NewReader(input), 4096, exportStreamCallbacks{
		Columns: func(columns []AnalyticsResultColumn) error {
			columnsCalled = true
			if len(columns) != 1 || columns[0].Name != "n" {
				t.Fatal("incorrect schema")
			}
			return nil
		},
		Row: func(row []json.RawMessage) error {
			if !columnsCalled {
				t.Fatal("rows arrived before schema")
			}
			rows = append(rows, string(row[0]))
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 2 || result.Statistics.Elapsed != 0.5 || len(rows) != 2 || rows[0] != `"18446744073709551615"` {
		t.Fatalf("invalid streamed values: %#v %#v", result, rows)
	}
	if string(result.Totals) != `["18446744073709551615"]` || string(result.Extremes) != `{"min":["2"],"max":["18446744073709551615"]}` || result.RowsBeforeLimitAtLeast == nil || *result.RowsBeforeLimitAtLeast != 12 {
		t.Fatal("result extras were discarded or overwritten")
	}
}

func TestExportStreamRejectsIncompleteOrAmbiguousResults(t *testing.T) {
	valid := `{` + exportTestMeta + `[["1"]],"rows":1}`
	cases := []string{
		``, `n\n1\nCode: 241. secret SQL`,
		`{` + exportTestMeta + `[["1"]]`,
		`{` + exportTestMeta + `[["1"]],"rows":1`,
		`{` + exportTestMeta + `[["1"]],"rows":2}`,
		`{` + exportTestMeta + `[["1"]],"rows":1,"exception":"secret SQL"}`,
		valid + `Code: 241. secret SQL`, valid + `{}`,
		`{` + exportTestMeta + `[["1"]],"rows":1,"rows":1}`,
		`{` + exportTestMeta + `[["1"]],"rows":1,"statistics":{},"statistics":{}}`,
		`{` + exportTestMeta + `[["1"]],"rows":1,"secret":"raw SQL"}`,
		`{` + exportTestMeta + `[["1"],],"rows":1}`,
		`{` + exportTestMeta + `[["1"]],"rows":1,}`,
		`{"data":[["1"]],"meta":[{"name":"n","type":"UInt64"}],"rows":1}`,
		`{"meta":[],"data":[],"rows":null}`,
		`{"meta":[],"data":[],"rows":0,"statistics":{"elapsed":null}}`,
		`{` + exportTestMeta + `[["1","2"]],"rows":1}`,
		`{` + exportTestMeta + `[null],"rows":1}`,
		`{"meta":[{"name":"n","type":"String"},{"name":"n","type":"String"}],"data":[],"rows":0}`,
		`{"meta":[{"name":"","type":"String"}],"data":[],"rows":0}`,
		`{"meta":[{"name":"n","name":"m","type":"String"}],"data":[],"rows":0}`,
		`{` + exportTestMeta + `[],"rows":0,"totals":[]}`,
		`{` + exportTestMeta + `[],"rows":0,"extremes":{"min":["1"]}}`,
		`{` + exportTestMeta + `[],"rows":0,"statistics":{"elapsed":0,"unexpected":1}}`,
	}
	for index, input := range cases {
		_, err := streamExportJSON(context.Background(), strings.NewReader(input), 4096, exportStreamCallbacks{})
		if err == nil {
			t.Errorf("case %d accepted invalid stream", index)
		} else if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "SQL") {
			t.Fatal("source error leaked")
		}
	}
	for index := 0; index < len(valid); index++ {
		if _, err := streamExportJSON(context.Background(), strings.NewReader(valid[:index]), 4096, exportStreamCallbacks{}); err == nil {
			t.Fatalf("accepted truncation at byte %d", index)
		}
	}
	if _, err := streamExportJSON(context.Background(), io.MultiReader(strings.NewReader(valid), exportErrorReader{}), 4096, exportStreamCallbacks{}); err == nil {
		t.Fatal("unexpected transport EOF after complete JSON treated as success")
	}
}

type exportErrorReader struct{}

func (exportErrorReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type exportCountingReader struct {
	reader io.Reader
	bytes  int
}

func (r *exportCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += n
	return n, err
}

func TestExportStreamBoundsEveryValue(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	for _, input := range []string{
		`{` + exportTestMeta + `[["` + big + `"]],"rows":1}`,
		`{"meta":[{"name":"` + big + `","type":"String"}],"data":[],"rows":0}`,
		`{"` + big + `":1}`,
		`{` + exportTestMeta + `[],"rows":0,"statistics":{"` + big + `":0}}`,
		`{` + exportTestMeta + `[],"rows":0,"totals":["` + big + `"]}`,
		`{` + exportTestMeta + `[],"rows":0,"extremes":{"min":["` + big + `"],"max":["x"]}}`,
	} {
		reader := &exportCountingReader{reader: strings.NewReader(input)}
		_, err := streamExportJSON(context.Background(), reader, 256, exportStreamCallbacks{})
		if !errors.Is(err, errExportValueTooLarge) && !errors.Is(err, errExportMetadataTooLarge) {
			t.Fatalf("oversized value not rejected: %v", err)
		}
		if reader.bytes > 256+(64<<10) {
			t.Fatalf("oversized value consumed excessive input: %d", reader.bytes)
		}
	}
	if _, err := streamExportJSON(context.Background(), strings.NewReader(`{}`), 0, exportStreamCallbacks{}); !errors.Is(err, errExportValueTooLarge) {
		t.Fatal("invalid bound accepted")
	}
}

func TestExportStreamPropagatesCallbacksAndCancellation(t *testing.T) {
	input := `{` + exportTestMeta + `[["1"]],"rows":1}`
	callbackErr := errors.New("sink failed")
	for _, callbacks := range []exportStreamCallbacks{
		{Columns: func([]AnalyticsResultColumn) error { return callbackErr }},
		{Row: func([]json.RawMessage) error { return callbackErr }},
	} {
		if _, err := streamExportJSON(context.Background(), strings.NewReader(input), 4096, callbacks); !errors.Is(err, callbackErr) {
			t.Fatal("callback failure lost")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, err := streamExportJSON(ctx, strings.NewReader(input), 4096, exportStreamCallbacks{Columns: func([]AnalyticsResultColumn) error { cancel(); return nil }})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("canceled stream continued")
	}
}

type exportSmallRows struct {
	remaining int
	first     bool
	current   string
}

func (r *exportSmallRows) Read(p []byte) (int, error) {
	if len(r.current) == 0 {
		if r.remaining == 0 {
			return 0, io.EOF
		}
		r.current = `["18446744073709551615"]`
		if !r.first {
			r.current = "," + r.current
		}
		r.first = false
		r.remaining--
	}
	n := copy(p, r.current)
	r.current = r.current[n:]
	return n, nil
}

func TestExportStreamMillionSmallRows(t *testing.T) {
	const count = 1000000
	src := io.MultiReader(strings.NewReader(`{`+exportTestMeta+`[`), &exportSmallRows{remaining: count, first: true}, strings.NewReader(`],"rows":1000000,"statistics":{}}`))
	var rows int64
	result, err := streamExportJSON(context.Background(), src, 256, exportStreamCallbacks{Row: func(row []json.RawMessage) error {
		rows++
		if string(row[0]) != `"18446744073709551615"` {
			t.Fatal("value precision lost")
		}
		return nil
	}})
	if err != nil || result.Rows != count || rows != count {
		t.Fatalf("large streaming result failed: rows=%d result=%d err=%v", rows, result.Rows, err)
	}
}

func TestExportStreamMetadataHasSeparateSmallBudget(t *testing.T) {
	large := strings.Repeat("x", 300<<10)
	input := `{"meta":[{"name":"n","type":"String"}],"data":[["` + large + `"]],"rows":1}`
	if result, err := streamExportJSON(context.Background(), strings.NewReader(input), 1<<20, exportStreamCallbacks{}); err != nil || result.Rows != 1 {
		t.Fatal("large allowed row was constrained by metadata limit", err)
	}
	for _, input := range []string{
		`{"meta":[{"name":"` + large + `","type":"String"}],"data":[],"rows":0}`,
		`{"meta":[{"name":"n","type":"String"}],"data":[],"rows":0,"totals":["` + large + `"]}`,
		`{"meta":[{"name":"` + strings.Repeat("<", 64<<10) + `","type":"String"}],"data":[],"rows":0}`,
	} {
		if _, err := streamExportJSON(context.Background(), strings.NewReader(input), 1<<20, exportStreamCallbacks{}); !errors.Is(err, errExportMetadataTooLarge) {
			t.Fatal("oversized serialized metadata accepted", err)
		}
	}
}
