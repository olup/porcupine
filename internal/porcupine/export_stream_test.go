package porcupine

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestExportJSONInputPreservesValues(t *testing.T) {
	t.Parallel()
	input := exportJSONInput{ctx: context.Background(), reader: bufio.NewReader(strings.NewReader(" {\"n\":18446744073709551615,\"s\":\"a\\\"b\",\"nested\":[null,true]}\n{\"n\":2}")), limit: 4096}
	for _, want := range []string{`{"n":18446744073709551615,"s":"a\"b","nested":[null,true]}`, `{"n":2}`} {
		raw, err := input.value(4096)
		if err != nil || string(raw) != want {
			t.Fatalf("value=%s want=%s err=%v", raw, want, err)
		}
	}
	if _, err := input.peek(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream did not end: %v", err)
	}
}

func TestExportJSONInputRejectsTruncationAndInvalidValues(t *testing.T) {
	t.Parallel()
	valid := `{"n":["value",18446744073709551615]}`
	for n := range len(valid) {
		input := exportJSONInput{ctx: context.Background(), reader: bufio.NewReader(strings.NewReader(valid[:n])), limit: 4096}
		if _, err := input.value(4096); err == nil {
			t.Fatalf("accepted truncation at %d", n)
		}
	}
	for _, raw := range []string{`{"a":}`, `{"a":[1,]}`, string([]byte{'"', 0xff, '"'}), strings.Repeat("[", 257) + strings.Repeat("]", 257)} {
		input := exportJSONInput{ctx: context.Background(), reader: bufio.NewReader(strings.NewReader(raw)), limit: 4096}
		if _, err := input.value(4096); !errors.Is(err, errExportStreamInvalid) {
			t.Fatalf("accepted invalid value: %v", err)
		}
	}
	for _, raw := range []string{`{"a":1,"a":2}`, `[]`, `{"a":1} {"b":2}`} {
		if _, err := exportJSONObject([]byte(raw)); !errors.Is(err, errExportStreamInvalid) {
			t.Fatalf("accepted ambiguous object: %v", err)
		}
	}
}

type exportCountingReader struct {
	reader io.Reader
	bytes  int
}

func (r *exportCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += n
	return n, err
}

func TestExportJSONInputBoundsValueAndCancellation(t *testing.T) {
	t.Parallel()
	reader := &exportCountingReader{reader: strings.NewReader(`{"value":"` + strings.Repeat("x", 1<<20) + `"}`)}
	input := exportJSONInput{ctx: context.Background(), reader: bufio.NewReaderSize(reader, 64<<10), limit: 256}
	if _, err := input.value(256); !errors.Is(err, errExportValueTooLarge) {
		t.Fatalf("oversized value accepted: %v", err)
	}
	if reader.bytes > 256+(64<<10) {
		t.Fatalf("read excessive input: %d", reader.bytes)
	}
	ctx, cancel := context.WithCancel(context.Background())
	input = exportJSONInput{ctx: ctx, reader: bufio.NewReader(strings.NewReader(`{}`)), limit: 256}
	cancel()
	if _, err := input.peek(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
