package delivery

import (
	"bytes"
	"testing"
)

func TestWebhookBatchEncodingBoundaries(t *testing.T) {
	t.Parallel()
	prefix, err := WebhookBatchPrefix("id", "pipeline", "project", 0, 60000)
	if err != nil {
		t.Fatal(err)
	}
	first := int64(WebhookChunkBytes - len(prefix) - 2 - 1 - 2)
	for _, test := range []struct {
		name    string
		lengths []int64
		want    int
		max     int64
		fail    bool
	}{
		{"exact byte boundary", []int64{first, 2}, 2, 2 << 20, false},
		{"over byte boundary", []int64{first, 3}, 1, 2 << 20, false},
		{"oversized singleton", []int64{WebhookChunkBytes, 2}, 1, 2 << 20, false},
		{"singleton hard limit", []int64{2 << 20}, 0, 2 << 20, true},
		{"negative length", []int64{-1}, 0, 2 << 20, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := WebhookBatchCount(len(prefix), test.lengths, test.max)
			if (err != nil) != test.fail || got != test.want {
				t.Fatal(got, err)
			}
		})
	}
	lengths := make([]int64, 101)
	for i := range lengths {
		lengths[i] = 2
	}
	if n, err := WebhookBatchCount(len(prefix), lengths, 2<<20); err != nil || n != 100 {
		t.Fatal(n, err)
	}
	raw, err := EncodeWebhookBatch(prefix, [][]byte{[]byte(`{"a":"<"}`), []byte(`{"b":2}`)}, 2<<20)
	if err != nil || !bytes.Equal(raw, append(append(bytes.Clone(prefix), []byte(`{"a":"<"},{"b":2}`)...), []byte(`]}`)...)) {
		t.Fatal(string(raw), err)
	}
}
