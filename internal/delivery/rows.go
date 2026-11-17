package delivery

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
)

// RowEncoder formats one query result row without owning storage or jobs.
type RowEncoder struct {
	format string
	names  [][]byte
	header []byte
}

var ErrEncoding = errors.New("invalid delivery row encoding")

func NewRowEncoder(format string, names []string) (*RowEncoder, error) {
	if format != "jsonl" && format != "csv" {
		return nil, ErrEncoding
	}
	encoder := &RowEncoder{format: format, names: make([][]byte, len(names))}
	seen := map[string]bool{}
	for i, name := range names {
		if format == "jsonl" && seen[name] {
			return nil, ErrEncoding
		}
		seen[name] = true
		encoded, err := EncodeJSON(name)
		if err != nil {
			return nil, err
		}
		encoder.names[i] = encoded
	}
	if format == "csv" {
		var header bytes.Buffer
		writer := csv.NewWriter(&header)
		if err := writer.Write(names); err != nil {
			return nil, err
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return nil, err
		}
		encoder.header = header.Bytes()
	}
	return encoder, nil
}

func (w *RowEncoder) Header() []byte { return w.header }

func (w *RowEncoder) Encode(values []json.RawMessage) ([]byte, error) {
	if w == nil || len(values) != len(w.names) {
		return nil, ErrEncoding
	}
	var row bytes.Buffer
	if w.format == "jsonl" {
		row.WriteByte('{')
		for i, value := range values {
			if i > 0 {
				row.WriteByte(',')
			}
			row.Write(w.names[i])
			row.WriteByte(':')
			row.Write(value)
		}
		row.WriteString("}\n")
	} else {
		fields := make([]string, len(values))
		for i, value := range values {
			if bytes.Equal(value, []byte("null")) {
				continue
			}
			if len(value) > 0 && value[0] == '"' {
				if json.Unmarshal(value, &fields[i]) != nil {
					return nil, ErrEncoding
				}
			} else {
				fields[i] = string(value)
			}
		}
		writer := csv.NewWriter(&row)
		if err := writer.Write(fields); err != nil {
			return nil, ErrEncoding
		}
		writer.Flush()
		if writer.Error() != nil {
			return nil, ErrEncoding
		}
	}
	return row.Bytes(), nil
}
