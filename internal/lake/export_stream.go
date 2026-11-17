package lake

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

const (
	exportMetadataTotalBytes = 1 << 20
)

var (
	ErrExportStreamInvalid    = errors.New("invalid export stream")
	ErrExportValueTooLarge    = errors.New("export value exceeds configured limit")
	ErrExportMetadataTooLarge = errors.New("export metadata exceeds configured limit")
)

// The caller has already bounded raw, so Decoder.Token cannot allocate an
// unbounded string here. Duplicate member names are rejected rather than lost.
func exportJSONObject(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, ErrExportStreamInvalid
	}
	members := map[string]json.RawMessage{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, ErrExportStreamInvalid
		}
		key, ok := token.(string)
		if !ok {
			return nil, ErrExportStreamInvalid
		}
		if _, exists := members[key]; exists {
			return nil, ErrExportStreamInvalid
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, ErrExportStreamInvalid
		}
		members[key] = value
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return nil, ErrExportStreamInvalid
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return nil, ErrExportStreamInvalid
	}
	return members, nil
}

type exportJSONInput struct {
	ctx       context.Context
	reader    *bufio.Reader
	limit     int64
	scratch   []byte
	readCount uint64
}

func exportWhitespace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }
func (r *exportJSONInput) peek() (byte, error) {
	for {
		if r.ctx.Err() != nil {
			return 0, r.ctx.Err()
		}
		b, err := r.reader.Peek(1)
		if err != nil {
			return 0, err
		}
		if !exportWhitespace(b[0]) {
			return b[0], nil
		}
		r.reader.Discard(1)
	}
}

// value scans one complete JSON value with bounded storage before asking the
// standard decoder to validate it. The envelope and data array are never buffered.
func (r *exportJSONInput) value(limit int64) ([]byte, error) {
	limit = min(limit, r.limit)
	first, err := r.peek()
	if err != nil {
		return nil, ErrExportStreamInvalid
	}
	raw := r.scratch[:0]
	if cap(raw) == 0 {
		raw = make([]byte, 0, int(min(limit, 128)))
	}
	composite := first == '{' || first == '['
	quoted := first == '"'
	inString, escaped := false, false
	depth := 0
	for {
		if int64(len(raw)) >= limit {
			return nil, ErrExportValueTooLarge
		}
		if r.readCount%4096 == 0 && r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, ErrExportStreamInvalid
		}
		r.readCount++
		raw = append(raw, b)
		if inString {
			if escaped {
				escaped = false
			} else if b == '\\' {
				escaped = true
			} else if b == '"' {
				inString = false
				if quoted {
					break
				}
			}
		} else {
			switch b {
			case '"':
				inString = true
			case '{', '[':
				depth++
				if depth > 256 {
					return nil, ErrExportStreamInvalid
				}
			case '}', ']':
				depth--
				if depth == 0 && composite {
					goto complete
				}
			}
		}
		if !composite && !quoted {
			next, err := r.reader.Peek(1)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, ErrExportStreamInvalid
			}
			if exportWhitespace(next[0]) || next[0] == ',' || next[0] == ']' || next[0] == '}' {
				break
			}
		}
	}
complete:
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return nil, ErrExportStreamInvalid
	}
	r.scratch = raw
	return raw, nil
}
