package porcupine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"unicode/utf8"
)

const (
	exportMetadataValueBytes = 256 << 10
	exportMetadataTotalBytes = 1 << 20
)

var (
	errExportStreamInvalid    = errors.New("invalid export stream")
	errExportValueTooLarge    = errors.New("export value exceeds configured limit")
	errExportMetadataTooLarge = errors.New("export metadata exceeds configured limit")
)

// streamExportJSON validates the complete JSONCompact envelope. Row callbacks
// may run before the result is verified; their output must remain unpublished.
func streamExportJSON(ctx context.Context, src io.Reader, maxValueBytes int64, callbacks exportStreamCallbacks) (exportStreamResult, error) {
	var result exportStreamResult
	if maxValueBytes <= 0 {
		return result, errExportValueTooLarge
	}
	input := exportJSONInput{ctx: ctx, reader: bufio.NewReaderSize(src, 64<<10), limit: maxValueBytes}
	fail := func(err error) (exportStreamResult, error) {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, err
	}
	if err := input.expect('{'); err != nil {
		return fail(err)
	}
	seen := map[string]bool{}
	first := true
	var metadataBytes int64
	metadataValue := func(limit int64) ([]byte, error) {
		raw, err := input.value(min(limit, int64(exportMetadataValueBytes)))
		if errors.Is(err, errExportValueTooLarge) {
			return nil, errExportMetadataTooLarge
		}
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(json.RawMessage(raw))
		if err != nil {
			return nil, errExportStreamInvalid
		}
		metadataBytes += int64(len(encoded))
		if len(encoded) > exportMetadataValueBytes || metadataBytes > exportMetadataTotalBytes {
			return nil, errExportMetadataTooLarge
		}
		return raw, nil
	}
	var declaredRows int64
	for {
		next, err := input.peek()
		if err != nil {
			return fail(err)
		}
		if next == '}' {
			input.reader.ReadByte()
			break
		}
		if !first {
			if err := input.expect(','); err != nil {
				return fail(err)
			}
		}
		first = false
		keyRaw, err := input.value(128)
		if errors.Is(err, errExportValueTooLarge) {
			return fail(errExportMetadataTooLarge)
		}
		if err != nil {
			return fail(err)
		}
		var key string
		if json.Unmarshal(keyRaw, &key) != nil || seen[key] {
			return fail(errExportStreamInvalid)
		}
		seen[key] = true
		if err := input.expect(':'); err != nil {
			return fail(err)
		}
		switch key {
		case "meta":
			if seen["data"] {
				return fail(errExportStreamInvalid)
			}
			raw, err := metadataValue(maxValueBytes)
			if err != nil {
				return fail(err)
			}
			result.Columns, err = exportColumns(raw)
			if err != nil {
				return fail(err)
			}
			if callbacks.Columns != nil {
				if err := callbacks.Columns(result.Columns); err != nil {
					return fail(err)
				}
			}
		case "data":
			if !seen["meta"] || seen["rows"] {
				return fail(errExportStreamInvalid)
			}
			if err := input.expect('['); err != nil {
				return fail(err)
			}
			firstRow := true
			for {
				next, err := input.peek()
				if err != nil {
					return fail(err)
				}
				if next == ']' {
					input.reader.ReadByte()
					break
				}
				if !firstRow {
					if err := input.expect(','); err != nil {
						return fail(err)
					}
				}
				firstRow = false
				raw, err := input.value(maxValueBytes)
				if err != nil {
					return fail(err)
				}
				row, err := exportRow(raw, len(result.Columns))
				if err != nil {
					return fail(err)
				}
				if ctx.Err() != nil {
					return fail(ctx.Err())
				}
				if result.Rows == math.MaxInt64 {
					return fail(errExportStreamInvalid)
				}
				if callbacks.Row != nil {
					if err := callbacks.Row(row); err != nil {
						return fail(err)
					}
				}
				result.Rows++
			}
		case "rows":
			if !seen["data"] {
				return fail(errExportStreamInvalid)
			}
			raw, err := metadataValue(min(maxValueBytes, 64))
			if err != nil {
				return fail(err)
			}
			if len(raw) == 0 || raw[0] < '0' || raw[0] > '9' || json.Unmarshal(raw, &declaredRows) != nil || declaredRows < 0 {
				return fail(errExportStreamInvalid)
			}
		case "statistics":
			raw, err := metadataValue(maxValueBytes)
			if err != nil {
				return fail(err)
			}
			members, err := exportJSONObject(raw)
			if err != nil {
				return fail(err)
			}
			for name, value := range members {
				switch name {
				case "elapsed":
					if len(value) == 0 || value[0] == 'n' || json.Unmarshal(value, &result.Statistics.Elapsed) != nil || result.Statistics.Elapsed < 0 {
						return fail(errExportStreamInvalid)
					}
				case "rows_read", "bytes_read":
					if _, err := exportUnsigned(value); err != nil {
						return fail(err)
					}
				default:
					return fail(errExportStreamInvalid)
				}
			}
		case "rows_before_limit_at_least", "rows_before_aggregation":
			raw, err := metadataValue(min(maxValueBytes, 64))
			if err != nil {
				return fail(err)
			}
			count, err := exportUnsigned(raw)
			if err != nil {
				return fail(err)
			}
			if key == "rows_before_limit_at_least" {
				result.RowsBeforeLimitAtLeast = &count
			}
		case "totals":
			if !seen["meta"] {
				return fail(errExportStreamInvalid)
			}
			raw, err := metadataValue(maxValueBytes)
			if err != nil {
				return fail(err)
			}
			if _, err := exportRow(raw, len(result.Columns)); err != nil {
				return fail(err)
			}
			result.Totals = append(json.RawMessage(nil), raw...)
		case "extremes":
			if !seen["meta"] {
				return fail(errExportStreamInvalid)
			}
			raw, err := metadataValue(maxValueBytes)
			if err != nil {
				return fail(err)
			}
			members, err := exportJSONObject(raw)
			if err != nil || len(members) != 2 {
				return fail(errExportStreamInvalid)
			}
			for _, name := range []string{"min", "max"} {
				if _, err := exportRow(members[name], len(result.Columns)); err != nil {
					return fail(err)
				}
			}
			result.Extremes = append(json.RawMessage(nil), raw...)
		default:
			return fail(errExportStreamInvalid)
		}
	}
	if !seen["meta"] || !seen["data"] || !seen["rows"] || declaredRows != result.Rows {
		return fail(errExportStreamInvalid)
	}
	if _, err := input.peek(); !errors.Is(err, io.EOF) {
		return fail(errExportStreamInvalid)
	}
	if ctx.Err() != nil {
		return fail(ctx.Err())
	}
	return result, nil
}

func exportColumns(raw []byte) ([]AnalyticsResultColumn, error) {
	if len(raw) == 0 || raw[0] != '[' {
		return nil, errExportStreamInvalid
	}
	var entries []json.RawMessage
	if json.Unmarshal(raw, &entries) != nil {
		return nil, errExportStreamInvalid
	}
	columns := make([]AnalyticsResultColumn, 0, len(entries))
	seen := map[string]bool{}
	for _, entry := range entries {
		members, err := exportJSONObject(entry)
		if err != nil || len(members) != 2 {
			return nil, errExportStreamInvalid
		}
		var column AnalyticsResultColumn
		if json.Unmarshal(members["name"], &column.Name) != nil || json.Unmarshal(members["type"], &column.Type) != nil || column.Name == "" || column.Type == "" || seen[column.Name] {
			return nil, errExportStreamInvalid
		}
		seen[column.Name] = true
		columns = append(columns, column)
	}
	return columns, nil
}

func exportRow(raw []byte, width int) ([]json.RawMessage, error) {
	if len(raw) == 0 || raw[0] != '[' {
		return nil, errExportStreamInvalid
	}
	var row []json.RawMessage
	if json.Unmarshal(raw, &row) != nil || len(row) != width {
		return nil, errExportStreamInvalid
	}
	return row, nil
}

// The caller has already bounded raw, so Decoder.Token cannot allocate an
// unbounded string here. Duplicate member names are rejected rather than lost.
func exportJSONObject(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errExportStreamInvalid
	}
	members := map[string]json.RawMessage{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errExportStreamInvalid
		}
		key, ok := token.(string)
		if !ok {
			return nil, errExportStreamInvalid
		}
		if _, exists := members[key]; exists {
			return nil, errExportStreamInvalid
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, errExportStreamInvalid
		}
		members[key] = value
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return nil, errExportStreamInvalid
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return nil, errExportStreamInvalid
	}
	return members, nil
}

func exportUnsigned(raw []byte) (uint64, error) {
	var value uint64
	if len(raw) > 0 && raw[0] >= '0' && raw[0] <= '9' && json.Unmarshal(raw, &value) == nil {
		return value, nil
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if value, err := strconv.ParseUint(text, 10, 64); err == nil {
			return value, nil
		}
	}
	return 0, errExportStreamInvalid
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
func (r *exportJSONInput) expect(want byte) error {
	got, err := r.peek()
	if err != nil || got != want {
		return errExportStreamInvalid
	}
	r.reader.ReadByte()
	return nil
}

// value scans one complete JSON value with bounded storage before asking the
// standard decoder to validate it. The envelope and data array are never buffered.
func (r *exportJSONInput) value(limit int64) ([]byte, error) {
	limit = min(limit, r.limit)
	first, err := r.peek()
	if err != nil {
		return nil, errExportStreamInvalid
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
			return nil, errExportValueTooLarge
		}
		if r.readCount%4096 == 0 && r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, errExportStreamInvalid
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
					return nil, errExportStreamInvalid
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
				return nil, errExportStreamInvalid
			}
			if exportWhitespace(next[0]) || next[0] == ',' || next[0] == ']' || next[0] == '}' {
				break
			}
		}
	}
complete:
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return nil, errExportStreamInvalid
	}
	r.scratch = raw
	return raw, nil
}
