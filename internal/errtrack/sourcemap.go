package errtrack

import (
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"path"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	MaxSourceMapBytes = 2 << 20
	maxSourceMapDepth = 16
	maxMapCoordinate  = maxCoordinate - 1
)

var (
	errInvalidSourceMap = errors.New("invalid source map v3")
	errSourceMapSize    = errors.New("source map exceeds size limit")
)

type OriginalPosition struct {
	Filename string `json:"filename"`
	Line     int    `json:"lineno"`
	Column   int    `json:"colno"`
	Function string `json:"function,omitempty"`
}

type sourceMapJSON struct {
	Version    int                     `json:"version"`
	File       string                  `json:"file,omitempty"`
	SourceRoot string                  `json:"sourceRoot,omitempty"`
	Sources    *[]*string              `json:"sources,omitempty"`
	Names      *[]*string              `json:"names,omitempty"`
	Mappings   *string                 `json:"mappings,omitempty"`
	Sections   *[]sourceMapSectionJSON `json:"sections,omitempty"`
}

type sourceMapSectionJSON struct {
	Offset struct {
		Line   *int32 `json:"line"`
		Column *int32 `json:"column"`
	} `json:"offset"`
	Map *sourceMapJSON  `json:"map"`
	URL json.RawMessage `json:"url,omitempty"`
}

type mapPosition struct {
	line, column int32
}

func (p mapPosition) before(other mapPosition) bool {
	return p.line < other.line || (p.line == other.line && p.column < other.column)
}

// A section's column offset applies only to its first generated line.
func (p mapPosition) offsetBy(offset mapPosition) mapPosition {
	if p.line == 0 {
		p.column += offset.column
	}
	p.line += offset.line
	return p
}

type mapSegment struct {
	generated    mapPosition
	original     mapPosition
	source, name int32
}

type sourceMapSection struct {
	offset mapPosition
	child  *SourceMap
}

type SourceMap struct {
	sections   []sourceMapSection
	segments   []mapSegment
	sources    []*string
	sourceRoot string
	names      []*string
}

// PrepareSourceMap validates an upload and removes embedded source text and extensions.
func PrepareSourceMap(raw []byte) ([]byte, error) {
	document, err := readSourceMap(raw)
	if err != nil {
		return nil, err
	}
	if _, _, err := document.decode(0); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		return nil, errInvalidSourceMap
	}
	if len(canonical) > MaxSourceMapBytes {
		return nil, errSourceMapSize
	}
	return canonical, nil
}

func ParseSourceMap(raw []byte) (*SourceMap, error) {
	document, err := readSourceMap(raw)
	if err != nil {
		return nil, err
	}
	parsed, _, err := document.decode(0)
	return parsed, err
}

func readSourceMap(raw []byte) (*sourceMapJSON, error) {
	if len(raw) > MaxSourceMapBytes {
		return nil, errSourceMapSize
	}
	var document sourceMapJSON
	if !utf8.Valid(raw) || json.Unmarshal(raw, &document) != nil {
		return nil, errInvalidSourceMap
	}
	return &document, nil
}

func (m *sourceMapJSON) decode(depth int) (*SourceMap, mapPosition, error) {
	if m.Version != 3 || depth >= maxSourceMapDepth {
		return nil, mapPosition{}, errInvalidSourceMap
	}
	if m.Sections != nil {
		return m.decodeSections(depth)
	}
	if m.Sources == nil || m.Mappings == nil {
		return nil, mapPosition{}, errInvalidSourceMap
	}

	parsed := &SourceMap{sources: *m.Sources, sourceRoot: m.SourceRoot}
	if m.Names != nil {
		parsed.names = *m.Names
		for _, name := range parsed.names {
			if name == nil {
				return nil, mapPosition{}, errInvalidSourceMap
			}
		}
	}
	last, err := parsed.decodeMappings(*m.Mappings)
	if err != nil {
		return nil, mapPosition{}, err
	}
	return parsed, last, nil
}

func (m *sourceMapJSON) decodeSections(depth int) (*SourceMap, mapPosition, error) {
	if m.Sources != nil || m.Names != nil || m.Mappings != nil || m.SourceRoot != "" {
		return nil, mapPosition{}, errInvalidSourceMap
	}
	parsed := &SourceMap{sections: make([]sourceMapSection, 0, len(*m.Sections))}
	var last mapPosition
	for i, section := range *m.Sections {
		if section.Map == nil || len(section.URL) != 0 || section.Offset.Line == nil || section.Offset.Column == nil {
			return nil, mapPosition{}, errInvalidSourceMap
		}
		offset := mapPosition{*section.Offset.Line, *section.Offset.Column}
		if offset.line < 0 || offset.column < 0 || offset.line > maxMapCoordinate || offset.column > maxMapCoordinate ||
			(i > 0 && !last.before(offset)) {
			return nil, mapPosition{}, errInvalidSourceMap
		}
		child, end, err := section.Map.decode(depth + 1)
		if err != nil {
			return nil, mapPosition{}, err
		}
		if end.line > maxMapCoordinate-offset.line ||
			(end.line == 0 && end.column > maxMapCoordinate-offset.column) {
			return nil, mapPosition{}, errInvalidSourceMap
		}
		last = end.offsetBy(offset)
		parsed.sections = append(parsed.sections, sourceMapSection{offset: offset, child: child})
	}
	return parsed, last, nil
}

// Original accepts one-based browser positions. Source maps express columns in
// UTF-16 code units, so conversion is limited to changing the index base.
func (m *SourceMap) Original(line, column int) (OriginalPosition, bool) {
	if m == nil || line < 1 || column < 1 || line-1 > maxMapCoordinate || column-1 > maxMapCoordinate {
		return OriginalPosition{}, false
	}
	return m.original(mapPosition{int32(line - 1), int32(column - 1)})
}

func (m *SourceMap) original(generated mapPosition) (OriginalPosition, bool) {
	if len(m.sections) > 0 {
		i := sort.Search(len(m.sections), func(i int) bool {
			return generated.before(m.sections[i].offset)
		})
		if i == 0 {
			return OriginalPosition{}, false
		}
		section := m.sections[i-1]
		generated.line -= section.offset.line
		if generated.line == 0 {
			generated.column -= section.offset.column
		}
		return section.child.original(generated)
	}

	i := sort.Search(len(m.segments), func(i int) bool {
		return generated.before(m.segments[i].generated)
	})
	if i == 0 {
		return OriginalPosition{}, false
	}
	segment := m.segments[i-1]
	if segment.generated.line != generated.line || segment.source < 0 || m.sources[segment.source] == nil {
		return OriginalPosition{}, false
	}
	filename := displaySourcePath(m.sourceRoot, *m.sources[segment.source])
	if filename == "" {
		return OriginalPosition{}, false
	}
	position := OriginalPosition{
		Filename: filename,
		Line:     int(segment.original.line) + 1,
		Column:   int(segment.original.column) + 1,
	}
	if segment.name >= 0 {
		name := *m.names[segment.name]
		if len(name) <= 256 && functionPattern.MatchString(name) {
			position.Function = name
		}
	}
	return position, true
}

func (m *SourceMap) decodeMappings(mappings string) (mapPosition, error) {
	var generated, last mapPosition
	var previous [5]int64
	afterComma, unsorted := false, false
	for mappings != "" {
		if mappings[0] == ';' && !afterComma {
			generated.line++
			generated.column, previous[0] = 0, 0
			mappings = mappings[1:]
			continue
		}
		end := strings.IndexAny(mappings, ",;")
		if end < 0 {
			end = len(mappings)
		}
		var fields [5]int64
		count, ok := decodeMapSegment(mappings[:end], &fields)
		if !ok {
			return mapPosition{}, errInvalidSourceMap
		}
		for i := 0; i < count; i++ {
			previous[i] += fields[i]
			if previous[i] < 0 || previous[i] > maxMapCoordinate {
				return mapPosition{}, errInvalidSourceMap
			}
		}
		generated.column = int32(previous[0])
		segment := mapSegment{generated: generated, source: -1, name: -1}
		if count >= 4 {
			if previous[1] >= int64(len(m.sources)) {
				return mapPosition{}, errInvalidSourceMap
			}
			segment.source = int32(previous[1])
			segment.original = mapPosition{int32(previous[2]), int32(previous[3])}
		}
		if count == 5 {
			if previous[4] >= int64(len(m.names)) {
				return mapPosition{}, errInvalidSourceMap
			}
			segment.name = int32(previous[4])
		}
		m.segments = append(m.segments, segment)
		if generated.before(last) {
			unsorted = true
		} else {
			last = generated
		}
		mappings = mappings[end:]
		afterComma = mappings != "" && mappings[0] == ','
		if afterComma {
			mappings = mappings[1:]
		}
	}
	if afterComma {
		return mapPosition{}, errInvalidSourceMap
	}
	if unsorted {
		sort.SliceStable(m.segments, func(i, j int) bool {
			return m.segments[i].generated.before(m.segments[j].generated)
		})
	}
	return last, nil
}

func decodeMapSegment(encoded string, fields *[5]int64) (int, bool) {
	count := 0
	var value uint64
	var shift uint
	for i := 0; i < len(encoded); i++ {
		if count == len(fields) || shift > 30 {
			return 0, false
		}
		digit, ok := mapBase64Digit(encoded[i])
		if !ok {
			return 0, false
		}
		value |= uint64(digit&31) << shift
		if digit&32 != 0 {
			shift += 5
			continue
		}
		if value > math.MaxUint32 {
			return 0, false
		}
		fields[count] = int64(value >> 1)
		if value&1 != 0 {
			fields[count] = -fields[count]
			if value == 1 {
				fields[count] = math.MinInt32
			}
		}
		count++
		value, shift = 0, 0
	}
	return count, shift == 0 && (count == 1 || count == 4 || count == 5)
}

func mapBase64Digit(value byte) (byte, bool) {
	switch {
	case value >= 'A' && value <= 'Z':
		return value - 'A', true
	case value >= 'a' && value <= 'z':
		return value - 'a' + 26, true
	case value >= '0' && value <= '9':
		return value - '0' + 52, true
	case value == '+':
		return 62, true
	case value == '/':
		return 63, true
	default:
		return 0, false
	}
}

func displaySourcePath(root, source string) string {
	if source == "" || len(source) > maxFilename || len(root) > maxFilename {
		return ""
	}
	source = strings.ReplaceAll(source, "\\", "/")
	root = strings.ReplaceAll(root, "\\", "/")
	sourceURL, sourceErr := url.Parse(source)
	rootURL, rootErr := url.Parse(root)

	if sourceErr == nil && (sourceURL.IsAbs() || sourceURL.Host != "") {
		if sourceURL.Opaque != "" {
			return ""
		}
		return displaySourceURL(sourceURL)
	}
	if sourceErr == nil {
		source = sourceURL.Path
	}
	if source == "" {
		return ""
	}
	if rootErr == nil && (rootURL.IsAbs() || rootURL.Host != "") {
		if rootURL.Opaque != "" {
			return ""
		}
		if !strings.HasPrefix(source, "/") {
			source = path.Join(rootURL.Path, source)
		}
		rootURL.Path, rootURL.RawPath = source, ""
		return displaySourceURL(rootURL)
	}
	if rootErr == nil {
		root = rootURL.Path
	}
	if strings.HasPrefix(source, "/") {
		return displaySourceURL(&url.URL{Path: path.Clean(source)})
	}
	return displaySourceURL(&url.URL{Path: path.Join(root, source)})
}

func displaySourceURL(u *url.URL) string {
	u.User = nil
	u.RawQuery, u.Fragment, u.RawFragment = "", "", ""
	u.ForceQuery = false
	if u.Host != "" {
		return displayFilename(u.EscapedPath())
	}
	return displayFilename(u.String())
}
