package errtrack

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxExceptions = 4
	maxFrames     = 32
	maxFilename   = 2048
	maxMessage    = 512
	maxCoordinate = 1<<31 - 1
)

type Properties struct {
	List        []Exception `json:"$exception_list"`
	BuildID     string      `json:"$exception_build_id,omitempty"`
	Release     string      `json:"$exception_release,omitempty"`
	Fingerprint string      `json:"$exception_fingerprint"`
	Degraded    bool        `json:"$exception_degraded,omitempty"`
}

type Exception struct {
	Type       string      `json:"type,omitempty"`
	Value      string      `json:"value,omitempty"`
	Stacktrace *Stacktrace `json:"stacktrace,omitempty"`
}

type Stacktrace struct {
	Frames []Frame `json:"frames"`
}

type Frame struct {
	Filename string `json:"filename"`
	Line     int    `json:"lineno,omitempty"`
	Column   int    `json:"colno,omitempty"`
	Function string `json:"function,omitempty"`
	ChunkID  string `json:"chunk_id,omitempty"`
}

var (
	idPattern              = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
	typePattern            = regexp.MustCompile(`^[a-zA-Z_$][a-zA-Z0-9_.$ -]*$`)
	functionPattern        = regexp.MustCompile(`^[\pL\pN_$<>. /\[\]:-]+$`)
	secretPattern          = regexp.MustCompile(`(?i)\b(?:authorization|bearer|password|passwd|secret|(?:access[_ -]?)?token|api[_ -]?key|cookie)\s*(?:[:=]\s*|\s+)(?:(?:bearer|basic)\s+)?(?:"[^"]*"|'[^']*'|[^\s,;]+)`)
	emailPattern           = regexp.MustCompile(`(?i)[^\s<>()"'@]+@[a-z0-9.-]+\.[a-z]{2,}`)
	urlPattern             = regexp.MustCompile(`(?i)(?:[a-z][a-z0-9+.-]*://|(?:data|blob|javascript):)[^\s<>"']+`)
	inlineQueryPattern     = regexp.MustCompile(`[?#][^\s<>"']+`)
	displayFilenamePattern = regexp.MustCompile(`^[\pL\pN_./@%+~$:-]+$`)
	quotedPattern          = regexp.MustCompile("\"[^\"\\n]*\"|'[^'\\n]*'|`[^`\\n]*`")
)

// ValidID accepts a build or chunk identifier suitable for exact storage and lookup.
func ValidID(s string) bool {
	return len(s) <= 128 && idPattern.MatchString(s)
}

// AssetURL canonicalizes a browser asset URL for storage and exact lookup.
func AssetURL(s string) (string, bool) {
	if len(s) > maxFilename {
		return "", false
	}
	u, err := url.Parse(strings.SplitN(strings.SplitN(s, "#", 2)[0], "?", 2)[0])
	if err != nil || u.Opaque != "" || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", false
	}
	if port := u.Port(); port != "" {
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return "", false
		}
	}
	if strings.HasSuffix(u.Host, ":") {
		return "", false
	}
	u.User = nil
	u.Host = strings.ToLower(u.Host)
	if u.Scheme == "http" && u.Port() == "80" {
		u.Host = strings.TrimSuffix(u.Host, ":80")
	}
	if u.Scheme == "https" && u.Port() == "443" {
		u.Host = strings.TrimSuffix(u.Host, ":443")
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), true
}

// Normalize keeps only bounded exception fields and derives the grouping key from them.
func Normalize(input map[string]any) Properties {
	p := Properties{List: make([]Exception, 0)}
	if buildID, _ := input["$exception_build_id"].(string); ValidID(buildID) {
		p.BuildID = buildID
	}
	if release, _ := input["$exception_release"].(string); ValidID(release) {
		p.Release = release
	}
	list, _ := input["$exception_list"].([]any)
	if len(list) > maxExceptions {
		list = list[len(list)-maxExceptions:]
		p.Degraded = true
	}
	for _, item := range list {
		exception, complete := normalizeException(item)
		if !complete {
			p.Degraded = true
		}
		if exception != (Exception{}) {
			p.List = append(p.List, exception)
		}
	}
	p.Degraded = p.Degraded || len(p.List) == 0
	p.Fingerprint = fingerprint(p)
	return p
}

func normalizeException(item any) (Exception, bool) {
	raw, _ := item.(map[string]any)
	errorType, _ := raw["type"].(string)
	if len(errorType) > 128 || !typePattern.MatchString(errorType) {
		errorType = ""
	}
	e := Exception{Type: errorType, Value: safeMessage(raw["value"])}
	stack, _ := raw["stacktrace"].(map[string]any)
	frames, _ := stack["frames"].([]any)
	complete := len(frames) > 0 && len(frames) <= maxFrames
	if len(frames) > maxFrames {
		frames = frames[len(frames)-maxFrames:]
	}
	for _, item := range frames {
		raw, _ := item.(map[string]any)
		frame := normalizeFrame(raw)
		if frame == (Frame{}) {
			complete = false
			continue
		}
		if e.Stacktrace == nil {
			e.Stacktrace = &Stacktrace{Frames: make([]Frame, 0, len(frames))}
		}
		complete = complete && frame.Line > 0
		e.Stacktrace.Frames = append(e.Stacktrace.Frames, frame)
	}
	return e, complete
}

func normalizeFrame(raw map[string]any) Frame {
	filename, _ := raw["filename"].(string)
	if asset, ok := AssetURL(filename); ok {
		filename = asset
	} else {
		filename = displayFilename(filename)
	}
	chunkID, _ := raw["chunk_id"].(string)
	if !ValidID(chunkID) {
		chunkID = ""
	}
	if filename == "" && chunkID == "" {
		return Frame{}
	}
	function, _ := raw["function"].(string)
	if len(function) > 256 || !functionPattern.MatchString(function) {
		function = ""
	}
	return Frame{Filename: filename, Function: function, ChunkID: chunkID, Line: coordinate(raw["lineno"]), Column: coordinate(raw["colno"])}
}

func displayFilename(s string) string {
	if len(s) > maxFilename {
		return ""
	}
	s = strings.SplitN(strings.SplitN(s, "#", 2)[0], "?", 2)[0]
	if !displayFilenamePattern.MatchString(s) || emailPattern.MatchString(s) {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil || u.User != nil || u.Host != "" || u.Opaque != "" {
		return ""
	}
	if u.Scheme != "" && u.Scheme != "file" && u.Scheme != "webpack" && u.Scheme != "app" && u.Scheme != "node" {
		return ""
	}
	return s
}

func coordinate(raw any) int {
	switch n := raw.(type) {
	case json.Number:
		value, err := strconv.ParseUint(string(n), 10, 31)
		if err == nil {
			return int(value)
		}
	case float64:
		if n >= 0 && n <= maxCoordinate && n == float64(int(n)) {
			return int(n)
		}
	case int:
		if n >= 0 && n <= maxCoordinate {
			return n
		}
	}
	return 0
}

func safeMessage(raw any) string {
	s, _ := raw.(string)
	// Redact before truncating so clipping cannot retain the start of a credential.
	// Drop oversize input entirely to keep processing cost bounded.
	if len(s) > 4*maxMessage || !utf8.ValidString(s) {
		return ""
	}
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = quotedPattern.ReplaceAllString(s, "[redacted]")
	s = secretPattern.ReplaceAllString(s, "[redacted]")
	s = urlPattern.ReplaceAllString(s, "[url]")
	s = inlineQueryPattern.ReplaceAllString(s, "[redacted]")
	s = emailPattern.ReplaceAllString(s, "[email]")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxMessage {
		end := maxMessage
		for !utf8.RuneStart(s[end]) {
			end--
		}
		s = s[:end]
	}
	return s
}

func fingerprint(p Properties) string {
	type site struct {
		Filename     string
		Line, Column int
		Function     string
	}
	type cause struct {
		Type, Value string
		Sites       []site
	}
	causes := make([]cause, 0, len(p.List))
	for _, exception := range p.List {
		c := cause{Type: exception.Type}
		if exception.Stacktrace == nil {
			c.Value = exception.Value
		} else {
			frames := exception.Stacktrace.Frames
			for _, frame := range frames[max(0, len(frames)-3):] {
				asset := frame.Filename
				if frame.ChunkID != "" {
					asset = frame.ChunkID
				}
				c.Sites = append(c.Sites, site{asset, frame.Line, frame.Column, frame.Function})
			}
		}
		causes = append(causes, c)
	}
	key, _ := json.Marshal(struct {
		Version int
		BuildID string
		Causes  []cause
	}{1, p.BuildID, causes})
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:])
}
