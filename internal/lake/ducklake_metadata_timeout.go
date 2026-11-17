package lake

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
)

const defaultDuckLakeMetadataTimeoutSeconds = 10

// This bounds ordinary native metadata connection, statement and dead-peer
// waits. Socket options are platform-dependent; the query watchdog remains
// necessary when a live proxy acknowledges TCP but withholds PostgreSQL replies.
func nativePostgresMetadataURL(raw string, seconds int) (string, error) {
	if seconds < 2 || seconds > 60 {
		return "", errors.New("DuckLake metadata timeout must be between 2 and 60 seconds")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" || strings.Contains(u.Host, ",") {
		return "", errors.New("native PostgreSQL metadata requires a single-host PostgreSQL URL")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", errors.New("invalid native PostgreSQL metadata URL parameters")
	}
	for _, key := range []string{"host", "hostaddr", "port"} {
		values := q[key]
		if len(values) > 1 || len(values) == 1 && strings.Contains(values[0], ",") {
			return "", errors.New("native PostgreSQL metadata requires a single host and port")
		}
	}
	if len(q["options"]) > 1 {
		return "", errors.New("ambiguous native PostgreSQL metadata startup options")
	}
	options, err := boundedPostgresOptions(q.Get("options"), seconds)
	if err != nil {
		return "", err
	}
	q.Set("options", options)
	q.Set("connect_timeout", strconv.Itoa(seconds))
	q.Set("keepalives", "1")
	q.Set("keepalives_idle", strconv.Itoa(seconds))
	q.Set("keepalives_interval", "1")
	q.Set("keepalives_count", "3")
	q.Set("tcp_user_timeout", strconv.Itoa(seconds*1000))
	// libpq URI parameters use percent encoding rather than form encoding.
	// A plus sign must not stand for a startup-options argument separator.
	u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	return u.String(), nil
}

func boundedPostgresOptions(raw string, seconds int) (string, error) {
	invalid := errors.New("native PostgreSQL startup options must contain complete -c name=value or --name=value settings")
	args, err := splitPostgresOptions(raw)
	if err != nil {
		return "", invalid
	}
	kept := make([]string, 0, len(args)+4)
	for i := 0; i < len(args); i++ {
		option := args[i]
		switch {
		case option == "-c":
			i++
			if i == len(args) {
				return "", invalid
			}
			option = args[i]
		case strings.HasPrefix(option, "-c") && len(option) > 2:
			option = option[2:]
		case strings.HasPrefix(option, "--"):
			option = option[2:]
		default:
			return "", invalid
		}
		name, _, ok := strings.Cut(option, "=")
		if !ok || name == "" {
			return "", invalid
		}
		for _, char := range name {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '.' || char == '-') {
				return "", invalid
			}
		}
		normalized := strings.ReplaceAll(strings.ToLower(name), "-", "_")
		if normalized == "statement_timeout" || normalized == "lock_timeout" {
			continue
		}
		kept = append(kept, "-c", option)
	}
	kept = append(kept, "-c", "statement_timeout="+strconv.Itoa(seconds*1000), "-c", "lock_timeout="+strconv.Itoa(seconds*500))
	for i, arg := range kept {
		kept[i] = escapePostgresOption(arg)
	}
	return strings.Join(kept, " "), nil
}

// libpq options use backslash escaping, not shell quoting. Re-encoding parsed
// arguments makes a trailing operator backslash unable to swallow our boundary.
func splitPostgresOptions(raw string) ([]string, error) {
	var args []string
	var part strings.Builder
	escaped, started := false, false
	for i := 0; i < len(raw); i++ {
		char := raw[i]
		if escaped {
			part.WriteByte(char)
			escaped = false
			started = true
			continue
		}
		if char == '\\' {
			escaped = true
			started = true
			continue
		}
		if postgresOptionSpace(char) {
			if started {
				args = append(args, part.String())
				part.Reset()
				started = false
			}
			continue
		}
		part.WriteByte(char)
		started = true
	}
	if escaped {
		return nil, errors.New("incomplete PostgreSQL option escape")
	}
	if started {
		args = append(args, part.String())
	}
	return args, nil
}

func escapePostgresOption(raw string) string {
	var encoded strings.Builder
	for i := 0; i < len(raw); i++ {
		char := raw[i]
		if char == '\\' || postgresOptionSpace(char) {
			encoded.WriteByte('\\')
		}
		encoded.WriteByte(char)
	}
	return encoded.String()
}
func postgresOptionSpace(char byte) bool {
	return char == ' ' || char == '\t' || char == '\n' || char == '\r' || char == '\v' || char == '\f'
}
