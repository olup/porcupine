package porcupine

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (s *Service) exportReader(ctx context.Context, projectID string) (namespace, reader, password string, err error) {
	if s.exports == nil {
		err = errors.New("export reader unavailable")
		return
	}
	return s.configureExportReader(ctx, projectID, s.exports.config, "_export")
}

func (s *Service) exportReaderWithConfig(ctx context.Context, projectID string, config ExportConfig) (namespace, reader, password string, err error) {
	return s.configureExportReader(ctx, projectID, config, "_pipeline_export")
}

func (s *Service) configureExportReader(ctx context.Context, projectID string, config ExportConfig, suffix string) (namespace, reader, password string, err error) {
	if s.ch == nil {
		return "", "", "", errors.New("export reader unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	namespace, _, _, err = s.analyticsReader(ctx, projectID)
	if err != nil {
		return
	}
	reader = namespace + suffix
	mac := hmac.New(sha256.New, s.querySecret)
	mac.Write([]byte("export:" + projectID + "\x00" + s.ch.config.Database))
	password = hex.EncodeToString(mac.Sum(nil))
	if config.MemoryBytes <= 0 || config.MaxExecutionSeconds <= 0 || config.MaxReadBytes <= 0 || config.TemporaryBytes <= 0 || config.BandwidthBytesPerSecond <= 0 {
		err = errors.New("invalid export reader budgets")
		return
	}
	if err = s.queryMu.Lock(ctx); err != nil {
		return
	}
	defer s.queryMu.Unlock()
	if err = ctx.Err(); err != nil {
		return
	}
	passwordHash := sha256.Sum256([]byte(password))
	authentication := " IDENTIFIED WITH sha256_hash BY '" + hex.EncodeToString(passwordHash[:]) + "'"
	spill := min(int64(128<<20), max(int64(1), config.MemoryBytes/4))
	settings := []string{
		"readonly=1 READONLY", "allow_ddl=0 READONLY", "allow_introspection_functions=0 READONLY",
		"final=1 READONLY", "optimize_move_to_prewhere_if_final=0 READONLY", "max_threads=1 READONLY",
		"max_memory_usage=" + strconv.FormatInt(config.MemoryBytes, 10) + " READONLY",
		"max_execution_time=" + strconv.Itoa(config.MaxExecutionSeconds) + " READONLY",
		"max_bytes_to_read=" + strconv.FormatInt(config.MaxReadBytes, 10) + " READONLY",
		"max_network_bandwidth=" + strconv.FormatInt(config.BandwidthBytesPerSecond, 10) + " READONLY",
		"max_remote_read_network_bandwidth=" + strconv.FormatInt(config.BandwidthBytesPerSecond, 10) + " READONLY",
		"max_temporary_data_on_disk_size_for_query=" + strconv.FormatInt(config.TemporaryBytes, 10) + " READONLY",
		"max_bytes_before_external_group_by=" + strconv.FormatInt(spill, 10) + " READONLY",
		"max_bytes_before_external_sort=" + strconv.FormatInt(spill, 10) + " READONLY",
		"max_rows_to_read=0 READONLY", "max_result_rows=0 READONLY", "max_result_bytes=0 READONLY",
		"read_overflow_mode='throw' READONLY", "result_overflow_mode='throw' READONLY", "timeout_overflow_mode='throw' READONLY",
		"session_timezone='UTC' READONLY", "use_query_cache=0 READONLY", "output_format_json_quote_64bit_integers=1 READONLY",
		"cancel_http_readonly_queries_on_client_close=1 READONLY",
	}
	database := s.ch.config.Database
	statements := []string{
		"CREATE USER IF NOT EXISTS " + reader + authentication,
		"ALTER USER " + reader + authentication + " DEFAULT ROLE NONE SETTINGS " + strings.Join(settings, ", "),
		"REVOKE ALL ON *.* FROM " + reader,
		"CREATE ROW POLICY OR REPLACE " + namespace + suffix + "_scope ON " + database + "." + eventTable + " USING project_id=unhex('" + hex.EncodeToString([]byte(projectID)) + "') TO " + reader,
		"GRANT SELECT(event_id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,received_at,received_at_ms,revision) ON " + database + "." + eventTable + " TO " + reader,
		"GRANT SELECT ON " + namespace + ".events TO " + reader,
	}
	for _, statement := range statements {
		if _, err = s.ch.admin(ctx, statement); err != nil {
			return
		}
	}
	return
}

var exportQueryID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// openExport streams only through the restricted reader. Its context owns the
// whole-query deadline; the interactive transport keeps its existing short timeouts.
func (c *clickHouse) openExport(ctx context.Context, namespace, reader, password, queryID, canonicalSQL string, parameters map[string]string) (io.ReadCloser, error) {
	if !clickHouseIdentifier.MatchString(namespace) || !clickHouseIdentifier.MatchString(reader) || !exportQueryID.MatchString(queryID) {
		return nil, errors.New("invalid export reader request")
	}
	values := url.Values{
		"database": {namespace}, "default_format": {"JSONCompact"}, "wait_end_of_query": {"0"},
		"query_id": {queryID}, "buffer_size": {"65536"},
	}
	for name, value := range parameters {
		if !clickHouseParameterName.MatchString(name) {
			return nil, errors.New("invalid export query parameter")
		}
		values.Set("param_"+name, value)
	}
	base := c.client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	original, ok := base.(*http.Transport)
	if !ok {
		return nil, errors.New("unsupported export transport")
	}
	transport := original.Clone()
	transport.ResponseHeaderTimeout = 0
	transport.DisableCompression = true
	transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost, transport.MaxIdleConns = 1, 1, 1
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	u := *c.url
	u.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(canonicalSQL))
	if err != nil {
		transport.CloseIdleConnections()
		return nil, &clickHouseError{}
	}
	request.Header.Set("X-ClickHouse-User", reader)
	request.Header.Set("X-ClickHouse-Key", password)
	request.Header.Set("Content-Type", "text/plain; charset=utf-8")
	response, err := client.Do(request)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, &clickHouseError{}
	}
	failure := func(code int) (io.ReadCloser, error) {
		response.Body.Close()
		transport.CloseIdleConnections()
		return nil, &clickHouseError{Code: code, Status: response.StatusCode}
	}
	code, _ := strconv.Atoi(response.Header.Get("X-ClickHouse-Exception-Code"))
	if response.StatusCode < 200 || response.StatusCode >= 300 || code != 0 {
		return failure(code)
	}
	buffered := bufio.NewReaderSize(response.Body, 64<<10)
	for n := 0; n < 4096; n++ {
		first, err := buffered.Peek(1)
		if err != nil {
			return failure(0)
		}
		if first[0] == '{' {
			return &exportResponseBody{reader: buffered, body: response.Body, transport: transport}, nil
		}
		if first[0] != ' ' && first[0] != '\n' && first[0] != '\r' && first[0] != '\t' {
			prefix, _ := buffered.Peek(min(buffered.Buffered(), 4096))
			if match := clickHouseExceptionCode.FindSubmatch(prefix); len(match) == 2 {
				code, _ = strconv.Atoi(string(match[1]))
			}
			return failure(code)
		}
		buffered.Discard(1)
	}
	return failure(0)
}

type exportResponseBody struct {
	reader    io.Reader
	body      io.ReadCloser
	transport *http.Transport
	once      sync.Once
	closeErr  error
}

func (b *exportResponseBody) Read(p []byte) (int, error) { return b.reader.Read(p) }
func (b *exportResponseBody) Close() error {
	b.once.Do(func() { b.closeErr = b.body.Close(); b.transport.CloseIdleConnections() })
	return b.closeErr
}

func (c *clickHouse) cancelExport(ctx context.Context, queryID string) error {
	if !exportQueryID.MatchString(queryID) {
		return errors.New("invalid export query identifier")
	}
	_, err := c.admin(ctx, "KILL QUERY WHERE query_id=unhex('"+hex.EncodeToString([]byte(queryID))+"') SYNC")
	return err
}
