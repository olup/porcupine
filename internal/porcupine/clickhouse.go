package porcupine

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type ClickHouseConfig struct {
	URL            string
	Database       string
	User           string
	Password       string
	StoragePolicy  string
	HotStorageDays int
	ColdVolume     string
}

type clickHouse struct {
	config          ClickHouseConfig
	url             *url.URL
	client          *http.Client
	schemaMu        contextMutex
	projectionGuard *projectionGuard
}

// clickHouseError deliberately excludes server messages, SQL, and credentials.
type clickHouseError struct {
	Code   int
	Status int
}

func (e *clickHouseError) Error() string { return "ClickHouse request failed" }

var clickHouseIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)
var clickHouseParameterName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)
var clickHouseExceptionCode = regexp.MustCompile(`^Code:\s*([0-9]+)\b`)

func newClickHouse(c ClickHouseConfig) (*clickHouse, error) {
	if c.Database == "" {
		c.Database = "porcupine"
	}
	if c.User == "" {
		c.User = "default"
	}
	if err := normalizeStorageConfig(&c); err != nil {
		return nil, err
	}
	u, err := url.Parse(c.URL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || !clickHouseIdentifier.MatchString(c.Database) {
		return nil, errors.New("invalid ClickHouse configuration")
	}
	if strings.ContainsAny(c.User+c.Password, "\r\n") {
		return nil, errors.New("invalid ClickHouse configuration")
	}
	transport := &http.Transport{
		Proxy:                  http.ProxyFromEnvironment,
		DialContext:            (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           8,
		MaxIdleConnsPerHost:    4,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    5 * time.Second,
		ResponseHeaderTimeout:  10 * time.Second,
		MaxResponseHeaderBytes: 16 << 10,
	}
	return &clickHouse{config: c, url: u, client: &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}, nil
}

func (c *clickHouse) admin(ctx context.Context, sql string) ([]byte, error) {
	return c.request(ctx, c.config.User, c.config.Password, "", nil, strings.NewReader(sql), 64<<10)
}

func (c *clickHouse) query(ctx context.Context, user, password, database, sql string, parameters map[string]string) ([]byte, error) {
	if database != "" && !clickHouseIdentifier.MatchString(database) {
		return nil, errors.New("invalid ClickHouse database")
	}
	values := make(url.Values, len(parameters)+1)
	values.Set("default_format", "JSON")
	for name, value := range parameters {
		if !clickHouseParameterName.MatchString(name) {
			return nil, errors.New("invalid ClickHouse parameter name")
		}
		values.Set("param_"+name, value)
	}
	return c.request(ctx, user, password, database, values, strings.NewReader(sql), 2<<20)
}

func (c *clickHouse) request(ctx context.Context, user, password, database string, parameters url.Values, body io.Reader, limit int64) ([]byte, error) {
	u := *c.url
	if parameters == nil {
		parameters = make(url.Values)
	}
	parameters.Set("wait_end_of_query", "1")
	if database != "" {
		parameters.Set("database", database)
	}
	u.RawQuery = parameters.Encode()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, &clickHouseError{}
	}
	r.Header.Set("X-ClickHouse-User", user)
	r.Header.Set("X-ClickHouse-Key", password)
	r.Header.Set("Content-Type", "text/plain; charset=utf-8")
	response, err := c.client.Do(r)
	if err != nil {
		return nil, &clickHouseError{}
	}
	defer response.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(response.Body, limit+1))
	code, _ := strconv.Atoi(response.Header.Get("X-ClickHouse-Exception-Code"))
	if code == 0 {
		if matches := clickHouseExceptionCode.FindSubmatch(data); len(matches) == 2 {
			code, _ = strconv.Atoi(string(matches[1]))
		}
	}
	if readErr != nil || int64(len(data)) > limit || response.StatusCode < 200 || response.StatusCode >= 300 || code != 0 {
		return nil, &clickHouseError{Code: code, Status: response.StatusCode}
	}
	return data, nil
}
