package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"
)

const (
	probeEventName     = "porcupine_visibility_probe"
	probeSQL           = "SELECT message_id FROM events WHERE event = $event AND message_id = $message_id LIMIT 1"
	probeResponseLimit = 16 << 10
)

type probeOutcome int

const (
	probeVisible probeOutcome = iota
	probeVisibilityTimeout
	probeCancelled
	probeIngestError
	probeQueryError
	probeOutcomeCount
)

var probeOutcomes = [probeOutcomeCount]string{"visible", "visibility_timeout", "cancelled", "ingest_error", "query_error"}

type probeResult struct {
	outcome probeOutcome
	elapsed time.Duration
	err     error
}

func (r probeResult) report() string {
	if r.err != nil {
		return fmt.Sprintf("visibility probe %s (%s)", probeOutcomes[r.outcome], r.err)
	}
	return fmt.Sprintf("visibility probe visible after %s", r.elapsed)
}

type visibilityProbe struct {
	client            *http.Client
	ingestURL         string
	queryURL          string
	ingestToken       string
	queryToken        string
	pollInterval      time.Duration
	visibilityTimeout time.Duration
}

func visibilityProbeCommand() *cli.Command {
	return &cli.Command{
		Name:        "visibility-probe",
		Usage:       "Measure the time between native ingestion acknowledgment and SQL visibility",
		Description: "Use a dedicated project and set PORCUPINE_PROBE_INGEST_TOKEN and PORCUPINE_PROBE_QUERY_TOKEN to its API bearer keys.",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "ingest-url", Required: true, Usage: "Ingestion worker origin"},
			&cli.StringFlag{Name: "query-url", Usage: "Query worker origin, defaults to --ingest-url"},
			&cli.StringFlag{Name: "project", Required: true, Usage: "Dedicated probe project ID"},
			&cli.DurationFlag{Name: "interval", Usage: "Delay between runs, or 0 for a single run without a metrics listener"},
			&cli.DurationFlag{Name: "poll-interval", Value: 250 * time.Millisecond, Usage: "Delay between SQL polls"},
			&cli.DurationFlag{Name: "visibility-timeout", Value: 30 * time.Second, Usage: "Maximum wait for visibility after acknowledgment"},
			&cli.DurationFlag{Name: "request-timeout", Value: 5 * time.Second, Usage: "Maximum wait for an individual HTTP request"},
			&cli.StringFlag{Name: "metrics-listen", Value: "127.0.0.1:9465", Usage: "Prometheus listener IP and port, using a loopback or private address"},
		},
		Action: runVisibilityProbe,
	}
}

func runVisibilityProbe(ctx context.Context, c *cli.Command) error {
	if c.NArg() != 0 {
		return errors.New("unexpected positional arguments, see visibility-probe --help")
	}
	ingestOrigin, err := parseInstanceOrigin(c.String("ingest-url"))
	if err != nil {
		return errors.New("--ingest-url requires an HTTPS origin or an HTTP loopback origin")
	}
	queryOrigin := ingestOrigin
	if raw := c.String("query-url"); raw != "" {
		queryOrigin, err = parseInstanceOrigin(raw)
		if err != nil {
			return errors.New("--query-url requires an HTTPS origin or an HTTP loopback origin")
		}
	}
	project := c.String("project")
	if !importPathSegment(project) || strings.TrimSpace(project) != project || project == "*" {
		return errors.New("--project requires a dedicated project ID")
	}
	ingestToken := os.Getenv("PORCUPINE_PROBE_INGEST_TOKEN")
	queryToken := os.Getenv("PORCUPINE_PROBE_QUERY_TOKEN")
	if ingestToken == "" || queryToken == "" {
		return errors.New("set PORCUPINE_PROBE_INGEST_TOKEN and PORCUPINE_PROBE_QUERY_TOKEN for the dedicated project")
	}
	interval := c.Duration("interval")
	pollInterval := c.Duration("poll-interval")
	visibilityTimeout := c.Duration("visibility-timeout")
	requestTimeout := c.Duration("request-timeout")
	if interval < 0 || pollInterval <= 0 || visibilityTimeout <= 0 || requestTimeout <= 0 {
		return errors.New("use positive durations for --poll-interval, --visibility-timeout and --request-timeout, and a nonnegative --interval")
	}

	p := visibilityProbe{
		client:            newProbeHTTPClient(requestTimeout),
		ingestURL:         ingestOrigin + "/ingest/v1/events",
		queryURL:          queryOrigin + "/api/v1/projects/" + url.PathEscape(project) + "/query",
		ingestToken:       ingestToken,
		queryToken:        queryToken,
		pollInterval:      pollInterval,
		visibilityTimeout: visibilityTimeout,
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM)
	defer stop()
	defer p.client.CloseIdleConnections()
	if interval == 0 {
		result := p.run(ctx)
		if result.err != nil {
			return errors.New(result.report())
		}
		fmt.Fprintln(c.Root().Writer, result.report())
		return nil
	}
	address := c.String("metrics-listen")
	if err := validateProbeMetricsAddress(address); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("bind probe metrics listener: %w", err)
	}
	return p.runPeriodic(ctx, interval, listener, c.Root().ErrWriter)
}

func newProbeHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func (p visibilityProbe) run(ctx context.Context) probeResult {
	if err := ctx.Err(); err != nil {
		return probeResult{outcome: probeCancelled, err: err}
	}
	var nonce [16]byte
	_, _ = rand.Read(nonce[:])
	messageID := hex.EncodeToString(nonce[:])
	event, _ := json.Marshal(map[string]string{"type": "track", "message_id": messageID, "name": probeEventName})
	var ack struct {
		Status   int `json:"status"`
		Receipts []struct {
			MessageID string `json:"message_id"`
		} `json:"receipts"`
	}
	if err := p.post(ctx, p.ingestURL, p.ingestToken, event, &ack); err != nil {
		if ctx.Err() != nil {
			return probeResult{outcome: probeCancelled, err: ctx.Err()}
		}
		return probeResult{outcome: probeIngestError, err: err}
	}
	ackTime := time.Now()
	if ack.Status != 1 || len(ack.Receipts) != 1 || ack.Receipts[0].MessageID != messageID {
		return probeResult{outcome: probeIngestError, err: errors.New("native acknowledgment did not include the submitted message")}
	}

	query, _ := json.Marshal(struct {
		SQL        string            `json:"sql"`
		Parameters map[string]string `json:"parameters"`
	}{SQL: probeSQL, Parameters: map[string]string{"event": probeEventName, "message_id": messageID}})
	pollCtx, cancel := context.WithDeadline(ctx, ackTime.Add(p.visibilityTimeout))
	defer cancel()
	for pollCtx.Err() == nil {
		var result struct {
			Result struct {
				Data []struct {
					MessageID string `json:"message_id"`
				} `json:"data"`
			} `json:"result"`
		}
		err := p.post(pollCtx, p.queryURL, p.queryToken, query, &result)
		elapsed := time.Since(ackTime)
		if elapsed >= p.visibilityTimeout || pollCtx.Err() != nil {
			break
		}
		if err != nil {
			return probeResult{outcome: probeQueryError, err: err}
		}
		if len(result.Result.Data) == 1 && result.Result.Data[0].MessageID == messageID {
			return probeResult{outcome: probeVisible, elapsed: elapsed}
		}
		if result.Result.Data == nil || len(result.Result.Data) != 0 {
			return probeResult{outcome: probeQueryError, err: errors.New("SQL response did not match the visibility query")}
		}
		if !waitForProbe(pollCtx, p.pollInterval) {
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return probeResult{outcome: probeCancelled, err: err}
	}
	return probeResult{outcome: probeVisibilityTimeout, err: errors.New("deadline expired before SQL returned the event")}
}

func (p visibilityProbe) post(ctx context.Context, endpoint, token string, body []byte, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("could not construct HTTP request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := p.client.Do(req)
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("HTTP request timed out")
	}
	if err != nil {
		return errors.New("HTTP request failed")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP status %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, probeResponseLimit+1))
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("HTTP request timed out")
	}
	if err != nil || len(data) > probeResponseLimit || json.Unmarshal(data, dst) != nil {
		return errors.New("invalid HTTP response")
	}
	return nil
}

func (p visibilityProbe) runPeriodic(ctx context.Context, interval time.Duration, listener net.Listener, log io.Writer) error {
	metrics := &visibilityProbeMetrics{}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	defer server.Close()
	ctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	go func() {
		if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			stop(fmt.Errorf("serve probe metrics: %w", err))
		}
	}()
	for ctx.Err() == nil {
		result := p.run(ctx)
		metrics.observe(result)
		fmt.Fprintln(log, result.report())
		if !waitForProbe(ctx, interval) {
			break
		}
	}
	if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
		return cause
	}
	return nil
}

func waitForProbe(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
