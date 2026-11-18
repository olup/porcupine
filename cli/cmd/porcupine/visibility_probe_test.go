package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	probeTestIngestPath  = "/ingest/v1/events"
	probeTestQueryPath   = "/api/v1/projects/dedicated-probe/query"
	probeTestIngestToken = "writer-secret"
	probeTestQueryToken  = "reader-secret"
)

func TestVisibilityProbeCommandRegistered(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	app := newApp()
	app.Writer = &output
	app.ErrWriter = io.Discard
	if err := app.Run(t.Context(), []string{"porcupine", "visibility-probe", "--help"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"--ingest-url", "--query-url", "--project", "--metrics-listen", "127.0.0.1:9465"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("help missing %q", want)
		}
	}
}

func TestVisibilityProbeUsesSeparateWorkersAndBoundMarker(t *testing.T) {
	t.Parallel()
	var polls atomic.Int64
	var marker atomic.Value
	p := newTestVisibilityProbe(t, func(w http.ResponseWriter, r *http.Request) {
		marker.Store(ackTestVisibilityEvent(t, w, r))
	}, func(w http.ResponseWriter, r *http.Request) {
		checkProbeHTTPRequest(t, r, probeTestQueryPath, probeTestQueryToken)
		var query struct {
			SQL        string            `json:"sql"`
			Parameters map[string]string `json:"parameters"`
		}
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			t.Errorf("decode SQL request: %v", err)
			return
		}
		messageID := marker.Load().(string)
		if query.SQL != probeSQL || strings.Contains(query.SQL, messageID) || query.Parameters["message_id"] != messageID || query.Parameters["event"] != probeEventName {
			t.Errorf("unexpected SQL parameters: %+v", query)
		}
		if polls.Add(1)%2 == 1 {
			fmt.Fprint(w, `{"result":{"data":[]}}`)
			return
		}
		fmt.Fprintf(w, `{"result":{"data":[{"message_id":%q}]}}`, messageID)
	})
	first := p.run(t.Context())
	firstID := marker.Load()
	second := p.run(t.Context())
	if first.outcome != probeVisible || second.outcome != probeVisible || first.elapsed < p.pollInterval || second.elapsed < p.pollInterval {
		t.Fatalf("probe results: %+v, %+v", first, second)
	}
	if polls.Load() != 4 || marker.Load() == firstID {
		t.Fatal("each run should issue a new marker and wait for the matching row")
	}
}

func TestVisibilityProbeOutcomes(t *testing.T) {
	t.Parallel()
	ack := func(w http.ResponseWriter, r *http.Request) { ackTestVisibilityEvent(t, w, r) }
	empty := func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"result":{"data":[]}}`) }
	reject := func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, r.Header.Get("Authorization"), http.StatusUnauthorized)
	}
	for _, tc := range []struct {
		name   string
		ingest http.HandlerFunc
		query  http.HandlerFunc
		want   probeOutcome
	}{
		{"ingest rejected", reject, nil, probeIngestError},
		{"incorrect receipt", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"status":1,"receipts":[{"message_id":"some-other-event"}]}`)
		}, nil, probeIngestError},
		{"query rejected", ack, reject, probeQueryError},
		{"missing result", ack, func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"result":{}}`)
		}, probeQueryError},
		{"incorrect row", ack, func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"result":{"data":[{"message_id":"some-other-event"}]}}`)
		}, probeQueryError},
		{"empty until deadline", ack, empty, probeVisibilityTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			query := tc.query
			if query == nil {
				query = func(w http.ResponseWriter, _ *http.Request) {
					t.Error("query must not run after failed ingestion")
					w.WriteHeader(http.StatusInternalServerError)
				}
			}
			p := newTestVisibilityProbe(t, tc.ingest, query)
			if tc.want == probeVisibilityTimeout {
				p.visibilityTimeout = 100 * time.Millisecond
			}
			result := p.run(t.Context())
			if result.outcome != tc.want || result.err == nil || result.elapsed != 0 {
				t.Fatalf("probe result %+v, want %s", result, probeOutcomes[tc.want])
			}
			if report := result.report(); strings.Contains(report, probeTestIngestToken) || strings.Contains(report, probeTestQueryToken) {
				t.Error("probe report contained a bearer token")
			}
		})
	}
}

func TestVisibilityProbeCancellation(t *testing.T) {
	t.Parallel()
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	p := newTestVisibilityProbe(t, func(w http.ResponseWriter, r *http.Request) {
		ackTestVisibilityEvent(t, w, r)
	}, func(w http.ResponseWriter, r *http.Request) {
		stop()
	})
	result := p.run(ctx)
	if result.outcome != probeCancelled {
		t.Fatalf("probe result %+v, want cancellation", result)
	}
}

func TestVisibilityProbeRefusesRedirects(t *testing.T) {
	t.Parallel()
	p := newTestVisibilityProbe(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/redirect-with-credentials")
		if r.URL.Path != probeTestIngestPath {
			t.Error("probe followed a redirect")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusTemporaryRedirect)
	}, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("probe queried after redirect")
		w.WriteHeader(http.StatusInternalServerError)
	})
	result := p.run(t.Context())
	if result.outcome != probeIngestError || result.err == nil || result.err.Error() != "HTTP status 307" {
		t.Fatalf("unexpected redirect result: %+v", result)
	}
}

func TestVisibilityProbeMetricsAddress(t *testing.T) {
	t.Parallel()
	for _, address := range []string{"127.0.0.1:9465", "[::1]:9465", "[::ffff:127.0.0.1]:9465", "10.1.2.3:0", "192.168.1.2:65535", "172.16.1.2:9465", "[fd01::2]:9465"} {
		if err := validateProbeMetricsAddress(address); err != nil {
			t.Errorf("rejects private address %q: %v", address, err)
		}
	}
	for _, address := range []string{
		"", "127.0.0.1", ":9465", "0.0.0.0:9465", "[::]:9465", "localhost:9465", "198.51.100.10:9465",
		"172.32.0.1:9465", "[::ffff:198.51.100.10]:9465", "[fe80::1]:9465", "[fd01::2%en0]:9465",
		"127.0.0.1:", "127.0.0.1:http", "127.0.0.1:-1", "127.0.0.1:65536",
	} {
		if err := validateProbeMetricsAddress(address); err == nil {
			t.Errorf("accepts unsafe or malformed address %q", address)
		}
	}
}

func TestVisibilityProbePeriodicMetrics(t *testing.T) {
	t.Parallel()
	p := newTestVisibilityProbe(t, func(w http.ResponseWriter, r *http.Request) {
		ackTestVisibilityEvent(t, w, r)
	}, func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			Parameters map[string]string `json:"parameters"`
		}
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			t.Errorf("decode query: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, `{"result":{"data":[{"message_id":%q}]}}`, query.Parameters["message_id"])
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- p.runPeriodic(ctx, time.Hour, listener, io.Discard) }()
	scrape, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.Addr().String()+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	for {
		res, err := p.client.Do(scrape)
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil || res.StatusCode != 200 || res.Header.Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
			t.Fatalf("failed to scrape metrics, status %d, error %v", res.StatusCode, err)
		}
		if bytes.Contains(content, []byte(`porcupine_visibility_probe_runs_total{outcome="visible"} 1`)) {
			for _, want := range []string{
				`porcupine_visibility_probe_runs_total{outcome="visibility_timeout"} 0`,
				`porcupine_visibility_probe_runs_total{outcome="cancelled"} 0`,
				`porcupine_visibility_probe_runs_total{outcome="ingest_error"} 0`,
				`porcupine_visibility_probe_runs_total{outcome="query_error"} 0`,
				`porcupine_visibility_probe_ack_to_visible_seconds_bucket{le="+Inf"} 1`,
				"porcupine_visibility_probe_ack_to_visible_seconds_count 1",
			} {
				if !bytes.Contains(content, []byte(want)) {
					t.Errorf("scrape missing %q", want)
				}
			}
			break
		}
		if !waitForProbe(ctx, 5*time.Millisecond) {
			t.Fatal("metrics did not report a successful probe")
		}
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func newTestVisibilityProbe(t *testing.T, ingest, query http.HandlerFunc) visibilityProbe {
	t.Helper()
	ingestServer := httptest.NewServer(ingest)
	t.Cleanup(ingestServer.Close)
	queryServer := httptest.NewServer(query)
	t.Cleanup(queryServer.Close)
	client := newProbeHTTPClient(time.Second)
	t.Cleanup(client.CloseIdleConnections)
	return visibilityProbe{
		client: client, ingestURL: ingestServer.URL + probeTestIngestPath, queryURL: queryServer.URL + probeTestQueryPath,
		ingestToken: probeTestIngestToken, queryToken: probeTestQueryToken,
		pollInterval: 5 * time.Millisecond, visibilityTimeout: time.Second,
	}
}

func checkProbeHTTPRequest(t *testing.T, r *http.Request, path, token string) {
	t.Helper()
	if r.Method != http.MethodPost || r.URL.Path != path || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("Content-Type") != "application/json" {
		t.Error("expected a JSON POST with the worker's bearer token and no URL parameters")
	}
}

func ackTestVisibilityEvent(t *testing.T, w http.ResponseWriter, r *http.Request) string {
	t.Helper()
	checkProbeHTTPRequest(t, r, probeTestIngestPath, probeTestIngestToken)
	var event map[string]string
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil || event["type"] != "track" || event["name"] != probeEventName || len(event["message_id"]) != 32 || len(event) != 3 {
		t.Errorf("unexpected native event: %v (%v)", event, err)
	}
	messageID := event["message_id"]
	fmt.Fprintf(w, `{"status":1,"durability":"buffered","receipts":[{"message_id":%q,"uuid":"event-uuid"}]}`, messageID)
	return messageID
}
