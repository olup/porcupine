package porcupine

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/observability"
)

type operationalMetrics struct {
	*observability.Registry
	journal      atomic.Pointer[journalQueueSample]
	stopSampling context.CancelFunc
	samplingDone chan struct{}
}

type journalQueueSample struct {
	events, bytes int64
	oldest, at    time.Time
}

func (s *Service) startQueueSampling() {
	m := s.metrics
	if m == nil || s.db == nil {
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	m.stopSampling = stop
	m.samplingDone = make(chan struct{})
	go func() {
		defer close(m.samplingDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for ctx.Err() == nil {
			s.sampleJournalQueue(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *Service) sampleJournalQueue(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	var sample journalQueueSample
	var oldest string
	err := s.db.QueryRowContext(ctx, `SELECT pending_count,pending_bytes,
COALESCE((SELECT received_at FROM events WHERE delivered=0 ORDER BY id LIMIT 1),'')
FROM queue_usage WHERE id=1`).Scan(&sample.events, &sample.bytes, &oldest)
	if err != nil {
		return
	}
	if oldest != "" {
		sample.oldest, err = time.Parse(time.RFC3339Nano, oldest)
		if err != nil {
			return
		}
	}
	sample.at = time.Now()
	s.metrics.journal.Store(&sample)
}

// MetricsHandler is served only on the dedicated listener.
func (s *Service) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.metrics == nil || r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", observability.ContentType)
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == http.MethodGet {
			s.metrics.Write(w, s.metricGauges())
		}
	})
}

func (s *Service) metricGauges() []observability.Gauge {
	var gauges []observability.Gauge
	add := func(name string, value float64) {
		gauges = append(gauges, observability.Gauge{Name: "porcupine_" + name, Value: value})
	}
	var events, bytes int64
	var oldest time.Time
	eventCapacity, byteCapacity := s.queueMax, s.queueMaxBytes
	var sampled bool
	if s.hosted != nil {
		snapshot := s.hosted.buffer.Snapshot()
		events, bytes, oldest = int64(snapshot.Events), snapshot.Bytes, snapshot.Oldest
		eventCapacity, byteCapacity = snapshot.CapacityEvents, snapshot.CapacityBytes
		add("flush_inflight", float64(snapshot.Inflight))
		sampled = true
	} else if sample := s.metrics.journal.Load(); sample != nil {
		events, bytes, oldest = sample.events, sample.bytes, sample.oldest
		add("queue_sample_timestamp_seconds", float64(sample.at.Unix()))
		sampled = true
	} else {
		add("queue_sample_timestamp_seconds", 0)
	}
	if sampled {
		add("queue_events", float64(events))
		add("queue_bytes", float64(bytes))
		var age float64
		if !oldest.IsZero() {
			age = max(0, s.clock().Sub(oldest).Seconds())
		}
		add("queue_oldest_seconds", age)
	}
	add("queue_capacity_events", float64(eventCapacity))
	add("queue_capacity_bytes", float64(byteCapacity))
	return s.queries.AppendGauges(gauges)
}

type httpObservation struct {
	operation string
}

type httpObservationKey struct{}

type observedResponse struct {
	http.ResponseWriter
	status    int
	rejection string
}

func markIngestionRejection(w http.ResponseWriter, reason string) {
	if observed, ok := w.(*observedResponse); ok {
		observed.rejection = reason
	}
}

func ingestionProblem(w http.ResponseWriter, reason, message string) {
	markIngestionRejection(w, reason)
	problem(w, http.StatusBadRequest, message)
}

func (w *observedResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *observedResponse) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.ResponseWriter.WriteHeader(status)
	if status >= 200 || status == http.StatusSwitchingProtocols {
		w.status = status
	}
}

func (w *observedResponse) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *observedResponse) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func requestGroup(path string, captureHeaders map[string][]string) string {
	if _, capture := captureHeaders[path]; capture {
		return "ingestion"
	}
	switch path {
	case "/healthz", "/readyz":
		return "health"
	case "/mcp":
		return "mcp"
	case "/openapi.json", "/openapi.yaml", "/openapi-3.0.json", "/openapi-3.0.yaml", "/ingest/v1/schema":
		return "metadata"
	}
	switch {
	case strings.HasPrefix(path, "/api/v1/"):
		return "management"
	case strings.HasPrefix(path, "/oauth/"), strings.HasPrefix(path, "/.well-known/"):
		return "oauth"
	default:
		return "other"
	}
}

func (s *Service) observeHTTP(captureHeaders map[string][]string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		group := requestGroup(r.URL.Path, captureHeaders)
		observation := httpObservation{}
		if group == "management" {
			r = r.WithContext(context.WithValue(r.Context(), httpObservationKey{}, &observation))
		}
		out := &observedResponse{ResponseWriter: w}
		next.ServeHTTP(out, r)
		status := out.status
		if status == 0 {
			status = http.StatusOK
		}
		elapsed := time.Since(started)
		if m := s.metrics; m != nil {
			route := observability.Label{Name: "route", Value: group}
			m.Add("porcupine_http_requests_total", 1, route, observability.Label{Name: "status_class", Value: statusClass(status)})
			m.ObserveDuration("porcupine_http_request_duration_seconds", elapsed, route)
			if reason := ingestionRejection(status, out.rejection); group == "ingestion" && r.Method == http.MethodPost && reason != "" {
				m.Add("porcupine_ingestion_rejected_total", 1,
					observability.Label{Name: "route", Value: r.URL.Path}, observability.Label{Name: "reason", Value: reason})
			}
		}
		if observation.operation != "" {
			s.observeOperation(r.Context(), "http", observation.operation, status, elapsed)
		} else if status >= 500 {
			slog.Warn("HTTP request failed", "route", group, "status", status, "duration_ms", elapsed.Milliseconds())
		}
	})
}

var ingestionRejectionReasons = map[int]string{
	400: "invalid_body", 401: "invalid_credential", 403: "forbidden",
	404: "unavailable", 413: "body_too_large", 415: "unsupported_encoding", 429: "unavailable",
}

func ingestionRejection(status int, reason string) string {
	if reason != "" {
		return reason
	}
	if status >= 500 {
		return "unavailable"
	}
	return ingestionRejectionReasons[status]
}

func statusClass(status int) string {
	switch status / 100 {
	case 2:
		return "2xx"
	case 3:
		return "3xx"
	case 4:
		return "4xx"
	case 5:
		return "5xx"
	default:
		return "other"
	}
}

func (s *Service) observeOperation(ctx context.Context, transport, operation string, status int, elapsed time.Duration) {
	outcome := "ok"
	switch {
	case status >= 400 && ctx.Err() != nil:
		outcome = "cancelled"
	case status >= 500:
		outcome = "error"
	case status >= 400:
		outcome = "rejected"
	}
	if m := s.metrics; m != nil {
		m.Add("porcupine_operations_total", 1,
			observability.Label{Name: "transport", Value: transport},
			observability.Label{Name: "operation", Value: operation},
			observability.Label{Name: "outcome", Value: outcome})
	}
	if outcome == "error" {
		slog.Warn("operation failed", "transport", transport, "operation", operation, "status", status, "duration_ms", elapsed.Milliseconds())
	}
}

func (s *Service) observeAcceptance(ctx context.Context, result acceptance.Result, err error) {
	m := s.metrics
	if m == nil {
		return
	}
	durability := observability.Label{Name: "durability", Value: string(s.acceptance.Durability())}
	outcome := "accepted"
	if err != nil {
		var rejected *apierr.Error
		var failure *acceptance.Failure
		var budget *tenantIngestionError
		switch {
		case ctx.Err() != nil:
			outcome = "cancelled"
		case errors.As(err, &failure) && failure.Pressure:
			outcome = "pressure"
			if errors.Is(err, errProjectQueueFull) {
				m.Add("porcupine_ingestion_project_queue_rejections_total", 1)
			}
		case errors.As(err, &budget), errors.As(err, &rejected) && rejected.GetStatus() < 500:
			outcome = "rejected"
		default:
			outcome = "error"
		}
	}
	m.Add("porcupine_ingestion_admissions_total", 1, durability, observability.Label{Name: "outcome", Value: outcome})
	switch result := result.(type) {
	case acceptance.DurableResult:
		m.observeIngestionEvents(acceptance.Durable, "accepted", uint64(result.Accepted))
		m.observeIngestionEvents(acceptance.Durable, "duplicate", uint64(result.Duplicates))
		m.observeIngestionEvents(acceptance.Durable, "identity_ignored", uint64(result.IdentityIgnored))
	case acceptance.BufferedResult:
		m.observeIngestionEvents(acceptance.Buffered, "buffered", uint64(result.Received))
	}
}

func (m *operationalMetrics) observeIngestionEvents(durability acceptance.Durability, outcome string, count uint64) {
	m.Add("porcupine_ingestion_events_total", count,
		observability.Label{Name: "durability", Value: string(durability)},
		observability.Label{Name: "outcome", Value: outcome})
}

func (m *operationalMetrics) observeTimestampAdjustment(reason string) {
	m.Add("porcupine_ingestion_timestamp_adjusted_total", 1, observability.Label{Name: "reason", Value: reason})
}
