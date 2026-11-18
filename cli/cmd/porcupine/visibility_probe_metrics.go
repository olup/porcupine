package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
)

const (
	probeRunsMetric    = "porcupine_visibility_probe_runs_total"
	probeLatencyMetric = "porcupine_visibility_probe_ack_to_visible_seconds"
)

var probeLatencyBounds = [...]float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}

func validateProbeMetricsAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return errors.New("--metrics-listen requires an IP address and port")
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" || !ip.Unmap().IsLoopback() && !ip.Unmap().IsPrivate() {
		return errors.New("--metrics-listen must bind to a loopback or private IP address")
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return errors.New("--metrics-listen port must be between 0 and 65535")
	}
	return nil
}

type visibilityProbeCounters struct {
	runs    [probeOutcomeCount]uint64
	buckets [len(probeLatencyBounds)]uint64
	sum     float64
}

type visibilityProbeMetrics struct {
	mu       sync.Mutex
	counters visibilityProbeCounters
}

func (m *visibilityProbeMetrics) observe(result probeResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters.runs[result.outcome]++
	if result.outcome == probeVisible {
		seconds := result.elapsed.Seconds()
		for i, bound := range probeLatencyBounds {
			if seconds <= bound {
				m.counters.buckets[i]++
			}
		}
		m.counters.sum += seconds
	}
}

func (m *visibilityProbeMetrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	c := m.counters
	m.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# HELP %s Visibility probe runs by outcome.\n# TYPE %s counter\n", probeRunsMetric, probeRunsMetric)
	for i, outcome := range probeOutcomes {
		fmt.Fprintf(w, "%s{outcome=%q} %d\n", probeRunsMetric, outcome, c.runs[i])
	}
	fmt.Fprintf(w, "# HELP %s Time from native ingestion acknowledgment to SQL visibility for successful runs.\n# TYPE %s histogram\n", probeLatencyMetric, probeLatencyMetric)
	for i, bound := range probeLatencyBounds {
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", probeLatencyMetric, bound, c.buckets[i])
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", probeLatencyMetric, c.runs[probeVisible])
	fmt.Fprintf(w, "%s_sum %g\n%s_count %d\n", probeLatencyMetric, c.sum, probeLatencyMetric, c.runs[probeVisible])
}
