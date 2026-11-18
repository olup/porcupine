// Package observability writes process-local metrics in Prometheus text format.
package observability

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"
)

const ContentType = "text/plain; version=0.0.4; charset=utf-8"

var durationBuckets = [...]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}

// Metric names and labels must come from fixed sets at the call site.
type series struct {
	name, labels string
}

type histogram struct {
	bins  [len(durationBuckets) + 1]uint64
	count uint64
	sum   float64
}

type Registry struct {
	mu        sync.Mutex
	profile   string
	counters  map[series]uint64
	durations map[series]histogram
}

func NewRegistry(profile string) *Registry {
	return &Registry{profile: profile, counters: make(map[series]uint64), durations: make(map[series]histogram)}
}

func (r *Registry) Add(name string, count uint64, labels ...Label) {
	key := series{name, r.labels(labels)}
	r.mu.Lock()
	r.counters[key] += count
	r.mu.Unlock()
}

func (r *Registry) ObserveDuration(name string, elapsed time.Duration, labels ...Label) {
	key := series{name, r.labels(labels)}
	seconds := max(0, elapsed.Seconds())
	index, _ := slices.BinarySearch(durationBuckets[:], seconds)
	r.mu.Lock()
	value := r.durations[key]
	value.bins[index]++
	value.count++
	value.sum += seconds
	r.durations[key] = value
	r.mu.Unlock()
}

// ObserveFlush records one standalone delivery or hosted flush attempt.
func (r *Registry) ObserveFlush(outcome string, elapsed time.Duration) {
	r.Add("porcupine_flush_attempts_total", 1, Label{Name: "outcome", Value: outcome})
	r.ObserveDuration("porcupine_flush_duration_seconds", elapsed)
}

type Label struct{ Name, Value string }

type Gauge struct {
	Name   string
	Value  float64
	Labels []Label
}

var labelEscapes = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func (r *Registry) labels(labels []Label) string {
	var list strings.Builder
	fmt.Fprintf(&list, `profile="%s"`, labelEscapes.Replace(r.profile))
	for _, label := range labels {
		fmt.Fprintf(&list, `,%s="%s"`, label.Name, labelEscapes.Replace(label.Value))
	}
	return list.String()
}

type sample struct {
	series
	kind   string
	number uint64
	gauge  float64
	histogram
}

func (r *Registry) Write(w io.Writer, gauges []Gauge) {
	r.mu.Lock()
	samples := make([]sample, 0, len(r.counters)+len(r.durations)+len(gauges))
	for key, value := range r.counters {
		samples = append(samples, sample{series: key, kind: "counter", number: value})
	}
	for key, value := range r.durations {
		samples = append(samples, sample{series: key, kind: "histogram", histogram: value})
	}
	r.mu.Unlock()
	for _, gauge := range gauges {
		samples = append(samples, sample{series: series{gauge.Name, r.labels(gauge.Labels)}, kind: "gauge", gauge: gauge.Value})
	}
	slices.SortFunc(samples, func(a, b sample) int {
		if order := cmp.Compare(a.name, b.name); order != 0 {
			return order
		}
		return cmp.Compare(a.labels, b.labels)
	})
	previous := ""
	for _, s := range samples {
		if s.name != previous {
			fmt.Fprintf(w, "# TYPE %s %s\n", s.name, s.kind)
			previous = s.name
		}
		switch s.kind {
		case "counter":
			fmt.Fprintf(w, "%s{%s} %d\n", s.name, s.labels, s.number)
		case "gauge":
			fmt.Fprintf(w, "%s{%s} %g\n", s.name, s.labels, s.gauge)
		case "histogram":
			var n uint64
			for i, boundary := range durationBuckets {
				n += s.bins[i]
				fmt.Fprintf(w, "%s_bucket{%s,le=\"%g\"} %d\n", s.name, s.labels, boundary, n)
			}
			fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %d\n", s.name, s.labels, s.count)
			fmt.Fprintf(w, "%s_sum{%s} %g\n", s.name, s.labels, s.sum)
			fmt.Fprintf(w, "%s_count{%s} %d\n", s.name, s.labels, s.count)
		}
	}
}
