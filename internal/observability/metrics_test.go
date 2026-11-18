package observability

import (
	"strings"
	"testing"
	"time"
)

func TestPrometheusTextMetrics(t *testing.T) {
	t.Parallel()
	r := NewRegistry("standalone")
	r.Add("requests_total", 2, Label{Name: "outcome", Value: "ok"})
	r.Add("requests_total", 1, Label{Name: "outcome", Value: "ok"})
	r.ObserveDuration("elapsed_seconds", time.Millisecond)
	r.ObserveDuration("elapsed_seconds", 123*time.Second)
	var out strings.Builder
	r.Write(&out, []Gauge{{Name: "depth", Value: 12}})
	for _, line := range []string{
		`# TYPE requests_total counter`,
		`requests_total{profile="standalone",outcome="ok"} 3`,
		`# TYPE depth gauge`,
		`depth{profile="standalone"} 12`,
		`# TYPE elapsed_seconds histogram`,
		`elapsed_seconds_bucket{profile="standalone",le="0.001"} 1`,
		`elapsed_seconds_bucket{profile="standalone",le="120"} 1`,
		`elapsed_seconds_bucket{profile="standalone",le="+Inf"} 2`,
		`elapsed_seconds_sum{profile="standalone"} 123.001`,
		`elapsed_seconds_count{profile="standalone"} 2`,
	} {
		if !strings.Contains(out.String(), line+"\n") {
			t.Errorf("missing sample %s in:\n%s", line, out.String())
		}
	}
}
