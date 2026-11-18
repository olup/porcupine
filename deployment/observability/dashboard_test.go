package observability_test

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

// Metrics exposed to this dashboard, with the labels specific to each family.
// Prometheus adds job and instance, and the server adds profile.
var dashboardMetrics = map[string]string{
	"porcupine_http_requests_total":                            "route status_class",
	"porcupine_http_request_duration_seconds_bucket":           "route le",
	"porcupine_operations_total":                               "transport operation outcome",
	"porcupine_ingestion_admissions_total":                     "durability outcome",
	"porcupine_ingestion_events_total":                         "durability outcome",
	"porcupine_queue_events":                                   "",
	"porcupine_queue_bytes":                                    "",
	"porcupine_queue_capacity_events":                          "",
	"porcupine_queue_capacity_bytes":                           "",
	"porcupine_queue_oldest_seconds":                           "",
	"porcupine_queue_sample_timestamp_seconds":                 "",
	"porcupine_flush_attempts_total":                           "outcome",
	"porcupine_flush_duration_seconds_bucket":                  "le",
	"porcupine_flush_dropped_events_total":                     "reason",
	"porcupine_flush_inflight":                                 "",
	"porcupine_query_admissions_total":                         "operation outcome",
	"porcupine_query_admission_duration_seconds_bucket":        "operation le",
	"porcupine_query_backend_duration_seconds_bucket":          "operation outcome le",
	"porcupine_query_active":                                   "operation",
	"porcupine_query_slots_used":                               "",
	"porcupine_query_slots_capacity":                           "",
	"porcupine_visibility_probe_runs_total":                    "outcome",
	"porcupine_visibility_probe_ack_to_visible_seconds_bucket": "le",
}

var (
	metricSelector = regexp.MustCompile(`([a-z_][a-z_0-9]*)\{([^{}]*)\}`)
	labelMatcher   = regexp.MustCompile(`([a-z_][a-z_0-9]*)\s*(=~|!~|!=|=)\s*("(?:\\.|[^"\\])*")`)
	groupLabels    = regexp.MustCompile(`\bsum\s+by\s*\(([^)]*)\)`)
	legendLabel    = regexp.MustCompile(`\{\{([a-z_][a-z_0-9]*)\}\}`)
	percentile     = regexp.MustCompile(`^histogram_quantile\(\s*0\.[0-9]+\s*,\s*sum\s+by\s*\([^)]*\)\s*\(\s*rate\(\s*porcupine_[a-z_]+_bucket\{[^{}]*\}\[\$__rate_interval\]\s*\)\s*\)\s*\)$`)
)

type dashboardSource struct {
	Type string
	UID  string
}

func TestDashboardPrometheusContract(t *testing.T) {
	t.Parallel()

	var dashboard struct {
		Inputs []struct {
			Name     string
			Type     string
			PluginID string `json:"pluginId"`
		} `json:"__inputs"`
		Title         string
		UID           string
		SchemaVersion int
		Templating    struct {
			List []struct {
				Name       string
				Datasource dashboardSource
			}
		}
		Panels []struct {
			ID         int
			Type       string
			Title      string
			Datasource dashboardSource
			Targets    []struct {
				Expr         string
				LegendFormat string
				Datasource   *dashboardSource
			}
		}
	}
	raw, err := os.ReadFile("porcupine-operator.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &dashboard); err != nil {
		t.Fatal(err)
	}
	if dashboard.Title == "" || dashboard.UID == "" || dashboard.SchemaVersion < 1 {
		t.Fatal("dashboard needs a title, UID, and schema version")
	}
	if len(dashboard.Inputs) != 1 {
		t.Fatal("dashboard needs one Prometheus import input")
	}
	input := dashboard.Inputs[0]
	if input.Name == "" || input.Type != "datasource" || input.PluginID != "prometheus" {
		t.Fatalf("invalid Prometheus input: %+v", input)
	}
	source := dashboardSource{Type: "prometheus", UID: "${" + input.Name + "}"}
	variables := make(map[string]bool)
	for _, variable := range dashboard.Templating.List {
		variables[variable.Name] = true
		if variable.Name == "worker" && variable.Datasource != source {
			t.Errorf("worker variable does not use the import input")
		}
	}
	for _, name := range []string{"server_job", "worker", "probe_job"} {
		if !variables[name] {
			t.Errorf("missing dashboard variable %s", name)
		}
	}

	seen := make(map[string]bool)
	panelIDs := make(map[int]bool)
	for _, panel := range dashboard.Panels {
		if panel.ID < 1 || panelIDs[panel.ID] || panel.Title == "" {
			t.Errorf("invalid panel %d %q", panel.ID, panel.Title)
		}
		panelIDs[panel.ID] = true
		if panel.Type == "row" {
			continue
		}
		if panel.Datasource != source || len(panel.Targets) == 0 {
			t.Errorf("%s: expected Prometheus import input and queries", panel.Title)
		}
		for _, target := range panel.Targets {
			if target.Datasource != nil && *target.Datasource != source {
				t.Errorf("%s: query overrides the import input", panel.Title)
			}
			for _, name := range checkDashboardQuery(t, panel.Title, target.Expr, target.LegendFormat) {
				seen[name] = true
			}
		}
	}
	for name := range dashboardMetrics {
		if !seen[name] {
			t.Errorf("no dashboard query uses %s", name)
		}
	}
}

func checkDashboardQuery(t *testing.T, panel, expr, legend string) []string {
	t.Helper()
	var names []string
	var grouping []string
	if group := groupLabels.FindStringSubmatch(expr); group != nil {
		grouping = strings.Fields(strings.ReplaceAll(group[1], ",", " "))
	}
	for _, selector := range metricSelector.FindAllStringSubmatch(expr, -1) {
		name := selector[1]
		labels, known := dashboardMetrics[name]
		if name != "up" && !known {
			t.Errorf("%s: unknown metric %s", panel, name)
			continue
		}
		names = append(names, name)
		probeMetric := strings.HasPrefix(name, "porcupine_visibility_probe_")
		allowed := map[string]bool{"instance": true, "job": true}
		if name != "up" && !probeMetric {
			allowed["profile"] = true
		}
		for label := range strings.FieldsSeq(labels) {
			allowed[label] = true
		}
		matches := make(map[string]string)
		for _, matcher := range labelMatcher.FindAllStringSubmatch(selector[2], -1) {
			if !allowed[matcher[1]] {
				t.Errorf("%s: %s has no label %s", panel, name, matcher[1])
			}
			matches[matcher[1]] = matcher[2] + matcher[3]
		}
		probe := probeMetric || name == "up" && matches["job"] == `="$probe_job"`
		if probe {
			if matches["job"] != `="$probe_job"` {
				t.Errorf("%s: %s should use the probe scrape job", panel, name)
			}
		} else if matches["job"] != `="$server_job"` || matches["instance"] != `=~"$worker"` {
			t.Errorf("%s: %s should filter by server job and worker", panel, name)
		}
		for _, label := range grouping {
			if !allowed[label] {
				t.Errorf("%s: cannot group %s by %s", panel, name, label)
			}
		}
		legendLabels := allowed
		if grouping != nil {
			legendLabels = make(map[string]bool)
			for _, label := range grouping {
				legendLabels[label] = true
			}
		}
		bucket := strings.HasSuffix(name, "_bucket")
		if bucket {
			if !percentile.MatchString(expr) || !legendLabels["le"] {
				t.Errorf("%s: compute percentiles by summing per-target bucket rates and retaining le", panel)
			}
			if probeMetric && !legendLabels["instance"] || !probeMetric && !legendLabels["profile"] {
				t.Errorf("%s: retain the server profile or probe instance in percentile groups", panel)
			}
		}
		for _, label := range legendLabel.FindAllStringSubmatch(legend, -1) {
			if !legendLabels[label[1]] || bucket && label[1] == "le" {
				t.Errorf("%s: query for %s does not return legend label %s", panel, name, label[1])
			}
		}
	}
	if len(names) == 0 {
		t.Errorf("%s: query has no known metric selector", panel)
	}
	return names
}
