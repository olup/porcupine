// Package usage records technical consumption independently of prices or plans.
package usage

import (
	"errors"
	"math"
	"strconv"
	"time"
)

const (
	Version             = 1
	SnapshotTTL         = 15 * time.Minute
	MaxPeriod           = 366 * 24 * time.Hour
	MaxRows             = 100000
	MaxBytes            = 32 << 20
	PageDefault         = 100
	PageMax             = 1000
	SampleInterval      = time.Hour
	SampleStale         = 2 * time.Hour
	ObservationInterval = 10 * time.Second
)

var (
	ErrConflict = errors.New("usage source revision conflicts with retained totals")
	ErrTooLarge = errors.New("usage snapshot exceeds resource bounds; reduce the period or tenant scope")
	ErrCapacity = errors.New("usage snapshot capacity reached; retry after snapshots expire")
	ErrCursor   = errors.New("invalid or mismatched usage cursor")
	ErrExpired  = errors.New("usage snapshot expired; restart the read")
	ErrQuery    = errors.New("invalid usage period")
)

type Config struct {
	RetentionDays    int   `json:"retention_days"`
	MaxSnapshots     int   `json:"max_snapshots"`
	MaxSnapshotBytes int64 `json:"max_snapshot_bytes"`
}

func Normalize(c *Config) (Config, error) {
	out := Config{RetentionDays: 365, MaxSnapshots: 16, MaxSnapshotBytes: 128 << 20}
	if c != nil {
		if c.RetentionDays != 0 {
			out.RetentionDays = c.RetentionDays
		}
		if c.MaxSnapshots != 0 {
			out.MaxSnapshots = c.MaxSnapshots
		}
		if c.MaxSnapshotBytes != 0 {
			out.MaxSnapshotBytes = c.MaxSnapshotBytes
		}
	}
	if out.RetentionDays < 1 || out.RetentionDays > 3650 || out.MaxSnapshots < 1 || out.MaxSnapshots > 256 || out.MaxSnapshotBytes < 1<<20 || out.MaxSnapshotBytes > 1<<30 {
		return Config{}, errors.New("usage requires retention 1..3650 days, snapshot capacity 1..256 and bytes 1048576..1073741824")
	}
	return out, nil
}

type Counters struct{ Live, Import, Queries, QueryMicros, ExportBytes, PipelineBytes, Webhooks int64 }

func (c Counters) Values() []int64 {
	return []int64{c.Live, c.Import, c.Queries, c.QueryMicros, c.ExportBytes, c.PipelineBytes, c.Webhooks}
}
func (c *Counters) Add(other Counters) error {
	a, b := []*int64{&c.Live, &c.Import, &c.Queries, &c.QueryMicros, &c.ExportBytes, &c.PipelineBytes, &c.Webhooks}, other.Values()
	for i, p := range a {
		if b[i] < 0 || *p > math.MaxInt64-b[i] {
			return errors.New("usage counter overflow or negative delta")
		}
	}
	for i, p := range a {
		*p += b[i]
	}
	return nil
}
func (c Counters) Valid() bool {
	for _, v := range c.Values() {
		if v < 0 {
			return false
		}
	}
	return true
}

type SourceTotals struct {
	SourceID, TenantID, ProjectID, Day string
	Revision                           int64
	Counters                           Counters
	Unresolved                         int64
	LastObserved                       string
}

type UsageCounters struct {
	Live          string `json:"accepted_live_events"`
	Import        string `json:"accepted_import_events"`
	Queries       string `json:"interactive_query_count"`
	QuerySeconds  string `json:"interactive_query_seconds" doc:"Observed backend wall time, not CPU. A lower bound while an execution is unresolved."`
	ExportBytes   string `json:"published_export_bytes" doc:"Compressed published data objects, not S3 download traffic or failed uploads."`
	PipelineBytes string `json:"published_pipeline_bytes"`
	Webhooks      string `json:"webhook_deliveries_succeeded" doc:"Unique delivery IDs with committed successful acknowledgments, not attempts or batched events."`
}

func (c Counters) Wire() *UsageCounters {
	return &UsageCounters{strconv.FormatInt(c.Live, 10), strconv.FormatInt(c.Import, 10), strconv.FormatInt(c.Queries, 10), strconv.FormatInt(c.QueryMicros/1000000, 10) + "." + padMicros(c.QueryMicros%1000000), strconv.FormatInt(c.ExportBytes, 10), strconv.FormatInt(c.PipelineBytes, 10), strconv.FormatInt(c.Webhooks, 10)}
}
func padMicros(n int64) string {
	s := strconv.FormatInt(n, 10)
	for len(s) < 6 {
		s = "0" + s
	}
	return s
}

type UsageQueryDuration struct {
	Quality      string `json:"quality" enum:"observed,lower_bound"`
	Unresolved   string `json:"unresolved_executions"`
	LastObserved string `json:"last_observed_at,omitempty"`
}
type UsageStorage struct {
	Bytes            *string `json:"current_parquet_bytes" nullable:"true"`
	ObservedAt       string  `json:"observed_at,omitempty"`
	OldestObservedAt string  `json:"oldest_observed_at,omitempty"`
	AgeSeconds       *int64  `json:"sample_age_seconds" nullable:"true"`
	Status           string  `json:"status" enum:"sampled,partial,stale,not_measured"`
	MeasuredProjects int     `json:"measured_projects"`
	ExpectedProjects int     `json:"expected_projects"`
}
type UsageRow struct {
	TenantID        string              `json:"tenant_id"`
	ProjectID       string              `json:"project_id,omitempty"`
	TenantDeleted   bool                `json:"tenant_deleted"`
	ProjectDeleted  bool                `json:"project_deleted"`
	PeriodStart     string              `json:"period_start"`
	PeriodEnd       string              `json:"period_end"`
	AvailableSince  string              `json:"available_since"`
	Partial         bool                `json:"partial"`
	AcceptanceBasis string              `json:"acceptance_basis" enum:"durable_commit" doc:"Counts durable retained outcomes, not memory acknowledgments; currently the SQLite journal, with native commit required for future hosted ingestion."`
	Counters        *UsageCounters      `json:"counters,omitempty" doc:"Absent when the whole bucket predates instrumentation; absence is not zero."`
	QueryDuration   *UsageQueryDuration `json:"query_duration,omitempty" doc:"Absent when this bucket predates instrumentation."`
	Storage         UsageStorage        `json:"storage"`
}
type UsagePage struct {
	MetricVersion     int        `json:"metric_version"`
	Epoch             string     `json:"usage_epoch"`
	SnapshotID        string     `json:"snapshot_id"`
	CreatedAt         string     `json:"snapshot_created_at"`
	ExpiresAt         string     `json:"snapshot_expires_at"`
	PublicationStatus string     `json:"publication_status" enum:"unknown" doc:"Only published durable source totals are included. This is not a sealed-through-time guarantee across stores or workers."`
	Rows              []UsageRow `json:"rows"`
	NextCursor        string     `json:"next_cursor,omitempty"`
}
type Query struct {
	Start, End, TenantID, Granularity, GroupBy, Cursor string
	Limit                                              int
}
type StorageSample struct {
	TenantID, ProjectID, Day, ObservedAt string
	Bytes                                int64
}

func Day(t time.Time) string { return t.UTC().Format("2006-01-02") + "T00:00:00Z" }
