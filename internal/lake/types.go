package lake

import (
	"encoding/json"
	"time"

	"github.com/olup/porcupine/internal/nativequery"
)

type AnalyticsResultColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type AnalyticsStatistics struct {
	Elapsed float64 `json:"elapsed"`
}

type AnalyticsResult struct {
	Meta       []AnalyticsResultColumn      `json:"meta"`
	Data       []map[string]json.RawMessage `json:"data" doc:"Typed SQL JSON values. 64-bit integers are represented as strings to preserve precision."`
	Rows       uint64                       `json:"rows"`
	Statistics AnalyticsStatistics          `json:"statistics"`
}

type QueryExecutionLimits struct {
	Timeout        time.Duration
	MaxRows        int
	MaxResultBytes int
}

func DefaultQueryExecutionLimits() QueryExecutionLimits {
	return QueryExecutionLimits{Timeout: 5 * time.Second, MaxRows: AnalyticsMaxRows, MaxResultBytes: AnalyticsMaxBytes}
}

type IdentityLinkSnapshot struct {
	AnonymousID  string `json:"anonymous_id"`
	IdentifiedID string `json:"identified_id"`
	Version      string `json:"version"`
}

type IdentitySnapshot struct {
	Version      string                 `json:"version"`
	IdentifiedID string                 `json:"identified_id"`
	Traits       map[string]any         `json:"traits"`
	Links        []IdentityLinkSnapshot `json:"links"`
}

type DeliveryStatus struct {
	PendingUnknown          bool   `json:"pending_unknown,omitempty" doc:"True on a shared query worker which does not own this project's SQLite queue. Do not interpret pending=0 as a drained queue; get-event-schema is routed to its owner for authoritative status."`
	OwnerWorkerID           string `json:"owner_worker_id,omitempty"`
	Enabled                 bool   `json:"enabled"`
	Pending                 int64  `json:"pending"`
	OldestPendingReceivedAt string `json:"oldest_pending_received_at,omitempty" doc:"Receipt time of the earliest queued original by acceptance order, including quarantined events, in this project"`
	Quarantined             int64  `json:"quarantined" doc:"Accepted originals retained in SQLite after confirmed row-specific rejection. Included in pending and queue capacity."`
	PendingMutations        int64  `json:"pending_mutations"`
	LastSuccess             string `json:"last_success,omitempty"`
	Error                   string `json:"error,omitempty"`
}

const (
	MaxSQLBytes       = nativequery.MaxSQLBytes
	AnalyticsMaxRows  = 1000
	AnalyticsMaxBytes = 1 << 20
)
