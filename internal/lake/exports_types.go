package lake

import (
	"encoding/json"
)

type ExportStreamResult struct {
	Columns []AnalyticsResultColumn
	Rows    int64
}

type ExportStreamCallbacks struct {
	Columns func([]AnalyticsResultColumn) error
	Row     func([]json.RawMessage) error
}
