package delivery

import eventapi "github.com/olup/porcupine/internal/event"

const WebhookChunkEvents = 100
const WebhookChunkBytes = 1 << 20

type WebhookPayload struct {
	DeliveryID    string           `json:"delivery_id"`
	PipelineID    string           `json:"pipeline_id"`
	ProjectID     string           `json:"project_id"`
	Event         *eventapi.Event  `json:"event,omitempty"`
	WindowStartMS *int64           `json:"window_start_ms,omitempty"`
	WindowEndMS   *int64           `json:"window_end_ms,omitempty"`
	Events        []eventapi.Event `json:"events,omitempty"`
}
