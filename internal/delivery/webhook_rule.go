package delivery

// WebhookRule is the persisted routing rule. The private destination is encrypted
// separately from this metadata.
type WebhookRule struct {
	IncludeEvents   []string `json:"include_events,omitempty"`
	ExcludeEvents   []string `json:"exclude_events,omitempty"`
	DeliveryMode    string   `json:"delivery_mode,omitempty"`
	IntervalSeconds int64    `json:"interval_seconds,omitempty"`
	StartAt         string   `json:"start_at,omitempty"`
}

func (rule WebhookRule) Matches(event string) bool {
	for _, name := range rule.ExcludeEvents {
		if name == event {
			return false
		}
	}
	if len(rule.IncludeEvents) == 0 {
		return true
	}
	for _, name := range rule.IncludeEvents {
		if name == event {
			return true
		}
	}
	return false
}
