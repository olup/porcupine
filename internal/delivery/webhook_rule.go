package delivery

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
