package main

import "github.com/olup/porcupine/internal/query"

func (s settings) interactiveQueryConfiguration() (*query.InteractiveQueryConfig, error) {
	c := &query.InteractiveQueryConfig{}
	for _, field := range []struct {
		name     string
		target   *int
		min, max int
	}{
		{"PORCUPINE_QUERY_MAX_CONCURRENT", &c.MaxConcurrent, 2, 64},
		{"PORCUPINE_QUERY_TIMEOUT_MS", &c.TimeoutMS, 1, 60000},
		{"PORCUPINE_QUERY_MAX_ROWS", &c.MaxRows, 1, 100000},
		{"PORCUPINE_QUERY_MAX_RESULT_BYTES", &c.MaxResultBytes, 256, 64 << 20},
	} {
		value, err := s.analyticsInteger(field.name, 0, field.min, field.max)
		if err != nil {
			return nil, err
		}
		*field.target = value
	}
	return c, nil
}
