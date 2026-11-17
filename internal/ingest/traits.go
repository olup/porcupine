package ingest

import (
	"encoding/json"
	"strings"
)

const MaxTraitsBytes = 32 << 10
const MaxTraitKeys = 128

func Traits(raw any) (map[string]any, bool) {
	if raw == nil {
		return map[string]any{}, true
	}
	traits, ok := raw.(map[string]any)
	if !ok || len(traits) > MaxTraitKeys {
		return nil, false
	}
	for key := range traits {
		if strings.TrimSpace(key) == "" || len(key) > 200 {
			return nil, false
		}
	}
	encoded, err := json.Marshal(traits)
	return traits, err == nil && len(encoded) <= MaxTraitsBytes
}
