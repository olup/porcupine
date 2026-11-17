package lake

import "strings"

const SourceEventUUIDPrefix = "porcupine-ingest-v1:"

func ValidStoredEventUUID(value string) bool {
	if len(value) <= 128 {
		return value != ""
	}
	digest, ok := strings.CutPrefix(value, SourceEventUUIDPrefix)
	if !ok || len(digest) != 128 {
		return false
	}
	for _, c := range digest {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
