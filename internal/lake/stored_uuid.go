package lake

import (
	"github.com/olup/porcupine/internal/importer"
	"strings"
)

const SourceEventUUIDPrefix = "porcupine-ingest-v1:"

func ValidStoredEventUUID(value string) bool {
	return validLiveEventUUID(value) || validUUIDDigest(value, importer.PostHogPersonUUIDPrefix)
}

func validLiveEventUUID(value string) bool {
	if len(value) <= 128 {
		return value != ""
	}
	return validUUIDDigest(value, SourceEventUUIDPrefix)
}

func validUUIDDigest(value, prefix string) bool {
	digest, ok := strings.CutPrefix(value, prefix)
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
