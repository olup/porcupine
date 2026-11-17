package importer

import (
	"encoding/json"
	"time"

	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/ingest"
)

type Row struct {
	Kind      string
	Event     event.Event
	Person    Person
	Ingestion *ingest.Metadata
}
type Person struct {
	InternalID     string
	IdentifiedID   string
	DistinctIDs    []string
	Properties     json.RawMessage
	IsIdentified   bool
	PersonVersion  uint64
	MappingVersion uint64
	Deleted        bool
	Timestamp      time.Time
}
