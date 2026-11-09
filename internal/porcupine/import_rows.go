package porcupine

import (
	"encoding/json"
	"time"
)

type ImportRow struct {
	Kind         string
	Event        Event
	Person       ImportPerson
	receiptKnown bool
}
type ImportPerson struct {
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
type ImportRowResult struct {
	Accepted   int64
	Duplicates int64
	Ignored    int64
	Unresolved int64
}
