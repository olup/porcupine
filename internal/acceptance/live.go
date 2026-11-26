// Package acceptance owns validated live input, acknowledgment types, hosted
// admission, and standalone receipt policy.
package acceptance

import (
	"net/netip"

	eventapi "github.com/olup/porcupine/internal/event"
	identityapi "github.com/olup/porcupine/internal/identity"
)

type Batch struct {
	ProjectID  string
	Events     []Input
	Cookieless bool
}

type Peer struct {
	Address   netip.Addr
	UserAgent string
}

type Failure struct {
	Message         string
	Retry, Pressure bool
}

func (e *Failure) Error() string { return e.Message }

type Durability string

const (
	Durable  Durability = "durable"
	Buffered Durability = "buffered"
)

type Result interface{ acknowledgment() }
type DurableResult struct {
	Accepted, Duplicates, IdentityIgnored int
	Receipts                              []DurableReceipt
}

func (DurableResult) acknowledgment() {}

type BufferedReceipt struct {
	MessageID string `json:"message_id"`
	UUID      string `json:"uuid"`
}
type BufferedResult struct {
	Received  int
	Receipts  []BufferedReceipt
	Journaled bool
}

func (BufferedResult) acknowledgment() {}

type DurableReceipt struct {
	MessageID string `json:"message_id"`
	EventID   string `json:"event_id"`
	UUID      string `json:"uuid"`
	Duplicate bool   `json:"duplicate"`
}
type Input struct {
	Event    eventapi.Event
	Identity *identityapi.Command
}

func ValidateInput(e eventapi.Event) (Input, error) {
	input := Input{Event: e}
	if identityapi.IsControl(e.Event) {
		control, err := identityapi.Parse(e.Event, e.DistinctID, e.Properties)
		if err != nil {
			return Input{}, err
		}
		input.Identity = &control
		input.Event.DistinctID, input.Event.Properties = "", []byte(`{}`)
		if e.SourceID != "" {
			input.Event.Context = []byte(`{}`)
		}
	}
	return input, nil
}
