package porcupine

import (
	"context"
	"errors"

	eventapi "github.com/olup/porcupine/internal/event"
)

// AcceptanceMode selects the live acknowledgment contract. Buffered acceptance
// remains unavailable until the hosted durable writer and state providers exist.
type AcceptanceMode string

const (
	AcceptanceModeDurable  AcceptanceMode = "durable"
	AcceptanceModeBuffered AcceptanceMode = "buffered"
)

func validateAcceptanceMode(mode AcceptanceMode) error {
	switch mode {
	case "", AcceptanceModeDurable:
		return nil
	case AcceptanceModeBuffered:
		return errors.New("buffered acceptance is unavailable until the hosted durable writer and shared state providers are implemented")
	default:
		return errors.New("acceptance mode must be durable or buffered")
	}
}

type acceptanceDurability string

const (
	acceptanceDurable  acceptanceDurability = "durable"
	acceptanceBuffered acceptanceDurability = "buffered"
)

type liveAcceptor interface {
	Accept(context.Context, preparedLiveBatch, ingestionPeer, bool) (liveAcceptanceResult, error)
	Durability() acceptanceDurability
}

type liveAcceptanceResult interface{ liveAcceptanceResult() }
type durableLiveResult struct {
	accepted, duplicates, identityIgnored int
	receipts                              []ingestionReceipt
}

func (durableLiveResult) liveAcceptanceResult() {}

type bufferedReceipt struct {
	MessageID string `json:"message_id"`
	UUID      string `json:"uuid"`
}
type bufferedLiveResult struct {
	received int
	receipts []bufferedReceipt
}

func (bufferedLiveResult) liveAcceptanceResult() {}

func liveAcceptanceResponse(result liveAcceptanceResult, receipts bool) (map[string]any, error) {
	switch result := result.(type) {
	case durableLiveResult:
		response := map[string]any{"status": 1, "durability": acceptanceDurable, "accepted": result.accepted, "duplicates": result.duplicates, "identity_ignored": result.identityIgnored}
		if receipts {
			response["receipts"] = result.receipts
		}
		return response, nil
	case bufferedLiveResult:
		response := map[string]any{"status": 1, "durability": acceptanceBuffered, "received": result.received}
		if receipts {
			response["receipts"] = result.receipts
		}
		return response, nil
	default:
		return nil, errors.New("live acceptance returned an invalid acknowledgment")
	}
}

type validatedLiveEvent struct {
	event    eventapi.Event
	identity *identityControl
}

func validateLiveEvent(e eventapi.Event) (validatedLiveEvent, error) {
	input := validatedLiveEvent{event: e}
	if isIdentityControl(e.Event) {
		control, err := parseIdentityControl(e)
		if err != nil {
			return validatedLiveEvent{}, err
		}
		input.identity = &control
		input.event.DistinctID, input.event.Properties = "", []byte(`{}`)
		if e.SourceID != "" {
			input.event.Context = []byte(`{}`)
		}
	}
	return input, nil
}
func (b preparedLiveBatch) eventValues() []eventapi.Event {
	events := make([]eventapi.Event, len(b.events))
	for i, input := range b.events {
		events[i] = input.event
	}
	return events
}
