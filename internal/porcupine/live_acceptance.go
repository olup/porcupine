package porcupine

import (
	"context"
	"errors"

	"github.com/olup/porcupine/internal/acceptance"
)

// AcceptanceMode selects the live acknowledgment contract for the deployment profile.
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
		return errors.New("buffered acceptance requires the hosted profile")
	default:
		return errors.New("acceptance mode must be durable or buffered")
	}
}

type liveAcceptor interface {
	Accept(context.Context, acceptance.Batch, acceptance.Peer, bool) (acceptance.Result, error)
	Durability() acceptance.Durability
}

func liveAcceptanceResponse(result acceptance.Result, receipts bool) (map[string]any, error) {
	switch result := result.(type) {
	case acceptance.DurableResult:
		response := map[string]any{"status": 1, "durability": acceptance.Durable, "accepted": result.Accepted, "duplicates": result.Duplicates, "identity_ignored": result.IdentityIgnored}
		if receipts {
			response["receipts"] = result.Receipts
		}
		return response, nil
	case acceptance.BufferedResult:
		durability := acceptance.Buffered
		if result.Journaled {
			durability = acceptance.Durable
		}
		response := map[string]any{"status": 1, "durability": durability, "received": result.Received}
		if receipts {
			response["receipts"] = result.Receipts
		}
		return response, nil
	default:
		return nil, errors.New("live acceptance returned an invalid acknowledgment")
	}
}
