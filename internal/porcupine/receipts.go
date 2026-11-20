package porcupine

import "github.com/olup/porcupine/internal/acceptance"

func (s *Service) receiptHistory() acceptance.ReceiptLookup {
	if s.lake == nil {
		return nil
	}
	if s.receiptLookup != nil {
		return s.receiptLookup
	}
	return s.lake.KnownEvents
}
