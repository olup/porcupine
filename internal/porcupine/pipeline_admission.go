package porcupine

import "context"

// Heavy queries share one slot so a batch cannot occupy the last interactive slot.
func (s *Service) acquirePipelineExport(ctx context.Context) (func(), bool) {
	if ctx.Err() != nil || len(s.querySlots) != 0 {
		return nil, false
	}
	select {
	case s.batchSlot <- struct{}{}:
	default:
		return nil, false
	}
	select {
	case s.querySlots <- struct{}{}:
		return func() { <-s.querySlots; <-s.batchSlot }, true
	default:
		<-s.batchSlot
		return nil, false
	}
}
