package porcupine

import (
	"context"
	"sync"
)

// contextMutex permits canceled provisioning callers to leave the wait queue.
// Its zero value is ready to use and it must not be copied after first use.
type contextMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *contextMutex) Lock(ctx context.Context) error {
	m.once.Do(func() { m.gate = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-m.gate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *contextMutex) Unlock() {
	select {
	case <-m.gate:
	default:
		panic("unlock of unlocked context mutex")
	}
}
