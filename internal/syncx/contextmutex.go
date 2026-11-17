// Package syncx holds the small locking primitives shared by the server packages.
package syncx

import (
	"context"
	"sync"
)

// ContextMutex permits canceled provisioning callers to leave the wait queue.
// Its zero value is ready to use and it must not be copied after first use.
type ContextMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *ContextMutex) Lock(ctx context.Context) error {
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

func (m *ContextMutex) Unlock() {
	select {
	case <-m.gate:
	default:
		panic("unlock of unlocked context mutex")
	}
}
