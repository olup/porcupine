package porcupine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestContextMutexZeroValueAndCanceledWaiter(t *testing.T) {
	t.Parallel()
	var gate contextMutex
	if err := gate.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- gate.Lock(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked waiter returned %v", err)
		}
	case <-time.After(time.Second):
		gate.Unlock()
		t.Fatal("canceled waiter remained blocked until unlock")
	}
	gate.Unlock()
	if err := gate.Lock(context.Background()); err != nil {
		t.Fatal("canceled waiter consumed gate", err)
	}
	gate.Unlock()
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	for range 100 {
		if err := gate.Lock(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal("already canceled context acquired gate", err)
		}
	}
}

func TestContextMutexSerializesConcurrentCallers(t *testing.T) {
	t.Parallel()
	var gate contextMutex
	var workers sync.WaitGroup
	counter := 0
	for range 16 {
		workers.Go(func() {
			for range 100 {
				if err := gate.Lock(context.Background()); err != nil {
					t.Error(err)
					return
				}
				counter++
				gate.Unlock()
			}
		})
	}
	workers.Wait()
	if counter != 1600 {
		t.Fatal("context gate did not serialize callers")
	}
}
