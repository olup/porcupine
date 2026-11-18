package query

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBackgroundSharesCapacityWithInteractiveReads(t *testing.T) {
	t.Parallel()
	limits, err := Normalize(nil)
	if err != nil {
		t.Fatal(err)
	}
	r := New(limits, time.Now, nil)
	const contenders = 16
	start := make(chan struct{})
	grants := make(chan func(), contenders)
	var workers sync.WaitGroup
	for range contenders {
		workers.Go(func() {
			<-start
			if release, ok := r.TryBackground(t.Context()); ok {
				grants <- release
			}
		})
	}
	close(start)
	workers.Wait()
	close(grants)
	background, ok := <-grants
	if !ok {
		t.Fatal("an idle worker refused all background jobs")
	}
	defer background()
	if _, ok := <-grants; ok || r.UsedSlots() != 1 {
		t.Fatal("background jobs received overlapping grants")
	}
	foreground, err := r.capacity.tryAcquire("tenant", 2)
	if err != nil {
		t.Fatal("background work starved interactive queries", err)
	}
	defer foreground()
	if _, err := r.capacity.tryAcquire("other", 2); !errors.Is(err, errWorkerFull) {
		t.Fatal("background and interactive jobs did not share the worker limit", err)
	}
	background()
	if _, ok := r.TryBackground(t.Context()); ok {
		t.Fatal("background job admitted while the worker was busy")
	}
	foreground()
	release, ok := r.TryBackground(t.Context())
	if !ok {
		t.Fatal("worker capacity did not recover")
	}
	release()
	background()
	foreground()
	if r.UsedSlots() != 0 {
		t.Fatal("capacity leaked after return", r.UsedSlots())
	}
}
