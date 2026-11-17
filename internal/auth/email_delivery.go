package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

const EmailDeliveryPeriod = 10 * time.Minute

// EmailDeliveryLimiter bounds local sender work without taking the browser session lock.
type EmailDeliveryLimiter struct {
	mu     sync.Mutex
	window emailDeliveryWindow
}

type emailDeliveryWindow struct {
	until         time.Time
	addressCounts map[string]int
	peerCounts    map[string]int
	total         int
	inFlight      int
}

func (o *EmailDeliveryLimiter) Reserve(now time.Time, address, peer string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	window := &o.window
	if !now.Before(window.until) {
		*window = emailDeliveryWindow{until: now.Add(EmailDeliveryPeriod), addressCounts: make(map[string]int), peerCounts: make(map[string]int), inFlight: window.inFlight}
	}
	// The global budget also bounds both maps, including entries for unknown addresses.
	digest := sha256.Sum256([]byte(address))
	address = hex.EncodeToString(digest[:])
	if window.total >= 120 || window.peerCounts[peer] >= 20 || window.addressCounts[address] >= 3 || window.inFlight >= 8 {
		return false
	}
	window.addressCounts[address]++
	window.peerCounts[peer]++
	window.total++
	window.inFlight++
	return true
}

func (o *EmailDeliveryLimiter) Finish() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.window.inFlight--
}
