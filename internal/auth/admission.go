package auth

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type oauthAuthorizationPeer struct {
	limiter *rate.Limiter
	last    time.Time
}
type AuthorizationGate struct {
	mu     sync.Mutex
	global *rate.Limiter
	peers  map[string]*oauthAuthorizationPeer
}

func (gate *AuthorizationGate) Allow(peer string) bool {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.global == nil {
		gate.global = rate.NewLimiter(5, 20)
		gate.peers = map[string]*oauthAuthorizationPeer{}
	}
	now := time.Now()
	current := gate.peers[peer]
	if current != nil {
		current.last = now
		if !current.limiter.Allow() {
			return false
		}
	}
	if !gate.global.Allow() {
		return false
	}
	if current == nil {
		for key, value := range gate.peers {
			if now.Sub(value.last) > 10*time.Minute {
				delete(gate.peers, key)
			}
		}
		if len(gate.peers) >= 1024 {
			return false
		}
		current = &oauthAuthorizationPeer{limiter: rate.NewLimiter(1, 5), last: now}
		gate.peers[peer] = current
		if !current.limiter.Allow() {
			return false
		}
	}
	return true
}
