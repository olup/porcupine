package porcupine

import (
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type oauthAuthorizationPeer struct {
	limiter *rate.Limiter
	last    time.Time
}
type oauthAuthorizationGate struct {
	mu     sync.Mutex
	global *rate.Limiter
	peers  map[string]*oauthAuthorizationPeer
}

func (s *Service) allowOAuthAuthorization(r *http.Request) (string, bool) {
	ip, err := s.clientIP(r)
	if err != nil {
		return "", false
	}
	peer := hash(ip.String())
	gate := &s.oauth.authorizationGate
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
			return peer, false
		}
	}
	if !gate.global.Allow() {
		return peer, false
	}
	if current == nil {
		for key, value := range gate.peers {
			if now.Sub(value.last) > 10*time.Minute {
				delete(gate.peers, key)
			}
		}
		if len(gate.peers) >= 1024 {
			return peer, false
		}
		current = &oauthAuthorizationPeer{limiter: rate.NewLimiter(1, 5), last: now}
		gate.peers[peer] = current
		if !current.limiter.Allow() {
			return peer, false
		}
	}
	return peer, true
}
