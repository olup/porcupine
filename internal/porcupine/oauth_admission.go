package porcupine

import "net/http"

func (s *Service) allowOAuthAuthorization(r *http.Request) (string, bool) {
	ip, err := s.clientIP(r)
	if err != nil {
		return "", false
	}
	peer := hash(ip.String())
	return peer, s.oauth.authorizationGate.Allow(peer)
}
