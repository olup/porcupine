package porcupine

import (
	"net/http"
	"net/url"
	"strings"
)

func (s *Service) authenticateOAuthEndpoint(r *http.Request, form url.Values) (*oauthClient, int, string) {
	var id, secret string
	basic := len(r.Header.Values("Authorization")) > 0
	if len(r.Header.Values("Authorization")) > 1 {
		return nil, 401, "invalid_client"
	}
	if basic {
		var ok bool
		id, secret, ok = r.BasicAuth()
		if !ok {
			return nil, 401, "invalid_client"
		}
		var err error
		id, err = url.QueryUnescape(id)
		if err != nil {
			return nil, 401, "invalid_client"
		}
		secret, err = url.QueryUnescape(secret)
		if err != nil {
			return nil, 401, "invalid_client"
		}
		if _, ok := form["client_secret"]; ok {
			return nil, 400, "invalid_request"
		}
		if requested, present := form["client_id"]; present && requested[0] != id {
			return nil, 400, "invalid_request"
		}
	} else {
		id = form.Get("client_id")
		secret = form.Get("client_secret")
	}
	if id == "" || len(id) > 256 || len(secret) > 4096 || strings.ContainsAny(id+secret, "\r\n\x00") {
		return nil, 401, "invalid_client"
	}
	client, err := loadOAuthClient(r.Context(), s.control, id)
	if err != nil {
		if authenticationUnavailable(err) {
			return nil, 503, "temporarily_unavailable"
		}
		return nil, 401, "invalid_client"
	}
	switch client.AuthMethod {
	case "none":
		if basic {
			return nil, 401, "invalid_client"
		}
		if _, present := form["client_secret"]; present {
			return nil, 400, "invalid_request"
		}
	case "client_secret_basic":
		if !basic {
			if _, present := form["client_secret"]; present {
				return nil, 400, "invalid_request"
			}
			return nil, 401, "invalid_client"
		}
		if secret == "" || !client.VerifyPassword(secret) {
			return nil, 401, "invalid_client"
		}
	case "client_secret_post":
		if basic {
			return nil, 401, "invalid_client"
		}
		if secret == "" || !client.VerifyPassword(secret) {
			return nil, 401, "invalid_client"
		}
	default:
		return nil, 401, "invalid_client"
	}
	return client, 0, ""
}
