package porcupine

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
)

func readOAuthForm(w http.ResponseWriter, r *http.Request) (url.Values, int, string) {
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	encoding := r.Header.Get("Content-Encoding")
	if r.Method != "POST" || err != nil || contentType != "application/x-www-form-urlencoded" || r.URL.RawQuery != "" || r.URL.ForceQuery || encoding != "" && encoding != "identity" {
		return nil, 400, "invalid_request"
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			return nil, 413, "invalid_request"
		}
		return nil, 400, "invalid_request"
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, 400, "invalid_request"
	}
	for _, values := range form {
		if len(values) != 1 {
			return nil, 400, "invalid_request"
		}
	}
	for _, key := range []string{"client_assertion", "client_assertion_type"} {
		if _, ok := form[key]; ok {
			return nil, 400, "invalid_request"
		}
	}
	return form, 0, ""
}
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
	client, err := loadOAuthClient(r.Context(), s.db, id)
	if err != nil {
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
