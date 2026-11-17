package auth

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
)

func ReadOAuthForm(w http.ResponseWriter, r *http.Request) (url.Values, int, string) {
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
