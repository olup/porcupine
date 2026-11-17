package auth

import (
	"errors"
	"net/http"
	"net/url"
)

// ReadOAuthQuery enforces bounded, single-valued authorization parameters.
func ReadOAuthQuery(r *http.Request) (url.Values, error) {
	if len(r.URL.RawQuery) > 16384 {
		return nil, errors.New("invalid authorization request")
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	for _, v := range values {
		if len(v) != 1 {
			return nil, errors.New("duplicate authorization parameter")
		}
	}
	return values, nil
}
