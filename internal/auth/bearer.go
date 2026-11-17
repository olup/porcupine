package auth

import (
	"errors"
	"net/http"
	"strings"
)

func BearerCredential(r *http.Request) (string, error) {
	if len(r.Header.Values("Authorization")) != 1 {
		return "", errors.New("one authorization header is required")
	}
	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") || len(fields[1]) > 16<<10 {
		return "", errors.New("invalid bearer authorization")
	}
	return fields[1], nil
}
