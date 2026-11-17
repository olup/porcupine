package auth

import (
	"errors"
	"net/mail"
	"strings"
)

func NormalizeEmail(raw string) (string, error) {
	raw = strings.Trim(raw, " ")
	address, err := mail.ParseAddress(raw)
	if err != nil || len(raw) > 254 || address.Name != "" || address.Address != raw ||
		strings.ContainsAny(raw, "\r\n\t <>[]\\\"") {
		return "", errors.New("expected a single email address")
	}
	for i := range len(raw) {
		if raw[i] < 33 || raw[i] > 126 {
			return "", errors.New("expected an ASCII email address")
		}
	}
	local, domain, ok := strings.Cut(raw, "@")
	if !ok || len(local) > 64 || domain == "" || len(domain) > 253 {
		return "", errors.New("invalid email address")
	}
	for _, label := range strings.Split(domain, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", errors.New("invalid email domain")
		}
		for _, r := range label {
			if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				return "", errors.New("invalid email domain")
			}
		}
	}
	return local + "@" + strings.ToLower(domain), nil
}
