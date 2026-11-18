package porcupine

import (
	"errors"
	"net/http"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
)

func authenticationUnavailable(err error) bool {
	var managed *apierr.Error
	return errors.Is(err, auth.ErrUnavailable) || errors.Is(err, auth.ErrExternalKeysUnavailable) || errors.As(err, &managed) && managed.GetStatus() == 503
}

func writeAuthenticationUnavailable(w http.ResponseWriter, err error) bool {
	if !authenticationUnavailable(err) {
		return false
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Retry-After", "1")
	write(w, 503, map[string]string{"error": "authentication temporarily unavailable", "code": "authentication_unavailable", "hint": "Retry the same request after Retry-After; keep existing event or message IDs."})
	return true
}
