package auth

import "errors"

// ErrUnavailable means a credential could not be checked. Callers must retry,
// rather than treating it as an invalid or revoked credential.
var ErrUnavailable = errors.New("authentication temporarily unavailable")
