// Package apierr is the error shape shared by the management API, MCP and
// the analytical store. It carries an HTTP status without importing HTTP code.
package apierr

// Error is returned to API callers as {"error", "code", "hint", "field"}.
type Error struct {
	Message string `json:"error"`
	Code    string `json:"code,omitempty"`
	Hint    string `json:"hint,omitempty"`
	Field   string `json:"field,omitempty"`
	status  int
}

func (e *Error) Error() string { return e.Message }

// GetStatus is the HTTP status; Huma reads it through this method name.
func (e *Error) GetStatus() int { return e.status }

// New returns a plain API error.
func New(status int, message string) error { return &Error{Message: message, status: status} }

// Detailed returns an API error with a machine code and a repair hint.
func Detailed(status int, code, message, hint, field string) error {
	return &Error{status: status, Code: code, Message: message, Hint: hint, Field: field}
}
