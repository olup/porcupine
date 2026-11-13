package porcupine

func analyticsError(status int, code, message, hint, field string) error {
	return &managementError{status: status, Code: code, Message: message, Hint: hint, Field: field}
}
