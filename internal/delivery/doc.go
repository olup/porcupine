// Package delivery owns durable webhook attempts and formats analytical output.
// Acceptance and mutation callers supply their own transaction. Current actor
// authorization, destination decryption and project activity are explicit host
// capabilities; the delivery module does not own their state.
package delivery
