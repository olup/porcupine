// Package importer validates historical records, manifests, and resumable JSONL
// cursors. It performs no live capture, identity updates, or outbound delivery.
// Callers commit its next cursor in the same transaction as accepted events.
package importer
