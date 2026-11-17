// Package ingest decodes producer protocols into a common input without choosing
// a destination project or granting authority. It performs no storage or network
// work. The acceptance transaction owns durable IDs, deduplication, and identity.
package ingest
