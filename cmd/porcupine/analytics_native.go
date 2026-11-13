//go:build ducklake

package main

import "path/filepath"

func defaultAnalyticsDirectory(dbPath string) string {
	if dbPath == "" {
		dbPath = "./data/porcupine.db"
	}
	return filepath.Join(filepath.Dir(dbPath), "ducklake")
}
