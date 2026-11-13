//go:build !ducklake

package porcupine

import "errors"

func newDuckLake(DuckLakeConfig) (lakeStore, error) {
	return nil, errors.New("DuckLake requires the ducklake build tag and native DuckDB build")
}
