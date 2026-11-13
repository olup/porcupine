//go:build ducklake && !linux && !darwin

package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"io"
)

func copyDuckLakeExport(context.Context, *sql.Conn, string, string, []any, func(io.Reader) error) error {
	return errors.New("native streaming exports require Linux or macOS")
}
