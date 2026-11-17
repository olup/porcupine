package porcupine

import (
	"context"
	"errors"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func (s *Service) nativeBackupWriterID(ctx context.Context) (string, error) {
	if _, err := s.db.ExecContext(ctx, "INSERT INTO metadata(key,value) VALUES('native_backup_writer_id',?) ON CONFLICT(key) DO NOTHING", eventapi.NewID()); err != nil {
		return "", err
	}
	var writer string
	err := s.db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key='native_backup_writer_id'").Scan(&writer)
	if err != nil {
		return "", err
	}
	if len(writer) != 36 || !lakeapi.LakePrefixSegmentPattern.MatchString(writer) {
		return "", errors.New("invalid native backup writer identity")
	}
	return writer, nil
}
