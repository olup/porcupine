package porcupine

import (
	"context"
	"database/sql"

	"github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

func (s *Service) initializeHostedQueries(store *lake.DuckLakeStore, register func(context.Context, *sql.Tx, string, string) error, finish func(context.Context, *sql.DB, string, string) error) error {
	reader, err := lake.NewHostedReader(store)
	if err != nil {
		return err
	}
	if err := s.queries.UseHostedActivity(register, finish); err != nil {
		return err
	}
	s.hostedReader = reader
	return nil
}

func (s *Service) interactiveReader() query.Reader {
	if s.hostedReader != nil {
		return s.hostedReader
	}
	if s.standaloneReader != nil {
		return s.standaloneReader
	}
	if s.lake == nil {
		return nil
	}
	return s.lake
}
