package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/output"
	"github.com/olup/porcupine/internal/usage"
)

func (s *Service) exportDependencies() output.ExportDependencies {
	issuer := ""
	if s.externalAuthEnabled() {
		issuer = s.resources.external.Config().Issuer
	}
	d := output.ExportDependencies{Journal: s.db, Control: s.control, Clock: func() time.Time { return s.clock() }, Reader: s.lake, Issuer: issuer,
		LoadActor: func(ctx context.Context, id string) (auth.Principal, error) {
			actor, err := s.loadCredentialActor(ctx, id)
			if errors.Is(err, errInteractiveGrant) {
				return auth.Principal{}, output.ErrExportRevoked
			}
			return actor, err
		},
		CurrentActor: func(ctx context.Context, caller auth.Principal) (auth.Principal, error) {
			return s.currentAuthority(ctx, s.control, caller)
		},
		RequireActive: s.requireProjectActive, BeginActivity: s.activity.begin, Acquire: s.queries.TryBackground}
	d.ValidateSQL = func(ctx context.Context, project, raw string, parameters map[string]string) (string, error) {
		return s.lake.ValidateExport(ctx, project, raw, parameters, false)
	}
	if s.usage != nil {
		d.Usage = s.usage.recorder
	}
	return d
}

func (s *Service) configureExports(config *output.ExportConfig) error {
	exports, err := output.NewSQLiteExports(s.exportDependencies(), config)
	if err != nil {
		return err
	}
	s.exports = exports
	return nil
}

func (s *Service) configureHostedExports(config *output.ExportConfig,
	begin func(context.Context, *sql.Tx, string, string) error,
	end func(context.Context, string, string) error,
	snapshotCut output.ExportSnapshotCut,
) error {
	reader, ok := s.lake.(output.RetainedExportReader)
	if !ok {
		return errors.New("hosted exports require a retained DuckLake reader")
	}
	d := s.exportDependencies()
	d.LoadActorTx = func(ctx context.Context, tx *sql.Tx, id string) (auth.Principal, error) {
		actor, err := s.loadCredentialActorFrom(ctx, tx, id)
		if errors.Is(err, errInteractiveGrant) {
			return auth.Principal{}, output.ErrExportRevoked
		}
		return actor, err
	}
	d.CurrentActorTx = func(ctx context.Context, tx *sql.Tx, caller auth.Principal) (auth.Principal, error) {
		return s.currentAuthority(ctx, tx, caller)
	}
	d.BeginActivity = s.hosted.incarnation.beginOutputWork
	d.BeginAttemptTx = begin
	d.EndAttempt = end
	d.SnapshotCut = snapshotCut
	if s.usage != nil {
		var source string
		if err := s.control.QueryRow(`SELECT value FROM metadata WHERE key='shared_source_id'`).Scan(&source); err != nil {
			return errors.New("hosted export usage source unavailable")
		}
		d.Usage = usage.NewRecorder(s.control, "exports-"+source, d.Clock)
	}
	exports, err := output.NewPostgresExports(d, reader, config)
	if err != nil {
		return err
	}
	s.exports = exports
	return nil
}
