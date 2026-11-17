package porcupine

import (
	"context"
	"errors"
	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/output"
	"time"
)

func (s *Service) configureExports(config *output.ExportConfig) error {
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
		RequireActive: s.requireProjectActive, RequireOwner: s.requireSharedProjectOwner, BeginActivity: s.activity.begin, Acquire: s.acquirePipelineExport}
	if s.usage != nil {
		d.Usage = s.usage.recorder
	}
	exports, err := output.NewSQLiteExports(d, config)
	if err != nil {
		return err
	}
	s.exports = exports
	return nil
}
