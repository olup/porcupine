package porcupine

import (
	"context"
	"database/sql"

	"github.com/olup/porcupine/internal/auth"
)

type hostedPipelineNativeAuthority struct{}

func (hostedPipelineNativeAuthority) ResolveCreatorTx(ctx context.Context, tx *sql.Tx, tenant, project, creator string) (bool, error) {
	authority := auth.PipelineNativeCredentialAuthority{
		ConstrainServiceKey: func(ctx context.Context, tx *sql.Tx, principal auth.Principal) (auth.Principal, error) {
			return constrainServiceKeyActor(ctx, tx, principal)
		},
	}
	return authority.ResolveCreatorTx(ctx, tx, tenant, project, creator)
}
