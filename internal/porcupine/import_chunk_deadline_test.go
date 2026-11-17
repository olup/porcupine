package porcupine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
)

func TestImportChunkCommitContextBounds(t *testing.T) {
	t.Parallel()
	for _, callerLimit := range []time.Duration{0, time.Second} {
		t.Run(callerLimit.String(), func(t *testing.T) {
			t.Parallel()
			s := service(t, 10)
			project, _ := project(t, s)
			ctx := t.Context()
			if callerLimit != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, callerLimit)
				defer cancel()
			}
			started := time.Now()
			limit := started.Add(importChunkCommitTimeout)
			if deadline, ok := ctx.Deadline(); ok && deadline.Before(limit) {
				limit = deadline
			}
			checked := false
			s.eventRules.prepareAcceptance = func(work context.Context, _ string) error {
				checked = true
				deadline, ok := work.Deadline()
				if !ok || deadline.After(limit.Add(10*time.Millisecond)) || !deadline.After(started) {
					t.Fatalf("commit deadline=%v present=%v, expected no later than %v", deadline, ok, limit)
				}
				if callerDeadline, ok := ctx.Deadline(); callerLimit != 0 && ok && !deadline.Equal(callerDeadline) {
					t.Fatalf("caller deadline changed: got %v, want %v", deadline, callerDeadline)
				}
				return errors.New("stop before opening the transaction")
			}
			if err := s.imports.commitImportChunk(ctx, project, "", storedImportUploadFile{}, importChunkWork{}, auth.Principal{}, false); err == nil || !checked {
				t.Fatalf("policy check not reached with bounded context: checked=%v error=%v", checked, err)
			}
		})
	}
}
