package lake

import (
	"context"
	"errors"
	"fmt"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

type nativeConflictRetryLimit struct {
	attempts int
	err      error
}

func (e *nativeConflictRetryLimit) Error() string {
	return fmt.Sprintf("native transaction still conflicts after %d attempts: %v", e.attempts, e.err)
}

func (e *nativeConflictRetryLimit) Unwrap() error { return e.err }

// A native transaction conflict rolls back its complete attempt. Reopen the
// transaction against the new snapshot, preserving the original IDs/revisions.
// Allocation failures use their separate chunk-reduction budget.
func retryNativeTransaction(ctx context.Context, operation func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation()
		if err == nil || !isNativeTransactionConflict(err) {
			return err
		}
		if attempt >= 15 {
			return &nativeConflictRetryLimit{attempts: attempt + 1, err: err}
		}
		timer := time.NewTimer(time.Duration(min(5*(attempt+1), 100)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func isNativeTransactionConflict(err error) bool {
	// Never infer retryability from an arbitrary server-provided string.
	var native *duckdb.Error
	return errors.As(err, &native) && native.Type == duckdb.ErrorTypeTransaction
}
