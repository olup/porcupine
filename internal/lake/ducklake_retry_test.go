package lake

import (
	"context"
	"errors"
	"fmt"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

func TestNativeTransactionRetryIsTypedBoundedAndCancelable(t *testing.T) {
	t.Parallel()
	t.Run("typed conflict succeeds after rollback", func(t *testing.T) {
		t.Parallel()
		attempts := 0
		err := retryNativeTransaction(t.Context(), func() error {
			attempts++
			if attempts == 1 {
				return fmt.Errorf("commit: %w", &duckdb.Error{Type: duckdb.ErrorTypeTransaction, Msg: "native conflict"})
			}
			return nil
		})
		if err != nil || attempts != 2 {
			t.Fatal("typed rollback was not retried once", attempts, err)
		}
	})
	t.Run("persistent conflict remains an error", func(t *testing.T) {
		t.Parallel()
		failure := &duckdb.Error{Type: duckdb.ErrorTypeTransaction, Msg: "persistent conflict"}
		attempts := 0
		err := retryNativeTransaction(t.Context(), func() error { attempts++; return failure })
		var exhausted *nativeConflictRetryLimit
		if !errors.Is(err, failure) || attempts != 16 || !errors.As(err, &exhausted) || exhausted.attempts != 16 {
			t.Fatal("native retry budget did not preserve the error", attempts, err)
		}
	})
	for _, failure := range []error{
		&duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "native allocation failure"},
		&duckdb.Error{Type: duckdb.ErrorTypeInvalidInput, Msg: "invalid projection"},
		errors.New("Transaction conflict - untrusted text"),
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			t.Parallel()
			attempts := 0
			err := retryNativeTransaction(t.Context(), func() error { attempts++; return failure })
			if !errors.Is(err, failure) || attempts != 1 {
				t.Fatal("non-transaction error consumed conflict retry budget", attempts, err)
			}
		})
	}
	t.Run("canceled before operation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		attempts := 0
		err := retryNativeTransaction(ctx, func() error { attempts++; return nil })
		if !errors.Is(err, context.Canceled) || attempts != 0 {
			t.Fatal("canceled operation started", attempts, err)
		}
	})
	t.Run("canceled during backoff", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		attempts := 0
		err := retryNativeTransaction(ctx, func() error {
			attempts++
			cancel()
			return &duckdb.Error{Type: duckdb.ErrorTypeTransaction, Msg: "canceled conflict"}
		})
		if !errors.Is(err, context.Canceled) || attempts != 1 {
			t.Fatal("cancellation did not stop retry backoff", attempts, err)
		}
	})
}
