package porcupine

import (
	"context"
	"errors"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestHostedProjectCleanupRecoveryFloor(t *testing.T) {
	t.Parallel()
	cut := time.Date(2026, time.October, 10, 10, 0, 0, 0, time.UTC)
	grace := 10 * time.Hour
	config := lakeapi.DuckLakeConfig{RestoreWindow: 8 * time.Hour, RestoreMaxReaderTime: time.Hour, RestoreSafetyMargin: time.Hour, CleanupGrace: grace}
	for _, test := range []struct {
		name      string
		observed  time.Time
		floor     time.Time
		floorErr  error
		noSource  bool
		permitted bool
	}{
		{name: "unknown", observed: cut.Add(grace), noSource: true},
		{name: "unavailable", observed: cut.Add(grace), floorErr: errors.New("offline")},
		{name: "zero", observed: cut.Add(grace)},
		{name: "oldest target precedes inventory", observed: cut.Add(grace), floor: cut.Add(-time.Microsecond)},
		{name: "floor is in the future", observed: cut.Add(grace), floor: cut.Add(grace + time.Microsecond)},
		{name: "grace has not elapsed", observed: cut.Add(grace - time.Microsecond), floor: cut},
		{name: "cut and grace both covered", observed: cut.Add(grace), floor: cut, permitted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := &hostedRuntime{native: &lakeapi.DuckLakeStore{Config: config}}
			if !test.noSource {
				h.recoveryFloor = func(context.Context) (time.Time, error) { return test.floor, test.floorErr }
			}
			err := h.checkCleanupRecoveryFloor(t.Context(), test.observed, cut)
			if test.permitted && err != nil || !test.permitted && !errors.Is(err, errHostedRecoveryFloor) {
				t.Fatalf("permitted = %v, got %v", test.permitted, err)
			}
		})
	}
}
