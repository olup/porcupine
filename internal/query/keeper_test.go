package query

import (
	"context"
	"testing"
	"time"
)

func TestPermitTiming(t *testing.T) {
	t.Parallel()
	if admissionTimeout+renewInterval+controlTimeout >= permitLifetime {
		t.Fatal("an initial acknowledgement can outlive its first renewal opportunity")
	}
}

func TestKeeperIgnoresLateQuarantineAfterReturn(t *testing.T) {
	t.Parallel()
	m := new(keeper)
	call, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := &permit{id: "permit", project: "project"}
	if err := m.start(p, cancel, time.Now().Add(time.Hour), nil, "worker"); err != nil {
		t.Fatal(err)
	}
	if err := m.returned(call, p, nil); err != nil {
		t.Fatal(err)
	}
	m.quarantine(p.id, "native_query_unresponsive", false)
	m.quarantine(p.id, "query_permit_uncertain", true)
	if status := m.status(); status.Active != 0 || status.Unavailable() || call.Err() != nil {
		t.Fatalf("late timer or renewal failure affected returned scope: %+v", status)
	}
}
