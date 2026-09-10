package download

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openabstractions/abstraction-identity/listen"
)

// S1. A client that found no service did not start one.
//
// The rule this keeps is the one the owner struck a silent fallback for
// (VISION.md 2026-09-10): absence is reported, and a provider arrives because
// somebody supplied it on purpose. A client that starts a helper because it
// could not find one is the same defect wearing a different coat, so the
// evidence is not "the call returned an error" — it is that the endpoint is
// still unserved afterwards.
// The endpoint is the announced one rather than an invented one, because that
// is the only name a client ever learns: ask reads it out of the heartbeat. A
// client wired to start what it could not find would start it there, so there
// is exactly one address to look at afterwards.
func TestAClientThatFoundNoServiceDidNotStartOne(t *testing.T) {
	_, store, _ := newRunner(t)
	b := announce(t, store, "jobd@test:1", "here")
	at := b.Endpoint
	b.Close()

	if err := Nudge(store); !errors.Is(err, ErrNoSupervisor) {
		t.Fatalf("Nudge answered %v, not absence", err)
	}
	if _, err := Who(store); !errors.Is(err, ErrNoSupervisor) {
		t.Fatalf("Who answered %v, not absence", err)
	}
	if c, err := listen.Dial(at); err == nil {
		c.Close()
		t.Fatalf("a client that found no service at %s brought one into being", at)
	}
}

// S2. Asking for a supervisor when one is there is answered by the one that is
// there.
//
// This is what makes an explicit start safe to call from an installer, a logon
// entry and a tool at the same time, and it is the platform's answer rather
// than ours: the endpoint admits one listener, so the second starter loses by
// name instead of by timing.
func TestStartingASupervisorThatIsAlreadyThereStartsNothing(t *testing.T) {
	_, store, _ := newRunner(t)
	b := announce(t, store, "jobd@test:1", "here")

	exe, args := neverRuns(t)
	got, err := StartSupervisor(Starting{
		Endpoint: b.Endpoint,
		Exe:      exe,
		Args:     args,
		Log:      filepath.Join(t.TempDir(), "start.log"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Already {
		t.Fatal("a start claimed to have started a supervisor that was already listening")
	}
	if got.Owner != "jobd@test:1" {
		t.Fatalf("the start named %q, not the supervisor that answered", got.Owner)
	}
	if got.PID != 0 {
		t.Fatalf("a start that had nothing to do ran pid %d", got.PID)
	}
}

// S3. A start that spawned something which never listened says so, and says it
// when the child dies rather than when a clock runs out.
//
// The 300ms sleep this replaces (research/bus206/RESULTS.md § 1.6) was a delay
// standing in for a notification: it covered a stop-then-start race by guessing
// how long a dying process holds its handles. Waiting on the child is a wait on
// something, and the ceiling below is only reached by a child that neither
// answers nor exits.
func TestAStartWhoseChildNeverListensSaysSoWhenItDies(t *testing.T) {
	at := endpoint(t)
	exe, args := neverRuns(t)
	began := time.Now()
	got, err := StartSupervisor(Starting{
		Endpoint: at,
		Exe:      exe,
		Args:     args,
		Log:      filepath.Join(t.TempDir(), "start.log"),
		Within:   30 * time.Second,
	})
	if !errors.Is(err, ErrNeverAnswered) {
		t.Fatalf("got %v, want a supervisor that never answered", err)
	}
	if got.PID == 0 {
		t.Fatal("nothing was spawned, so there was nothing to wait on")
	}
	if took := time.Since(began); took > 25*time.Second {
		t.Fatalf("the start waited %s for a child that had already exited", took)
	}
}

// neverRuns is a program that exists and does not serve: the child's exit is
// the signal, so the test needs something that spawns and then leaves, not
// something that fails to spawn. This binary run with a pattern no test matches
// is exactly that.
func neverRuns(t *testing.T) (string, []string) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return self, []string{"-test.run=^$"}
}

func TestAStartWithNoNameIsRefused(t *testing.T) {
	exe, args := neverRuns(t)
	_, err := StartSupervisor(Starting{Exe: exe, Args: args})
	if !errors.Is(err, ErrNoName) {
		t.Fatalf("got %v, want ErrNoName", err)
	}
}
