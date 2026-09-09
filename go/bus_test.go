package download

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
	job "github.com/openabstractions/abstraction-job/go"
)

// announce is a supervisor that answers: a bus this test serves, named in the
// heartbeat the way jobd names it.
func announce(t *testing.T, store job.Store, owner, tier string) *Bus {
	t.Helper()
	b, err := ListenBus(owner, func() string { return tier })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := Heartbeat(store, owner, tier, b.Endpoint, time.Minute); err != nil {
		t.Fatal(err)
	}
	return b
}

// B1. A look from an identified caller wakes the supervisor, and the caller
// hears back before it returns.
func TestALookFromAnIdentifiedCallerWakesTheSupervisor(t *testing.T) {
	_, store, _ := newRunner(t)
	b := announce(t, store, "jobd@test:1", "here")
	if err := Nudge(store); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.C():
	default:
		t.Fatal("an answered look left no wakeup pending")
	}
}

// B2. Twenty looks pending at once are one sweep.
func TestLooksCoalesce(t *testing.T) {
	_, store, _ := newRunner(t)
	b := announce(t, store, "jobd@test:1", "here")
	for i := 0; i < 20; i++ {
		if err := Nudge(store); err != nil {
			t.Fatal(err)
		}
	}
	<-b.C()
	select {
	case <-b.C():
		t.Fatal("looks are queued rather than coalesced; a caller can force a sweep per message")
	default:
	}
}

// B3. Who answers with the supervisor's name and the caller as the kernel saw
// it, which is this test binary.
func TestWhoNamesTheCaller(t *testing.T) {
	_, store, _ := newRunner(t)
	announce(t, store, "jobd@test:1", "nas")
	a, err := Who(store)
	if err != nil {
		t.Fatal(err)
	}
	if a.Owner != "jobd@test:1" || a.Tier != "nas" {
		t.Fatalf("answered as %q delegating to %q", a.Owner, a.Tier)
	}
	if !a.Caller.Bound {
		t.Fatalf("the caller was not bound: %s", a.Caller)
	}
	exe, _ := os.Executable()
	if !strings.Contains(strings.ToLower(a.Caller.Path), strings.ToLower(filepath.Base(exe))) {
		t.Fatalf("the supervisor saw %q; this process is %s", a.Caller.Path, exe)
	}
}

type unbound struct{ net.Conn }

func (unbound) Bind() (*identity.Binding, error) { return nil, identity.ErrNoBinding }

// B4. A caller the kernel cannot name is refused on every op, is told why, and
// wakes nothing.
func TestACallerTheKernelCannotNameIsRefused(t *testing.T) {
	b, err := ListenBus("jobd@test:1", func() string { return "here" })
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, op := range []string{"look", "who"} {
		server, client := net.Pipe()
		go b.serve(unbound{server})
		if _, err := client.Write([]byte(`{"op":"` + op + `"}` + "\n")); err != nil {
			t.Fatal(err)
		}
		var a Answer
		if err := json.NewDecoder(client).Decode(&a); err != nil {
			t.Fatal(err)
		}
		client.Close()
		if !strings.HasPrefix(a.Error, "jobd: refused, ") || !strings.Contains(a.Error, identity.ErrNoBinding.Error()) {
			t.Errorf("%s from a caller with no identity: %+v", op, a)
		}
		if a.Caller.Bound || a.Owner != "" {
			t.Errorf("%s: a refused caller learned something: %+v", op, a)
		}
	}
	select {
	case <-b.C():
		t.Fatal("a refused look woke the supervisor")
	default:
	}
}

// B5. Nothing announced is no supervisor, answered without dialling anything.
func TestNothingAnnouncedIsNoSupervisor(t *testing.T) {
	_, store, _ := newRunner(t)
	start := time.Now()
	if err := Nudge(store); !errors.Is(err, ErrNoSupervisor) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Nudge waited on nothing")
	}
}

// B6. A heartbeat outlives the process that wrote it; an endpoint does not.
func TestAnAnnouncedBusNobodyServesIsNoSupervisor(t *testing.T) {
	_, store, _ := newRunner(t)
	b := announce(t, store, "jobd@test:1", "here")
	b.Close()
	if err := Nudge(store); !errors.Is(err, ErrNoSupervisor) {
		t.Fatalf("a dead bus answered: %v", err)
	}
	if _, live := SupervisorOf(store); !live {
		t.Fatal("the heartbeat still says live; the bus is what says otherwise")
	}
}

// B7. A supervisor without a bus, or on another machine, is reached through
// the store only, and that is not the same as being gone.
func TestASupervisorWithoutABusIsReachedThroughTheStore(t *testing.T) {
	_, store, _ := newRunner(t)
	if err := Heartbeat(store, "jobd@host:1", "here", "", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := Nudge(store); !errors.Is(err, ErrNoBus) {
		t.Fatalf("no endpoint: %v", err)
	}
	announce(t, store, "jobd@test:1", "here")
	rewriteHeartbeat(t, store, func(s *Supervisor) { s.Host = "a-machine-that-is-not-this-one" })
	if err := Nudge(store); !errors.Is(err, ErrNoBus) {
		t.Fatalf("another host: %v", err)
	}
}

func failingSpec() Spec {
	return Spec{
		Sources: []Source{{Scheme: "http", Locator: "http://127.0.0.1:1/x.bin"}},
		Sink:    Sink{Final: "out/x.bin"},
	}
}

// B8. The heartbeat predicts and the connection decides: a submit whose
// announced supervisor does not answer is worked here.
func TestASubmitIsWorkedHereWhenTheAnnouncedSupervisorDoesNotAnswer(t *testing.T) {
	_, store, _ := newRunner(t)
	b := announce(t, store, "jobd@test:1", "here")
	b.Close()
	svc := NewClient(NewRunner(store, "app"))
	h, err := svc.Submit(failingSpec())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = svc.Deliver(ctx, h.ID())
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("nobody worked the job: it was handed to a supervisor that was gone")
	}
	rec, err := store.Load(h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if rec.Error == "" && !rec.State.Terminal() {
		t.Fatalf("the record shows nobody worked it: %+v", rec)
	}
}

// B8, the other half: a supervisor that answers is handed the job and this
// process starts nothing.
func TestASubmitIsHandedToASupervisorThatAnswers(t *testing.T) {
	_, store, _ := newRunner(t)
	b := announce(t, store, "jobd@test:1", "here")
	svc := NewClient(NewRunner(store, "app"))
	h, err := svc.Submit(failingSpec())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.C():
	default:
		t.Fatal("the submit did not reach the supervisor")
	}
	rec, err := store.Load(h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !store.Claimable(rec) || rec.Error != "" {
		t.Fatalf("this process took the job a supervisor had answered for: %+v", rec)
	}
}
