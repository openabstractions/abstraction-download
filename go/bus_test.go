package download

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	job "github.com/openabstractions/abstraction-job/go"
)

// endpoint is one test's own name for a bus, the way asks and rights each give
// their services one: the process and the test are the scope, so two runs of
// this binary — and two of these tests — never ask for one name.
func endpoint(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "windows" {
		return filepath.Join(t.TempDir(), "bus")
	}
	return fmt.Sprintf(`\\.\pipe\jobs-test-%d-%s`, os.Getpid(), t.Name())
}

// announce is a supervisor that answers: a bus this test serves, named in the
// heartbeat the way jobd names it.
func announce(t *testing.T, store job.Store, owner, tier string) *Bus {
	t.Helper()
	b, err := ListenBus(endpoint(t), owner, func() string { return tier })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := Heartbeat(store, owner, tier, b.Endpoint, time.Minute); err != nil {
		t.Fatal(err)
	}
	return b
}

// N1. The name is the same in another process. Everything the service work
// waits on — a trigger, a socket unit, a security descriptor, an installer —
// is written once by one program and used by another, so a name this process
// invented for itself is no name at all.
func TestTheNameIsTheSameInAnotherProcess(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(self, "-test.run=^"+childName+"$", "-test.v").CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	i := strings.Index(string(out), childSays)
	if i < 0 {
		t.Fatalf("the child said nothing about its endpoint:\n%s", out)
	}
	there := strings.TrimSpace(strings.SplitN(string(out[i+len(childSays):]), "\n", 2)[0])
	if there != DefaultEndpoint() {
		t.Fatalf("this process listens at %q and another one would listen at %q", DefaultEndpoint(), there)
	}
}

const (
	childName = "TestTheEndpointThisProcessWouldListenAt"
	childSays = "endpoint="
)

// The other half of N1: N1 runs this in a second process and compares what it
// prints with the name this process would listen at.
func TestTheEndpointThisProcessWouldListenAt(t *testing.T) {
	if DefaultEndpoint() == "" {
		t.Fatal("this process has no name to listen at")
	}
	fmt.Println(childSays + DefaultEndpoint())
}

// N2. One holder per name. Two supervisors sharing an address would each hear
// half the callers, and neither could say so.
func TestASecondSupervisorAtOneNameIsRefused(t *testing.T) {
	at := endpoint(t)
	first, err := ListenBus(at, "jobd@test:1", func() string { return "here" })
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := ListenBus(at, "jobd@test:2", func() string { return "here" })
	if err == nil {
		second.Close()
		t.Fatal("two supervisors took one name")
	}
	if !errors.Is(err, listen.ErrTaken) {
		t.Fatalf("a held name was refused for the wrong reason: %v", err)
	}
}

// N3. The scopes are different names, so a machine service and a user runtime
// on one machine do not fight over one.
func TestTheScopesAreDifferentNames(t *testing.T) {
	if UserScope.Endpoint() == MachineScope.Endpoint() {
		t.Fatalf("both scopes listen at %s", UserScope.Endpoint())
	}
}

// N3, the half that matters on a machine with two people on it: one account's
// user-scope name is not another's. A pipe name is machine-wide, so on Windows
// the account is in the name; elsewhere the runtime directory is already one
// per account and the name sits inside it.
func TestTwoAccountsDoNotShareTheUserScopeName(t *testing.T) {
	if runtime.GOOS == "windows" {
		if Account() == "" || !strings.Contains(UserScope.Endpoint(), Account()) {
			t.Fatalf("the name %q does not say whose account it is (%q)", UserScope.Endpoint(), Account())
		}
		return
	}
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), "one"))
	mine := UserScope.Endpoint()
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), "two"))
	if theirs := UserScope.Endpoint(); theirs == mine {
		t.Fatalf("two accounts both listen at %s", mine)
	}
}

// N4. A bus with no name refuses to listen. Scope.Endpoint is empty when the
// platform would not say which account this is, and every process that could
// not say would otherwise agree on one name.
func TestABusWithNoNameRefusesToListen(t *testing.T) {
	b, err := ListenBus("", "jobd@test:1", func() string { return "here" })
	if err == nil {
		b.Close()
		t.Fatal("a bus with no name listened")
	}
	if !errors.Is(err, ErrNoName) {
		t.Fatalf("got %v", err)
	}
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
	b, err := ListenBus(endpoint(t), "jobd@test:1", func() string { return "here" })
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
