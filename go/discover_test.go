package download

import (
	"fmt"
	"os"
	"strings"
	"testing"

	config "github.com/ReinisLusis/abstraction/config/go"
)

// An application that knows nothing gets the right tier, and the test is written
// the way Lemonade would be: no path, no hostname, no branch.
func TestDiscoverPicksTheTierTheMachineHas(t *testing.T) {
	_, store, _ := newRunner(t)

	// A machine with nothing set up.
	tiersMu.Lock()
	saved := tiers
	tiers = nil
	tiersMu.Unlock()
	t.Cleanup(func() { tiersMu.Lock(); tiers = saved; tiersMu.Unlock() })

	r := DiscoverIn(store)
	if got := r.Tier(); got != "here" {
		t.Fatalf("with nothing installed, Tier() = %q, want here", got)
	}

	// Now a tier registers itself, exactly as a binding's init would.
	RegisterTier(Tier{
		Name: "pretend-nas", Priority: 10,
		New: func(config.Config) (Delegator, error) { return &fakeDelegate{jobs: map[string]*fakeJob{}}, nil },
	})
	r = DiscoverIn(store)
	if got := r.Tier(); got != "fake-service" {
		t.Fatalf("Tier() = %q; the registered tier was not picked up", got)
	}
}

// Priority is what makes the chain a chain: a NAS outranks the OS service
// because it is always on and this machine is not.
func TestTiersAreOfferedWorkInPriorityOrder(t *testing.T) {
	tiersMu.Lock()
	saved := tiers
	tiers = nil
	tiersMu.Unlock()
	t.Cleanup(func() { tiersMu.Lock(); tiers = saved; tiersMu.Unlock() })

	RegisterTier(Tier{Name: "second", Priority: 20,
		New: func(config.Config) (Delegator, error) { return &fakeDelegate{jobs: map[string]*fakeJob{}}, nil }})
	RegisterTier(Tier{Name: "first", Priority: 10,
		New: func(config.Config) (Delegator, error) { return &fakeDelegate{jobs: map[string]*fakeJob{}}, nil }})

	got := RegisteredTiers()
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("registration order = %v, want [first second] regardless of when each registered", got)
	}
}

// A tier that cannot be used here must not be registered at all. Writing a job
// into a directory nobody is watching looks exactly like a download that
// started, and it would sit there forever.
func TestUnavailableTierIsNotOffered(t *testing.T) {
	_, store, _ := newRunner(t)
	tiersMu.Lock()
	saved := tiers
	tiers = nil
	tiersMu.Unlock()
	t.Cleanup(func() { tiersMu.Lock(); tiers = saved; tiersMu.Unlock() })

	RegisterTier(Tier{Name: "switched-off", Priority: 10,
		New: func(config.Config) (Delegator, error) {
			return nil, errNotHere
		}})

	if got := DiscoverIn(store).Tier(); got != "here" {
		t.Fatalf("Tier() = %q; an unreachable tier was offered work", got)
	}
}

var errNotHere = errTest("not available on this machine")

type errTest string

func (e errTest) Error() string { return string(e) }

// The program name comes from the OS, not from the caller.
//
// Discover used to take one — download.Discover("lemonade") — and that was a
// claim, in a project that has spent considerable effort establishing that
// claims are the weak kind of identity. It was also a trap: an integration
// snippet copied from one project into another leaves every lease in the store
// owned by "lemonade", and nobody notices until they are trying to work out
// which program is holding a job.
func TestOwnerNamesTheRealExecutable(t *testing.T) {
	prog := Program()
	if prog == "" || prog == "unknown" {
		t.Fatalf("Program() = %q; a store full of leases owned by nobody cannot be debugged", prog)
	}
	// Go's test binary is named after the package under test.
	if !strings.Contains(prog, "abstraction-download") && !strings.Contains(prog, "test") {
		t.Fatalf("Program() = %q, which does not look like this executable", prog)
	}
	if strings.HasSuffix(prog, ".exe") {
		t.Fatalf("Program() = %q; the extension is noise in a lease owner", prog)
	}

	owner := Owner()
	if !strings.HasPrefix(owner, prog+"@") {
		t.Fatalf("Owner() = %q, want it to start with %q", owner, prog+"@")
	}
	// Host and pid both matter: a lease held on another machine cannot be
	// broken by asking whether that pid is alive here.
	if !strings.Contains(owner, "@") || !strings.Contains(owner, ":") {
		t.Fatalf("Owner() = %q, want program@host:pid", owner)
	}
	if !strings.HasSuffix(owner, fmt.Sprintf(":%d", os.Getpid())) {
		t.Fatalf("Owner() = %q, want it to end with this pid", owner)
	}
}
