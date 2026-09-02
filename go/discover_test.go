package download

import (
	"testing"

	config "github.com/ReinisLusis/abstraction-config"
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

	r := DiscoverIn("lemonade", store)
	if got := r.Tier(); got != "here" {
		t.Fatalf("with nothing installed, Tier() = %q, want here", got)
	}

	// Now a tier registers itself, exactly as a binding's init would.
	RegisterTier(Tier{
		Name: "pretend-nas", Priority: 10,
		New: func(config.Config) (Delegator, error) { return &fakeDelegate{jobs: map[string]*fakeJob{}}, nil },
	})
	r = DiscoverIn("lemonade", store)
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

	if got := DiscoverIn("lemonade", store).Tier(); got != "here" {
		t.Fatalf("Tier() = %q; an unreachable tier was offered work", got)
	}
}

var errNotHere = errTest("not available on this machine")

type errTest string

func (e errTest) Error() string { return string(e) }
