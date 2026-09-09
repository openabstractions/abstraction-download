package nas

import (
	"context"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
)

// A handoff whose answer was lost must still be findable, or the facade falls
// back and the NAS and this machine fetch the same artifact at the same time.
func TestLocateFindsWorkTheDelegateAccepted(t *testing.T) {
	m := setup(t)
	id := m.submit(t, "model.gguf")

	if err := m.runner.Delegate(context.Background(), id); err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	rec, err := m.local.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Delegated() {
		t.Fatal("setup: nothing was delegated")
	}

	// The request identity is the local record's own id, which is what a later
	// process — after a reboot, in another language — has to work from.
	found, err := m.del.Locate(context.Background(), id)
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if found != rec.Delegation.ExternalID {
		t.Fatalf("Locate = %q, the delegate is running %q", found, rec.Delegation.ExternalID)
	}
}

// "I never took it" is a positive claim, and the facade is free to fall back on
// it. It must not be the answer given when the share cannot be read.
func TestLocateIsCertainAboutWorkItNeverTook(t *testing.T) {
	m := setup(t)

	found, err := m.del.Locate(context.Background(), "a-request-nobody-made")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if found != "" {
		t.Fatalf("Locate invented a handle %q for work it never accepted", found)
	}
}

func TestLocateRefusesAnEmptyRequest(t *testing.T) {
	m := setup(t)
	if _, err := m.del.Locate(context.Background(), ""); err == nil {
		t.Fatal("an empty request matched something")
	}
}

// The delegate must be a Locator at all: the facade discovers this by type
// assertion, and a delegate that silently is not one gets its lost handoffs
// treated as refusals again.
func TestDelegatorIsALocator(t *testing.T) {
	var d any = &Delegator{}
	if _, ok := d.(download.Locator); !ok {
		t.Fatal("nas.Delegator does not implement download.Locator")
	}
}
