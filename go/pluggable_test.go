package download

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	config "github.com/openabstractions/abstraction-config/go"
	identity "github.com/openabstractions/abstraction-identity"
)

// A tier somebody else published, which counts what it was asked and closes
// what it opened. Everything in this file is about those two facts.
type outsider struct {
	declaration Declaration
	asked       *int
	closed      *int
	live        *int
}

func (o outsider) System() string    { return "outsider" }
func (o outsider) Schemes() []string { return []string{"https"} }
func (o outsider) Capabilities() []Capability {
	return []Capability{CapSurvivesProcessExit}
}
func (o outsider) Declared() Declaration { *o.asked++; return o.declaration }
func (o outsider) Close() error          { *o.closed++; *o.live--; return nil }

func (o outsider) Start(context.Context, Spec, int64) (string, error) {
	return "", fmt.Errorf("the delegate was handed a job it declared it could never do")
}
func (o outsider) Poll(context.Context, string) (Status, error) { return Status{}, nil }
func (o outsider) Finalize(context.Context, string, string) error {
	return nil
}
func (o outsider) Abandon(context.Context, string) error { return nil }

// register puts an outsider at the best rank and returns its counters.
func register(t *testing.T, d Declaration) (asked, closed, live *int) {
	t.Helper()
	asked, closed, live = new(int), new(int), new(int)
	RegisterTier(Tier{
		Name:      "outsider",
		Priority:  -1,
		Over:      OverForeign,
		Facility:  "an engine of their own",
		Publisher: Publisher{Name: "someone else", Proof: identity.ProofClaimed},
		New: func(config.Config) (Delegator, error) {
			*live++
			return outsider{d, asked, closed, live}, nil
		},
	})
	t.Cleanup(func() { UnregisterTier("outsider") })
	return asked, closed, live
}

func aJob(size int64, locator string) Spec {
	return Spec{
		Artifact: Artifact{Digest: "sha256:00", Size: size},
		Sources:  []Source{{Scheme: "https", Locator: locator}},
	}
}

// The one that has to hold: removing a tier leaves the layer doing exactly what
// it did before that tier existed. Registration was append-only, so a plugin a
// person deleted stayed until the process restarted.
func TestRemovingATierLeavesTheLayerExactlyAsItWas(t *testing.T) {
	cfg := config.Config{}
	before, beforeOffers := RegisteredTiers(), Offers(cfg)

	register(t, Declaration{})
	if reflect.DeepEqual(RegisteredTiers(), before) {
		t.Fatal("registering changed nothing; the rest of this test proves nothing")
	}

	if !UnregisterTier("outsider") {
		t.Fatal("UnregisterTier did not find a tier that was registered")
	}
	if got := RegisteredTiers(); !reflect.DeepEqual(got, before) {
		t.Fatalf("tiers after removal = %v, want %v", got, before)
	}
	if got := Offers(cfg); !reflect.DeepEqual(got, beforeOffers) {
		t.Fatalf("offers after removal = %v, want %v", got, beforeOffers)
	}
	if UnregisterTier("outsider") {
		t.Fatal("UnregisterTier claimed to remove a tier twice")
	}
}

// A name is what Delegation.System records, so two tiers may not answer to one.
func TestRegisteringTwiceUnderOneNameReplaces(t *testing.T) {
	before := len(RegisteredTiers())
	register(t, Declaration{})
	register(t, Declaration{})
	if got := len(RegisteredTiers()) - before; got != 1 {
		t.Fatalf("registering one name twice left %d extra tiers, want 1", got)
	}
}

// The centre of the design: the core evaluates what the delegate declared once,
// and the delegate is never shown the job in order to refuse it.
func TestADeclarationIsEvaluatedHereAndTheDelegateIsNeverShownTheJob(t *testing.T) {
	for _, c := range []struct {
		name    string
		declare Declaration
		spec    Spec
		serve   bool
	}{
		{"a host that names this machine", Declaration{Hosts: HostRoutable},
			aJob(10, "https://127.0.0.1/m"), false},
		{"a host somewhere else", Declaration{Hosts: HostRoutable},
			aJob(10, "https://example.invalid/m"), true},
		{"larger than it will take", Declaration{MaxSize: 1 << 20},
			aJob(2<<20, "https://example.invalid/m"), false},
		{"smaller than it is worth", Declaration{MinSize: 1 << 20},
			aJob(10, "https://example.invalid/m"), false},
		{"a size nobody knows", Declaration{MaxSize: 1 << 20},
			aJob(0, "https://example.invalid/m"), true},
		{"a digest it cannot compute", Declaration{Digests: []string{"blake3"}},
			aJob(10, "https://example.invalid/m"), false},
		{"a digest it can", Declaration{Digests: []string{"sha256"}},
			aJob(10, "https://example.invalid/m"), true},
		{"nothing declared", Declaration{},
			aJob(10, "https://127.0.0.1/m"), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			asked := new(int)
			d := outsider{c.declare, asked, new(int), new(int)}
			ds := NewDelegators(d)
			got, ok := ds.ForSpec(c.spec, c.spec.Sources[0], nil)
			if ok != c.serve {
				t.Fatalf("served = %v, want %v", ok, c.serve)
			}
			if ok && got.System() != "outsider" {
				t.Fatalf("served by %q", got.System())
			}
			if *asked == 0 {
				t.Fatal("the declaration was never read; the filter is not the thing deciding")
			}
			if c.serve {
				return
			}
			if why := ds.WhyNot(c.spec, c.spec.Sources[0], nil); len(why) != 1 ||
				why[0].System != "outsider" {
				t.Fatalf("nothing names who refused: %v", why)
			}
		})
	}
}

// A closed vocabulary that reads an unknown term as "no constraint" is not
// closed: it hands the delegate exactly the jobs the term was written to keep
// away from it.
func TestADeclarationThisCoreCannotReadMakesTheTierUnusable(t *testing.T) {
	register(t, Declaration{Hosts: HostShape("whatever-the-next-version-adds")})
	for _, o := range Offers(config.Config{}) {
		if o.System != "outsider" {
			continue
		}
		if o.Usable {
			t.Fatal("a tier declaring a term this core cannot evaluate was offered work")
		}
		if o.Why == "" {
			t.Fatal("nothing a person could read says why")
		}
		return
	}
	t.Fatal("the tier was never offered at all; the guard was not what stopped it")
}

// Tier.New IS the probe and runs again on every Offers and every Rebind. A
// delegate that owns a process leaks one per call without this.
func TestEveryDelegateAProbeBuiltIsClosed(t *testing.T) {
	_, closed, live := register(t, Declaration{})

	cfg := config.Config{}
	Offers(cfg)
	Offers(cfg)
	if *closed != 2 {
		t.Fatalf("Offers built 2 delegates and closed %d", *closed)
	}
	if *live != 0 {
		t.Fatalf("%d delegates are still open after asking for a report", *live)
	}

	r := &Runner{}
	r.Rebind()
	r.Rebind()
	if *live != 1 {
		t.Fatalf("%d delegates live after two rebinds, want 1", *live)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if *live != 0 {
		t.Fatalf("%d delegates live after Close", *live)
	}
}

// An operator running one tier lower discards what it drops, and a discarded
// probe still holds whatever the probe acquired.
func TestDroppingATierClosesIt(t *testing.T) {
	closed, live := new(int), new(int)
	d := outsider{Declaration{}, new(int), closed, live}
	*live = 1
	NewDelegators(d).Without("outsider")
	if *closed != 1 || *live != 0 {
		t.Fatalf("dropped delegate closed=%d live=%d", *closed, *live)
	}
}

// Attribution: a person is entitled to read that the answer is a program
// somebody else wrote, and to see how firmly that name is known.
func TestAnOfferNamesWhoPublishedTheTier(t *testing.T) {
	register(t, Declaration{})
	var seen bool
	for _, o := range Offers(config.Config{}) {
		if o.System == "outsider" {
			seen = true
			if o.By.Name != "someone else" {
				t.Fatalf("published by %q", o.By.Name)
			}
			if o.By.Proof != identity.ProofClaimed {
				t.Fatalf("a name out of a manifest is graded %q", o.By.Proof)
			}
			continue
		}
		if o.By.Name != Program() || o.By.Proof != identity.ProofBound {
			t.Fatalf("a linked tier is published by %v", o.By)
		}
	}
	if !seen {
		t.Fatal("the tier under test was never offered")
	}
}

// The two endings are the whole retry model, and errors.Is does not survive
// JSON. A refusal that crosses as an ordinary failure is a job retried forever.
func TestAFailureKeepsItsClassAcrossAWire(t *testing.T) {
	for _, c := range []struct {
		err       error
		permanent bool
	}{
		{fmt.Errorf("%w: gated repository", ErrRefused), true},
		{ErrShortTransfer, false},
		{nil, false},
	} {
		b, err := json.Marshal(FailureOf(c.err))
		if err != nil {
			t.Fatal(err)
		}
		var got *Failure
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		back := got.Err()
		if c.err == nil {
			if back != nil {
				t.Fatalf("nil became %v", back)
			}
			continue
		}
		if back.Error() != c.err.Error() {
			t.Fatalf("text %q became %q", c.err, back)
		}
		if Permanent(back) != c.permanent {
			t.Fatalf("%q crossed as permanent=%v, want %v", c.err, Permanent(back), c.permanent)
		}
	}
}
