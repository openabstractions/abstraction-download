package download

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	config "github.com/openabstractions/abstraction-config/go"
	identity "github.com/openabstractions/abstraction-identity"
	job "github.com/openabstractions/abstraction-job/go"
)

const liarSystem = "fastdl"

// restartingDelegate is the delegate this whole file exists for: it claims it
// can resume, takes the offset, and starts again from zero. The bytes it
// delivers are correct, so nothing that looks at the file will ever notice.
type restartingDelegate struct {
	from      int64
	done      int64
	abandoned []string
}

func (d *restartingDelegate) System() string    { return liarSystem }
func (d *restartingDelegate) Close() error      { return nil }
func (d *restartingDelegate) Schemes() []string { return []string{"https"} }
func (d *restartingDelegate) Capabilities() []Capability {
	return []Capability{CapResume, CapSurvivesProcessExit, CapDelegates}
}

func (d *restartingDelegate) Start(_ context.Context, _ Spec, from int64) (string, error) {
	d.from = from
	return "handle-1", nil
}

func (d *restartingDelegate) Poll(context.Context, string) (Status, error) {
	return Status{State: DelegateRunning, Done: d.done, Total: 16 << 20}, nil
}

func (d *restartingDelegate) Finalize(context.Context, string, string) error { return nil }

func (d *restartingDelegate) Abandon(_ context.Context, id string) error {
	d.abandoned = append(d.abandoned, id)
	return nil
}

// stageProven leaves the record a previous owner left: bytes proven here, on
// this machine's disk, and nobody holding the lease.
func stageProven(t *testing.T, store job.Store, id string, verified int64) {
	t.Helper()
	held, err := store.Claim(id, "previous-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(id, held.Lease.Epoch, func(rr *job.Record) error {
		return rr.SetCheckpoint(Checkpoint{VerifiedPrefix: verified})
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(id, held.Lease.Epoch); err != nil {
		t.Fatal(err)
	}
}

// The kill condition. A plugin that claims resumption and starts from zero is
// caught, named, and not believed again — and the bytes it was about to waste
// stay where they were proven.
//
// Measured before it was hypothetical: 6,586,368 checkpointed bytes were thrown
// away exactly this way, and the remedy at the time was to edit our own tier's
// source. That remedy does not exist for a program somebody else wrote.
func TestAPluginThatClaimedResumeAndRestartedFromZeroIsCaughtAndDisbelieved(t *testing.T) {
	const proven = 6586368
	_, digest := payload(t, 64)
	r, store, root := newRunner(t)
	liar := &restartingDelegate{}
	r.Delegators = NewDelegators(liar)
	RegisterTier(Tier{
		Name:      liarSystem,
		Priority:  5,
		Publisher: Publisher{Name: "acme", Proof: identity.ProofClaimed},
		New:       func(config.Config) (Delegator, error) { return liar, nil },
	})
	t.Cleanup(func() { UnregisterTier(liarSystem) })

	id := submit(t, store, root, digest, 16<<20, Source{Scheme: "https", Locator: "https://example.invalid/x"})
	stageProven(t, store, id, proven)

	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	if liar.from != proven {
		t.Fatalf("the delegate was handed %d as its resume offset, want %d", liar.from, proven)
	}

	liar.done = 1 << 20 // it began again from zero, and is a megabyte in
	err := r.Reconcile(context.Background(), id)
	if !errors.Is(err, ErrClaimFalsified) {
		t.Fatalf("Reconcile = %v, want the claim refuted", err)
	}
	if !strings.Contains(err.Error(), liarSystem) || !strings.Contains(err.Error(), "acme") {
		t.Fatalf("the refusal does not name the plugin or its publisher: %v", err)
	}

	rec, lerr := store.Load(id)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if rec.Delegated() || rec.State != job.StatePending {
		t.Fatalf("the job was left with the delegate: delegated=%v state=%s", rec.Delegated(), rec.State)
	}
	cp, cerr := CheckpointOf(rec)
	if cerr != nil {
		t.Fatal(cerr)
	}
	if cp.VerifiedPrefix != proven {
		t.Fatalf("verified_prefix is %d, want the %d bytes this was all about", cp.VerifiedPrefix, proven)
	}
	if len(liar.abandoned) != 1 {
		t.Fatalf("the work being done twice was not cancelled: %v", liar.abandoned)
	}
	if !strings.Contains(rec.Error, liarSystem) {
		t.Fatalf("the record does not say who did this: %q", rec.Error)
	}

	// Disbelieved from then on, which is the half that makes it a remedy rather
	// than a report: the next sweep must not hand the same job straight back.
	if got := Disbelieved(liarSystem); len(got) != 1 || got[0] != CapResume {
		t.Fatalf("disbelieved claims = %v, want [%s]", got, CapResume)
	}
	again := r.Delegate(context.Background(), id)
	if !errors.Is(again, ErrNoDelegator) {
		t.Fatalf("Delegate = %v, want nothing eligible for a job with proven bytes", again)
	}
	if !strings.Contains(again.Error(), "caught not doing it") {
		t.Fatalf("the person is not told why the tier they installed stopped serving: %v", again)
	}

	// A job from zero requires nothing of it, so it still serves: it lied about
	// resuming, not about downloading.
	fresh := submit(t, store, root, digest, 16<<20, Source{Scheme: "https", Locator: "https://example.invalid/y"})
	if err := r.Delegate(context.Background(), fresh); err != nil {
		t.Fatalf("Delegate from zero: %v, want the tier still usable", err)
	}
}

// The other side of the instrument, and the reason it is one comparison rather
// than a policy: a delegate that has not started reports zero and is not lying,
// and a delegate that resumed reports at least the offset it was given.
func TestADelegateThatDidNotLieIsNotAccused(t *testing.T) {
	const proven = 4096
	body, digest := payload(t, 16<<10)
	r, store, fd, root := newDelegatingRunner(t, body)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})
	stageProven(t, store, id, proven)
	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	handle := fd.handleOf(t, store, id)

	for _, done := range []int64{0, proven, proven + 1024} {
		fd.advance(handle, done, DelegateRunning)
		if err := r.Reconcile(context.Background(), id); err != nil {
			t.Fatalf("a delegate reporting %d of %d proven was accused: %v", done, proven, err)
		}
	}
	if got := Disbelieved("fake-service"); len(got) != 0 {
		t.Fatalf("an honest delegate was disbelieved: %v", got)
	}
	if rec, _ := store.Load(id); !rec.Delegated() {
		t.Fatal("the job was taken back from a delegate that did nothing wrong")
	}
}

// Every capability says what a false claim costs. The roster is the vocabulary,
// and a claim nobody grades is a promise that reads exactly like a measurement.
func TestEveryCapabilitySaysWhatAFalseClaimCosts(t *testing.T) {
	want := map[Capability]Assurance{
		CapVerifies:            Checked,
		CapResume:              Falsifiable,
		CapSurvivesProcessExit: Recovered,
		CapDelegates:           Trusted,
	}
	all := AllCapabilities()
	if len(all) != len(want) {
		t.Fatalf("the roster holds %v; every one of them needs a grade in this test", all)
	}
	for c, w := range want {
		got, ok := Assured(c)
		if !ok || got != w {
			t.Fatalf("%s is graded %q (known=%v), want %q", c, got, ok, w)
		}
	}
	if _, ok := Assured(Capability("whatever-the-next-version-adds")); ok {
		t.Fatal("a word this core has never heard of came back graded")
	}
	// Unknown grants nothing rather than everything, which is the whole reason
	// an ungraded claim can be ignored where an unreadable constraint cannot.
	if hasAllCaps([]Capability{"whatever-the-next-version-adds"}, []string{"resume"}) {
		t.Fatal("an unknown claim satisfied a requirement")
	}
}

// A probe is code execution now, and a program can accept a request and never
// answer. Offers is what a control surface polls, so it must come back.
func TestAProbeThatNeverAnswersDoesNotFreezeOffers(t *testing.T) {
	budget := ProbeBudget
	ProbeBudget = 20 * time.Millisecond
	t.Cleanup(func() { ProbeBudget = budget })

	answer := make(chan struct{})
	closed := make(chan struct{})
	RegisterTier(Tier{
		Name:     "silent",
		Priority: 7,
		New: func(config.Config) (Delegator, error) {
			<-answer
			return &lateDelegate{closed: closed}, nil
		},
	})
	t.Cleanup(func() { UnregisterTier("silent") })

	var seen bool
	for _, o := range Offers(config.Config{}) {
		if o.System != "silent" {
			continue
		}
		seen = true
		if o.Usable {
			t.Fatal("a tier that never answered its probe was offered work")
		}
		if !strings.Contains(o.Why, "did not answer") {
			t.Fatalf("the person reads %q, which does not say the tier is not answering", o.Why)
		}
	}
	if !seen {
		t.Fatal("the tier under test was never offered at all")
	}

	// What arrives after the wait was abandoned is closed, not leaked. Waiting
	// on the close rather than on a clock is the whole difference between a
	// deadline and a sleep.
	close(answer)
	<-closed
}

type lateDelegate struct{ closed chan struct{} }

func (d *lateDelegate) System() string             { return "silent" }
func (d *lateDelegate) Close() error               { close(d.closed); return nil }
func (d *lateDelegate) Schemes() []string          { return []string{"https"} }
func (d *lateDelegate) Capabilities() []Capability { return nil }
func (d *lateDelegate) Start(context.Context, Spec, int64) (string, error) {
	return "", errors.New("never")
}
func (d *lateDelegate) Poll(context.Context, string) (Status, error) { return Status{}, nil }
func (d *lateDelegate) Finalize(context.Context, string, string) error {
	return nil
}
func (d *lateDelegate) Abandon(context.Context, string) error { return nil }
