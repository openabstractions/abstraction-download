package download

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// acceptedThenLost takes the work and then loses the answer on the way back.
//
// This is the case every delegate we ship has and none of them could report: a
// NAS whose record write landed on the share before the reply was dropped, a
// BITS job created and resumed by a PowerShell that died before its stdout was
// read. The transfer is running on the far side of a boundary the caller can no
// longer see, and Start returned an error indistinguishable from a refusal.
type acceptedThenLost struct {
	*fakeDelegate
	accepted chan string
	locate   func(request string) (string, error)
}

func (a *acceptedThenLost) Start(ctx context.Context, spec Spec, from int64) (string, error) {
	id, err := a.fakeDelegate.Start(ctx, spec, from)
	if err != nil {
		return "", err
	}
	a.mu.Lock()
	a.jobs[id].request = spec.Request
	a.mu.Unlock()
	a.accepted <- id
	return "", errors.New("connection reset by peer")
}

func (a *acceptedThenLost) Locate(ctx context.Context, request string) (string, error) {
	if a.locate != nil {
		return a.locate(request)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, j := range a.jobs {
		if j.request != "" && j.request == request {
			return id, nil
		}
	}
	return "", nil
}

func lostAfterAccepting(t *testing.T, body []byte) (*Runner, job.Store, *acceptedThenLost, string) {
	t.Helper()
	r, store, fd, root := newDelegatingRunner(t, body)
	lost := &acceptedThenLost{fakeDelegate: fd, accepted: make(chan string, 1)}
	r.Delegators = NewDelegators(lost)
	return r, store, lost, root
}

// mirrored is a source THIS process could fetch from, so that a second transfer
// starting is visible rather than merely possible.
func mirrored(t *testing.T, root string, body []byte) Source {
	t.Helper()
	path := filepath.Join(root, "mirror.bin")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	return Source{Scheme: "file", Locator: path}
}

// A request the delegate accepted must never be started a second time here.
//
// Fallback before acceptance is a different operation from fallback after it.
// Before, nothing has happened and trying elsewhere is free; after, the work is
// running somewhere this process cannot see, and starting again is not a retry.
func TestAcceptedThenLostIsNotTransferredTwice(t *testing.T) {
	body, digest := payload(t, 4<<10)
	r, store, lost, root := lostAfterAccepting(t, body)
	id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))

	err := r.Delegate(context.Background(), id)
	// Proof the delegate took it, read off the channel it published the handle
	// on rather than waited for.
	extID := <-lost.accepted
	if extID == "" {
		t.Fatal("setup: the delegate minted no handle")
	}
	if errors.Is(err, ErrNoDelegator) {
		t.Errorf("an accepted request was reported as though nothing took it: %v", err)
	}

	n, aerr := r.Adopt(context.Background())
	if aerr != nil {
		t.Fatal(aerr)
	}
	if n != 0 {
		t.Fatalf("Adopt started %d transfer(s) for work the delegate is already doing", n)
	}
	if _, serr := os.Stat(finalOf(t, store, id)); serr == nil {
		t.Fatal("the same artifact was fetched a second time and delivered locally")
	}
}

// The handle the delegate minted must end up on the record, so that every later
// sweep can poll, finalise and cancel the work that is actually running.
func TestAcceptedThenLostRecordsTheHandle(t *testing.T) {
	body, digest := payload(t, 4<<10)
	r, store, lost, root := lostAfterAccepting(t, body)
	id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))

	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	extID := <-lost.accepted

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Delegated() {
		t.Fatal("nothing on the record can ever find the transfer that is running")
	}
	if rec.Delegation.ExternalID != extID {
		t.Fatalf("recorded handle %q, the delegate is running %q", rec.Delegation.ExternalID, extID)
	}
}

// A delegate that cannot say whether it took the work leaves an UNKNOWN
// outcome, and unknown is not failure.
//
// Three answers, three recoveries: nothing here can serve this is
// ErrNoDelegator and the job is ours; the delegate refused it is a Refusal with
// a name; and this one, where the caller must not start again and must not
// declare the job dead either.
func TestUnresolvableHandoffIsUnknownNotRejection(t *testing.T) {
	body, digest := payload(t, 4<<10)
	r, store, lost, root := lostAfterAccepting(t, body)
	lost.locate = func(string) (string, error) { return "", errors.New("the share is gone") }
	id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))

	err := r.Delegate(context.Background(), id)
	<-lost.accepted
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Delegate = %v, want ErrOutcomeUnknown", err)
	}
	if errors.Is(err, ErrNoDelegator) {
		t.Fatal("an undetermined outcome reads as though no delegate would take the job")
	}

	n, aerr := r.Adopt(context.Background())
	if aerr != nil {
		t.Fatal(aerr)
	}
	if n != 0 {
		t.Fatalf("Adopt started %d transfer(s) against an undetermined handoff", n)
	}
}

// A delegate that is certain it never took the work must still be fallen back
// from, or every transient refusal strands a job nobody will pick up.
func TestCertainRejectionStillFallsBack(t *testing.T) {
	body, digest := payload(t, 4<<10)
	r, store, lost, root := lostAfterAccepting(t, body)
	lost.startErr = errors.New("bits: spec has no final path")
	lost.locate = func(string) (string, error) { return "", nil }
	id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))

	if err := r.Delegate(context.Background(), id); !errors.Is(err, ErrNoDelegator) {
		t.Fatalf("Delegate = %v, want ErrNoDelegator", err)
	}
	n, err := r.Adopt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("Adopt ran %d jobs; work nobody accepted must still get done here", n)
	}
}

// An unsettled handoff is settled the moment the delegate can be reached, and
// the work it was doing all along is picked up rather than started again.
func TestReconcileSettlesAnUnsettledHandoff(t *testing.T) {
	body, digest := payload(t, 4<<10)
	r, store, lost, root := lostAfterAccepting(t, body)
	unreachable := func(string) (string, error) { return "", errors.New("the share is gone") }
	lost.locate = unreachable
	id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))

	if err := r.Delegate(context.Background(), id); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Delegate = %v, want ErrOutcomeUnknown", err)
	}
	extID := <-lost.accepted

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Delegation.ExternalID != id {
		t.Fatalf("external id %q; an unsettled handoff carries the request", rec.Delegation.ExternalID)
	}

	lost.locate = nil
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec, err = store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Delegation.ExternalID != extID {
		t.Fatalf("recorded handle %q, the delegate is running %q", rec.Delegation.ExternalID, extID)
	}
	if _, serr := os.Stat(finalOf(t, store, id)); serr == nil {
		t.Fatal("the artifact was fetched here while the handoff was in doubt")
	}
}

// And when the delegate says it never took the work, the job comes back to us
// with its sources and checkpoint intact rather than waiting on a handle that
// will never exist.
func TestReconcileTakesBackAHandoffNobodyAccepted(t *testing.T) {
	body, digest := payload(t, 4<<10)
	r, store, lost, root := lostAfterAccepting(t, body)
	lost.locate = func(string) (string, error) { return "", errors.New("the share is gone") }
	id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))

	if err := r.Delegate(context.Background(), id); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Delegate = %v, want ErrOutcomeUnknown", err)
	}
	<-lost.accepted

	lost.locate = func(string) (string, error) { return "", nil }
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Delegated() {
		t.Fatal("the record still names a delegate that says it never had the work")
	}
	if rec.State != job.StatePending {
		t.Fatalf("state = %s, want pending so the work gets done here", rec.State)
	}
	if n, aerr := r.Adopt(context.Background()); aerr != nil || n != 1 {
		t.Fatalf("Adopt = %d, %v; work nobody accepted must still get done here", n, aerr)
	}
}

// A delegate that cannot be asked never gets an unsettled record, because
// nothing could ever settle it. Its lost handoffs stay the refusals they have
// always been — the reason the obligation is stated on Start.
func TestDelegateWithoutLocatorIsUnchanged(t *testing.T) {
	body, digest := payload(t, 4<<10)
	r, store, fd, root := newDelegatingRunner(t, body)
	fd.startErr = errors.New("bits: the service is not running")
	id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))

	if err := r.Delegate(context.Background(), id); !errors.Is(err, ErrNoDelegator) {
		t.Fatalf("Delegate = %v, want ErrNoDelegator", err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Delegated() {
		t.Fatal("a delegate that refused the work was recorded as holding it")
	}
}

// The identity the delegate is asked about must be the one it was given, and it
// must be the same on every attempt. A fresh identity per attempt is no
// identity at all: the second Start could never find the first one's work.
func TestRequestIdentityIsStableAcrossAttempts(t *testing.T) {
	body, digest := payload(t, 4<<10)
	r, store, lost, root := lostAfterAccepting(t, body)
	id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))

	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	first := <-lost.accepted

	lost.mu.Lock()
	request := lost.jobs[first].request
	lost.mu.Unlock()
	if request == "" {
		t.Fatal("the delegate was handed no request identity, so nothing can be asked about later")
	}

	got, err := lost.Locate(context.Background(), request)
	if err != nil || got != first {
		t.Fatalf("Locate(%q) = %q, %v; want %q", request, got, err, first)
	}
}
