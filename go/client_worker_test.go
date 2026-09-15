package download

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// heldFetcher blocks inside Fetch until the test releases it, so a worker
// holds its lease for exactly as long as the test decides.
type heldFetcher struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (f *heldFetcher) Schemes() []string          { return []string{"held"} }
func (f *heldFetcher) Capabilities() []Capability { return nil }
func (f *heldFetcher) Fetch(ctx context.Context, req Request) (Result, error) {
	f.calls.Add(1)
	f.entered <- struct{}{}
	select {
	case <-f.release:
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	return Result{}, errors.New("held fetch released")
}

// Injected scheduling: the first worker is held inside its fetch while the same
// request arrives again, and the record carries a failure from an earlier
// attempt. Every worker and the failure-clearing claim use one owner name, and
// the store lets an owner re-claim a lease it holds, so a repeat request that
// clears the failure or starts a second worker takes a new epoch from under the
// running worker. The epoch is read synchronously after Submit returns.
func TestAskingAgainWhileItRunsLeavesTheWorkerItsLease(t *testing.T) {
	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(store, "test-owner")
	held := &heldFetcher{entered: make(chan struct{}, 8), release: make(chan struct{})}
	runner.Fetchers = NewFetchers(held)
	svc := NewClient(runner, WithExecution(ExecuteHere))
	spec := Spec{Sources: []Source{{Scheme: "held", Locator: "held://example/x.bin"}}, Sink: Sink{Final: "out/held.bin"}}

	first, err := svc.Submit(spec)
	if err != nil {
		t.Fatal(err)
	}
	id := first.ID()
	released := false
	defer func() {
		if !released {
			close(held.release)
		}
	}()
	select {
	case <-held.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the first worker never reached its fetch")
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	epoch := rec.Lease.Epoch
	if _, err := store.Update(id, epoch, func(r *job.Record) error {
		return setFailure(r, errors.New("an earlier attempt failed"))
	}); err != nil {
		t.Fatal(err)
	}

	second, err := svc.Submit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID() != id {
		t.Fatalf("two ids for one request: %s and %s", id, second.ID())
	}
	after, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if after.Lease.Epoch != epoch {
		t.Fatalf("a repeat request re-claimed the running worker's lease: epoch %d, worker holds %d", after.Lease.Epoch, epoch)
	}
	select {
	case <-held.entered:
		t.Fatal("a second worker reached the fetch for work this client already runs")
	case <-time.After(300 * time.Millisecond):
	}

	close(held.release)
	released = true
	deadline := time.Now().Add(10 * time.Second)
	for {
		now, err := store.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		if !now.Lease.Held(time.Now()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the worker never let go of its lease")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := held.calls.Load(); got != 1 {
		t.Fatalf("fetch entered %d times, want 1", got)
	}
}
