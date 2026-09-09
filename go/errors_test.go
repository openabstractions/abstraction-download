package download

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// refusingUpdate says no to the first refusals writes it is asked for, standing
// in for the disk, the share or the remote store that will not take one. A test
// that wants every write refused asks for more than it will make.
type refusingUpdate struct {
	job.Store
	why      error
	refusals atomic.Int32
}

func (s *refusingUpdate) Update(id string, epoch int64, mutate func(*job.Record) error) (*job.Record, error) {
	if s.refusals.Add(-1) >= 0 {
		return nil, s.why
	}
	return s.Store.Update(id, epoch, mutate)
}

func refusesUpdates(store job.Store, why error, n int32) *refusingUpdate {
	s := &refusingUpdate{Store: store, why: why}
	s.refusals.Store(n)
	return s
}

// The comment over that write says nothing waiting on the record can ever stop
// waiting if it does not happen. The question it stopped the last reader asking
// is how anybody would find out that it had not — and the answer was nobody,
// because the refusal went on the floor one line below the argument for it.
func TestRunReportsTheFailureItCouldNotRecord(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	r, store, root := newRunner(t)
	_, digest := payload(t, 512)
	id := submit(t, store, root, digest, 512, Source{Scheme: "http", Locator: srv.URL + "/p.bin"})

	refused := errors.New("the store would not write the failure down")
	r.Store = refusesUpdates(store, refused, 1000)

	err := r.Run(context.Background(), id)
	if !errors.Is(err, refused) {
		t.Fatalf("Run returned %v, want the refused write of the terminal state", err)
	}
	rec, lerr := store.Load(id)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if rec.Error != "" {
		t.Fatalf("the record says %q, so this test proved nothing", rec.Error)
	}
}

// refusingRenew is a store that will not extend a lease, which is what one says
// to an owner whose epoch has moved on — and to an owner whose job somebody else
// is already working on.
type refusingRenew struct {
	job.Store
	why error
}

func (s refusingRenew) Renew(id string, epoch int64, ttl time.Duration) (*job.Record, error) {
	return nil, s.why
}

// The fence, on the one-connection path. TestARefusedRenewalStopsEveryRange
// covers the parallel one; this owner went on writing bytes and delivered a file
// for work the store had already told it that it no longer held.
func TestARefusedRenewalStopsASequentialTransfer(t *testing.T) {
	body, digest := payload(t, 64<<10)
	srv := serveOnce(t, body)

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})

	refused := fmt.Errorf("%w: fenced by the test", job.ErrStaleEpoch)
	r.Store = refusingRenew{Store: store, why: refused}
	r.Connections = 1
	r.PersistEvery = 1

	if err := r.Run(context.Background(), id); !errors.Is(err, refused) {
		t.Fatalf("Run returned %v, want the refused renewal", err)
	}
	if _, err := os.Stat(filepath.Join(root, "final.bin")); err == nil {
		t.Fatal("an owner whose renewal was refused still delivered the file")
	}
}

// The comment over that pair says the next runner must not resume onto a prefix
// already known to be wrong. What it stopped the last reader asking is what the
// record says if the write clearing it is refused: it still claims the bytes
// that were just deleted, which is the resume the comment forbids.
func TestADigestMismatchReportsACheckpointItCouldNotClear(t *testing.T) {
	body, digest := payload(t, 512)
	wrong := make([]byte, len(body))
	copy(wrong, body)
	wrong[0] ^= 0xff
	srv := serveOnce(t, wrong)

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})

	refused := errors.New("the store would not clear the checkpoint")
	r.Store = refusesUpdates(store, refused, 1)

	err := r.Run(context.Background(), id)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Run returned %v, want the digest mismatch", err)
	}
	if !errors.Is(err, refused) {
		t.Fatalf("Run returned %v, want the refused checkpoint clear beside it", err)
	}
}

// deafDelegate accepts work and will not let go of it. echo makes Start answer
// with the request id it was handed, which is the falsified-handle case.
type deafDelegate struct {
	*fakeDelegate
	why  error
	echo bool
}

func (d deafDelegate) Abandon(context.Context, string) error { return d.why }

func (d deafDelegate) Start(ctx context.Context, spec Spec, from int64) (string, error) {
	if d.echo {
		return spec.Request, nil
	}
	return d.fakeDelegate.Start(ctx, spec, from)
}

// "Cancelling is better than leaking a transfer nobody knows about" is the
// comment one line above the discard. What it stopped the last reader asking is
// what happens when the cancel is refused: the transfer leaks AND the caller is
// told it was cancelled, which is the one answer worse than either.
func TestAnUnrecordableHandleReportsACancelThatWasRefused(t *testing.T) {
	body, digest := payload(t, 8<<10)
	r, store, fd, root := newDelegatingRunner(t, body)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})

	stuck := errors.New("the delegate would not abandon the transfer")
	r.Delegators = NewDelegators(deafDelegate{fakeDelegate: fd, why: stuck})
	unwritable := errors.New("the store would not record the handle")
	r.Store = refusesUpdates(store, unwritable, 1000)

	err := r.Delegate(context.Background(), id)
	if !errors.Is(err, unwritable) {
		t.Fatalf("Delegate returned %v, want the refused write of the handle", err)
	}
	if !errors.Is(err, stuck) {
		t.Fatalf("Delegate returned %v, so the leaked transfer is invisible", err)
	}
}

// A delegate that answers with the request id has falsified its handle, and the
// transfer it started is abandoned before that is reported. A refused abandon
// leaves it running under a name nothing will ever poll.
func TestAFalsifiedHandleReportsACancelThatWasRefused(t *testing.T) {
	body, digest := payload(t, 8<<10)
	r, store, fd, root := newDelegatingRunner(t, body)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})

	stuck := errors.New("the delegate would not abandon the transfer")
	r.Delegators = NewDelegators(deafDelegate{fakeDelegate: fd, why: stuck, echo: true})

	err := r.Delegate(context.Background(), id)
	if !errors.Is(err, ErrClaimFalsified) {
		t.Fatalf("Delegate returned %v, want the falsified claim", err)
	}
	if !errors.Is(err, stuck) {
		t.Fatalf("Delegate returned %v, so the leaked transfer is invisible", err)
	}
}
