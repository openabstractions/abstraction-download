package download

import (
	"context"
	"errors"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// Advance ownership while an external observation is in flight, including ABA:
// the handle may look unchanged even though a successor has already acted.
func advanceDelegation(t *testing.T, store job.Store, id, handle string) {
	t.Helper()
	rec, err := store.Claim(id, "successor", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Update(id, rec.Lease.Epoch, func(r *job.Record) error {
		r.Delegation.ExternalID = handle
		r.Progress.Done = 123
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Release(id, rec.Lease.Epoch); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryRejectsLocateFromPreviousOwner(t *testing.T) {
	for _, found := range []string{"", "stale-handle"} {
		for _, aba := range []bool{false, true} {
			t.Run(found+map[bool]string{false: "/new-handle", true: "/same-handle"}[aba], func(t *testing.T) {
				body, digest := payload(t, 4096)
				r, store, lost, root := lostAfterAccepting(t, body)
				lost.locate = func(string) (string, error) { return "", errors.New("reply lost") }
				id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))
				if err := r.Delegate(context.Background(), id); !errors.Is(err, ErrOutcomeUnknown) {
					t.Fatal(err)
				}
				<-lost.accepted
				handle := "successor-handle"
				if aba {
					handle = id
				}
				lost.locate = func(string) (string, error) {
					advanceDelegation(t, store, id, handle)
					return found, nil
				}
				if err := r.Reconcile(context.Background(), id); !errors.Is(err, job.ErrStaleEpoch) && !errors.Is(err, job.ErrLeaseHeld) {
					t.Fatalf("stale recovery = %v", err)
				}
				rec, err := store.Load(id)
				if err != nil {
					t.Fatal(err)
				}
				if rec.Delegation == nil || rec.Delegation.ExternalID != handle || rec.Progress.Done != 123 {
					t.Fatalf("successor overwritten: %+v", rec)
				}
				if rec.Lease.Held(time.Now()) {
					t.Fatal("stale observer retained a lease")
				}
				if n, err := r.Adopt(context.Background()); err != nil || n != 0 {
					t.Fatalf("duplicate adoption: %d, %v", n, err)
				}
			})
		}
	}
}

type observingDelegate struct {
	*fakeDelegate
	observe func()
	state   DelegateState
}

func (d observingDelegate) Poll(context.Context, string) (Status, error) {
	d.observe()
	return Status{State: d.state}, nil
}

func TestRecoveryRejectsPollFromPreviousOwner(t *testing.T) {
	t.Run("snapshot-store", func(t *testing.T) { rejectStalePoll(t, false) })
	t.Run("base-store-interface", func(t *testing.T) { rejectStalePoll(t, true) })
}

func rejectStalePoll(t *testing.T, baseOnly bool) {
	body, digest := payload(t, 4096)
	r, store, delegate, root := newDelegatingRunner(t, body)
	if baseOnly {
		r.Store = struct{ job.Store }{store}
	}
	id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))
	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	r.Delegators = NewDelegators(observingDelegate{delegate, func() {
		advanceDelegation(t, store, id, "successor-handle")
	}, DelegateGone})
	if err := r.Reconcile(context.Background(), id); !errors.Is(err, job.ErrStaleEpoch) && !errors.Is(err, job.ErrLeaseHeld) {
		t.Fatalf("stale poll = %v", err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Delegation == nil || rec.Delegation.ExternalID != "successor-handle" {
		t.Fatal("stale absence erased successor")
	}
	if n, err := r.Adopt(context.Background()); err != nil || n != 0 {
		t.Fatalf("duplicate adoption: %d, %v", n, err)
	}
}

func TestRecoveryDefersDeliveryWhenIntentChangesDuringPoll(t *testing.T) {
	for _, want := range []job.Want{job.WantCancel, job.WantPause} {
		t.Run(string(want), func(t *testing.T) {
			body, digest := payload(t, 4096)
			r, store, delegate, root := newDelegatingRunner(t, body)
			id := submit(t, store, root, digest, int64(len(body)), mirrored(t, root, body))
			if err := r.Delegate(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			r.Delegators = NewDelegators(observingDelegate{delegate, func() {
				if _, err := store.SetIntent(id, want, "requester"); err != nil {
					t.Fatal(err)
				}
			}, DelegateTransferred})
			if err := r.Reconcile(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			rec, err := store.Load(id)
			if err != nil {
				t.Fatal(err)
			}
			if rec.Delegation == nil || rec.Delegation.Delivered || rec.State == job.StateTransferred || rec.Wants() != want {
				t.Fatalf("delivered against new intent: %+v", rec)
			}
			if rec.Lease.Held(time.Now()) {
				t.Fatal("intent handling deferred behind retained lease")
			}
		})
	}
}
