package download

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// ErrStalled is work this owner stopped itself, because it reported nothing for
// longer than the silence budget.
//
// It is deliberately the owner's own error and not a state on the record. A
// state is what a job IS; stalled is what somebody THINKS, and it changes on the
// next byte — see docs/stall-detection.md, which refuses to add one.
var ErrStalled = errors.New("download: no progress within the silence budget")

// DefaultSilenceBudget is how long an operation may report nothing before the
// owner stops holding its lease.
//
// The number is a guess and is documented as one. aria2's --timeout is sixty
// seconds, wget's --read-timeout is fifteen minutes, and curl does nothing at
// all unless asked; five minutes sits between the two that do. It is not the
// lease TTL and must not be confused with it: the TTL answers "how fast do we
// notice a dead owner", this answers "how long is silence acceptable", and
// thirty seconds is right for the first and wrong by an order of magnitude for
// the second.
const DefaultSilenceBudget = 5 * time.Minute

// A keeper holds a lease across an operation the store never hears from, and
// lets go of it when the operation goes quiet.
//
// # The failing case
//
// Reconcile's delegated finalise claimed for LeaseTTL — thirty seconds — and
// then copied the artifact across a share and hashed every byte of it, both
// proportional to the size of the file, and renewed nothing in between. The
// terminal Update was therefore refused for any transfer that took longer than
// thirty seconds to bring across and verify, which is every model this project
// exists to move. It passed on a 112 MB proof because 112 MB fits inside the
// window.
//
// # Why a timer, and why a bounded one
//
// Renewal elsewhere in this layer rides the data callback: reportWriter's
// Report fires when bytes land, and the renewal happens there. That is why an
// in-process owner whose socket goes silent is evicted today — liveness happens
// to ride the data path. Moving renewal to a bare timer would fix the finalise
// case and open that hole in the same edit: a delegate holding a connection and
// sending nothing would renew forever, and nobody could ever take the work.
//
// So the timer is bounded by a silence budget. The owner renews while it is
// within the budget, and stops renewing when the budget is spent — whether or
// not it managed to unblock itself. That deliberately creates the state "alive,
// renewing, not advancing" and makes it bounded, chosen by the owner, and
// visible: the record shows lease.expires_at moving and progress.updated_at
// standing still, which is the honest description of what is happening.
//
// # What feeds it
//
// beat, from the work itself — a chunk copied, a megabyte hashed, a phase
// beginning. A beat is in-process and free, so it is called far more often than
// the record is written; the two are different questions and were conflated
// before. A successful Renew is NOT a beat: a renewal that reset its own budget
// would renew forever, which is the hole again.
//
// # What it does when it stops
//
// It cancels the operation's context, and it remembers why. That is the fence
// clause of the lease protocol, which was never written down: an owner whose
// Renew or Update is refused for a stale epoch must stop acting on the work at
// that epoch, immediately and permanently — not write the artifact, not deliver
// it, not remove it. fenced reports that reason so a caller can return it
// instead of the "context canceled" it would otherwise see.
type keeper struct {
	store  job.Store
	id     string
	epoch  int64
	ttl    time.Duration
	budget time.Duration

	// watch is handed what the store said on each renewal. A renewal reads the
	// record anyway, so observing what somebody asked for there is free — and it
	// is the only bound on how long a pause takes to land for work whose own
	// checkpoints are far apart. See parallel.go, where one range can be minutes.
	watch func(*job.Record)

	cancel context.CancelFunc
	done   chan struct{}
	exited chan struct{}
	once   sync.Once

	mu    sync.Mutex
	last  time.Time
	fence error
}

// keep starts a keeper for work this process is about to do under a lease it
// already holds, and returns the context that work must run under.
func (r *Runner) keep(ctx context.Context, id string, epoch int64) (context.Context, *keeper) {
	return r.keepWatching(ctx, id, epoch, nil)
}

// keepWatching is keep, plus a callback given every record the renewals read.
func (r *Runner) keepWatching(ctx context.Context, id string, epoch int64, watch func(*job.Record)) (context.Context, *keeper) {
	ctx, cancel := context.WithCancel(ctx)
	budget := r.SilenceBudget
	if budget <= 0 {
		budget = DefaultSilenceBudget
	}
	ttl := r.LeaseTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	k := &keeper{
		store:  r.Store,
		id:     id,
		epoch:  epoch,
		ttl:    ttl,
		budget: budget,
		watch:  watch,
		cancel: cancel,
		done:   make(chan struct{}),
		exited: make(chan struct{}),
		last:   time.Now(),
	}
	go k.hold()
	return ctx, k
}

// hold is the timer. It renews until the work goes silent for the budget, until
// the store refuses, or until stop.
func (k *keeper) hold() {
	defer close(k.exited)
	// Three renewals per TTL, so a single missed one — a share that blinks, a
	// record being replaced at the moment we read it — is not the end of the
	// lease. The cost of each is one small file write.
	every := k.ttl / 3
	if every < time.Millisecond {
		every = time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-k.done:
			return
		case <-t.C:
		}

		k.mu.Lock()
		silent := time.Since(k.last)
		spent := k.fence != nil
		k.mu.Unlock()
		if spent {
			return
		}
		if silent >= k.budget {
			// Stop renewing and let the lease lapse. This is the whole of what
			// the watchdog can promise: cancelling the context unblocks an HTTP
			// body or a share copy that honours it, and a Read hung inside the
			// kernel on an SMB path it cannot touch at all. Letting go is the
			// part that always works — the work moves, and the fence refuses
			// whatever this owner's read eventually returns.
			k.refuse(fmt.Errorf("%w: nothing for %s", ErrStalled, silent.Truncate(time.Second)))
			return
		}

		rec, err := k.store.Renew(k.id, k.epoch, k.ttl)
		if err == nil {
			if k.watch != nil {
				k.watch(rec)
			}
			continue
		}
		if errors.Is(err, job.ErrStaleEpoch) || errors.Is(err, job.ErrLeaseExpiry) || errors.Is(err, job.ErrNotFound) {
			// Somebody else owns this now, or the lease lapsed while this owner
			// was not looking. Either way there is nothing here to hold and
			// nothing this epoch may still write.
			k.refuse(err)
			return
		}
		// Anything else is treated as possibly transient — the record was being
		// replaced, the share was briefly away. There are three attempts per
		// TTL, so one failure is not a lapse.
	}
}

// beat says the work moved. Cheap on purpose: it is called per chunk, and it
// writes nothing.
func (k *keeper) beat() {
	k.mu.Lock()
	k.last = time.Now()
	k.mu.Unlock()
}

// refused reports a store write this owner was turned down for.
//
// A refusal for a stale epoch or a lapsed lease is the fence: this owner must
// stop acting on the work, so it is remembered and the operation is cancelled.
// Anything else is left alone — an advisory write that failed for its own
// reasons must not stop a transfer it was only describing.
func (k *keeper) refused(err error) {
	if errors.Is(err, job.ErrStaleEpoch) || errors.Is(err, job.ErrLeaseExpiry) {
		k.refuse(err)
	}
}

func (k *keeper) refuse(err error) {
	k.mu.Lock()
	if k.fence == nil {
		k.fence = err
	}
	k.mu.Unlock()
	k.cancel()
}

// fenced is why this owner may no longer act, or nil while it still may.
//
// Check it before doing anything that is not idempotent — delivering a file,
// removing a partial, writing the terminal record — and check it after an
// operation returns an error, because a cancelled copy reports "context
// canceled" and the reason is here.
func (k *keeper) fenced() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.fence
}

// stop ends the renewals and returns the fence reason, if there is one.
//
// It waits for the timer to be out of the store, and the waiting is the point.
// A renewal re-writes a record it loaded moments earlier, so one still in flight
// would land on top of a write the caller has just made and quietly undo it —
// the terminal "transferred, delivered" write being the one that matters. Every
// caller must therefore stop the keeper BEFORE the write that ends the work,
// not in a defer afterwards. Idempotent, so the defer is still worth having for
// the paths that return early.
func (k *keeper) stop() error {
	k.once.Do(func() { close(k.done) })
	<-k.exited
	k.cancel()
	return k.fenced()
}
