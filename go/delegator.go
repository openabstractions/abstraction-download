package download

import (
	"context"
	"errors"
	"fmt"
	"time"

	job "github.com/ReinisLusis/abstraction-job"
)

// A Delegator hands the whole job to something outside this process and then
// watches it.
//
// This is the second of exactly two shapes, and between them they cover every
// implementation category the surveys turned up:
//
//	Fetcher    streams bytes through us      http, a file, an SMB path
//	Delegator  the other system does it      BITS, a curl subprocess, a NAS
//	                                         daemon, a torrent client, a
//	                                         durable execution engine
//
// The distinction is not stylistic. A Fetcher gives us an io.Writer's worth of
// control and dies with our process. A Delegator writes the file itself, under
// its own account, keeps going while every process that asked is closed, and
// hands back nothing but a handle. Windows BITS is the archetype: it owns an
// on-disk queue, survives reboot, and will not release the file at all until
// Finalize is called.
//
// Trying to force BITS through the Fetcher interface would have meant
// reimplementing BITS badly. Adding this interface instead is what the surveys
// were for.
type Delegator interface {
	// System is the name recorded in job.Delegation.System. It decides who can
	// interpret an ExternalID later — possibly after a reboot, in another
	// process, written by another language.
	System() string

	// Schemes are the source schemes this can serve.
	Schemes() []string

	// Capabilities are what it promises. A Delegator that does not claim
	// CapSurvivesProcessExit is not worth delegating to.
	Capabilities() []Capability

	// Start hands the work over and returns the external handle. It must not
	// block until the transfer finishes — the entire point is that the transfer
	// outlives this call, and usually this process.
	Start(ctx context.Context, spec Spec, from int64) (externalID string, err error)

	// Poll reports on delegated work. Any process may call it, including one
	// that never started anything — that is what makes progress observable from
	// outside, which a callback cannot be.
	Poll(ctx context.Context, externalID string) (Status, error)

	// Finalize takes delivery into dest. BITS calls this Complete(), and until
	// it happens the file is not the caller's and may not exist at its final
	// path at all. A job left unacknowledged sits in the BITS queue for 90 days.
	//
	// dest is the job's destination resolved on THIS machine. BITS ignores it,
	// because it was told where to put the file at Start and has been holding it
	// there. That is exactly why the parameter was missing at first, and why it
	// had to be added: an implementation whose work lands somewhere else — a NAS
	// writing to its own disk — has no way to discover the local destination
	// from a handle alone. The interface described one binding's habits rather
	// than the operation. "Take delivery" needs to say delivery of what, and
	// where.
	Finalize(ctx context.Context, externalID, dest string) error

	// Abandon cancels the work and cleans up after it. Note that BITS's Cancel
	// deletes completed files as well as partial ones, so this is not a way to
	// keep what has arrived so far.
	Abandon(ctx context.Context, externalID string) error
}

// DelegateState is the external system's view of the work.
type DelegateState string

const (
	// DelegateRunning is still working, or waiting to.
	DelegateRunning DelegateState = "running"
	// DelegateTransferred has all the bytes but has not handed them over.
	// Finalize turns this into a file the caller owns.
	DelegateTransferred DelegateState = "transferred"
	// DelegateFailed gave up.
	DelegateFailed DelegateState = "failed"
	// DelegateGone means the external system has never heard of this handle.
	// BITS reaps jobs after 90 days, queue databases get corrupted and
	// replaced, and a machine can be rebuilt — so a handle that no longer
	// resolves is a normal outcome, not an exception.
	DelegateGone DelegateState = "gone"
)

// Status is one poll's answer.
type Status struct {
	State DelegateState
	Done  int64
	Total int64
	Err   string
}

var (
	// ErrNoDelegator means nothing registered can hand this job anywhere.
	ErrNoDelegator = errors.New("download: no delegator for this job's sources")
	// ErrNotDelegated means the job is not in someone else's hands.
	ErrNotDelegated = errors.New("download: job is not delegated")
)

// Delegators picks a Delegator for a source, the same way Registry does for
// Fetchers.
type Delegators struct {
	all []Delegator
}

func NewDelegators(ds ...Delegator) *Delegators { return &Delegators{all: ds} }

func (d *Delegators) Add(x Delegator) { d.all = append(d.all, x) }

func (d *Delegators) For(src Source, requires []string) (Delegator, bool) {
	for _, x := range d.all {
		if !hasScheme(x.Schemes(), src.Scheme) {
			continue
		}
		if hasAllCaps(x.Capabilities(), requires) {
			return x, true
		}
	}
	return nil, false
}

// BySystem finds the delegator that can interpret a recorded handle. Without
// this, a job delegated by yesterday's process is unreachable today.
func (d *Delegators) BySystem(system string) (Delegator, bool) {
	for _, x := range d.all {
		if x.System() == system {
			return x, true
		}
	}
	return nil, false
}

func hasScheme(schemes []string, s string) bool {
	for _, x := range schemes {
		if x == s {
			return true
		}
	}
	return false
}

func hasAllCaps(caps []Capability, requires []string) bool {
	if len(requires) == 0 {
		return true
	}
	have := make(map[string]bool, len(caps))
	for _, c := range caps {
		have[string(c)] = true
	}
	for _, w := range requires {
		if !have[w] {
			return false
		}
	}
	return true
}

// Delegate hands a job to an external system and records the handle.
//
// After this returns, the work is out of our hands and this process may exit.
// The handle in the record is the only thing that can find it again.
func (r *Runner) Delegate(ctx context.Context, id string) error {
	if r.Delegators == nil {
		return ErrNoDelegator
	}
	rec, err := r.Store.Claim(id, r.Owner, r.LeaseTTL)
	if err != nil {
		return err
	}
	epoch := rec.Lease.Epoch

	spec, err := SpecOf(rec)
	if err != nil {
		return err
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		return err
	}

	for _, src := range spec.Sources {
		d, ok := r.Delegators.For(src, rec.Requires)
		if !ok {
			continue
		}
		one := spec
		one.Sources = []Source{src}
		// A delegate is a different process, sometimes a different account, and
		// it has no idea where our store lives. Hand it paths that are already
		// real on the machine it runs on.
		one.Sink.Partial, one.Sink.Final = spec.Sink.Resolve(r.Store.Root())
		extID, err := d.Start(ctx, one, cp.VerifiedPrefix)
		if err != nil {
			continue
		}
		_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Delegation = &job.Delegation{System: d.System(), ExternalID: extID}
			rr.State = job.StateRunning
			return nil
		})
		if err != nil {
			// The handle could not be recorded, so nothing will ever find this
			// work again. Cancelling is better than leaking a transfer nobody
			// knows about.
			d.Abandon(ctx, extID)
			return err
		}
		// Let go of the lease: we are not doing the work, and holding it would
		// stop anyone else from polling and finalising.
		r.Store.Release(id, epoch)
		return nil
	}
	return ErrNoDelegator
}

// Reconcile asks the external system how a delegated job is doing and moves the
// record to match. Anyone may call it — the process that delegated the work is
// usually long gone, which is the entire point.
//
// It verifies the bytes itself before accepting them. BITS "guarantees that the
// version of the file it transfers is consistent based on the file size and time
// stamp, not content", so the delegate finishing is not evidence that the file
// is right.
func (r *Runner) Reconcile(ctx context.Context, id string) error {
	rec, err := r.Store.Load(id)
	if err != nil {
		return err
	}
	if !rec.Delegated() {
		return fmt.Errorf("%w: %s", ErrNotDelegated, id)
	}
	if rec.State.Terminal() {
		return nil
	}
	// Already delivered. Polling again would ask about a job the delegate
	// destroyed on Finalize — BITS removes it from the queue — which reads back
	// as Gone, and Gone means "take the work back", so a second sweep would
	// re-download a file that is already correct at its final path.
	//
	// StateTransferred is deliberately not terminal (the consumer has yet to
	// acknowledge), so the terminal check above does not cover this. Delivered
	// is what distinguishes "the delegate finished" from "the delegate lost it".
	if rec.Delegation.Delivered || rec.State == job.StateTransferred {
		return nil
	}
	d, ok := r.Delegators.BySystem(rec.Delegation.System)
	if !ok {
		return fmt.Errorf("%w: nothing here understands %q", ErrNoDelegator, rec.Delegation.System)
	}

	st, err := d.Poll(ctx, rec.Delegation.ExternalID)
	if err != nil {
		return err
	}

	// Claim only now: polling needs no lease, and taking one before we know
	// there is something to do would block whoever else is watching.
	claimed, err := r.Store.Claim(id, r.Owner, r.LeaseTTL)
	if err != nil {
		return err
	}
	epoch := claimed.Lease.Epoch

	switch st.State {
	case DelegateRunning:
		_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Progress.Done = st.Done
			if st.Total > 0 {
				rr.Progress.Total = st.Total
			}
			rr.Progress.UpdatedAt = job.At(time.Now())
			return nil
		})
		r.Store.Release(id, epoch)
		return err

	case DelegateFailed, DelegateGone:
		// Hand it back to ourselves. The sources are still in the spec, the
		// checkpoint still says what was proven, and an in-process Fetcher can
		// carry on — a delegate disappearing is a reason to do the work here,
		// not a reason to lose it.
		_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Delegation = nil
			rr.State = job.StatePending
			if st.Err != "" {
				rr.Error = st.Err
			} else if st.State == DelegateGone {
				rr.Error = "delegate no longer knows this handle"
			}
			return nil
		})
		r.Store.Release(id, epoch)
		return err

	case DelegateTransferred:
		spec, err := SpecOf(claimed)
		if err != nil {
			return err
		}
		_, final := spec.Sink.Resolve(r.Store.Root())
		if err := d.Finalize(ctx, rec.Delegation.ExternalID, final); err != nil {
			return err
		}
		// Now the file is ours, and now we check it — because the delegate
		// mostly did not, and even the one that did sent the bytes over a second
		// network to get here.
		total, digest, err := hashFile(final)
		if err != nil {
			return err
		}
		if want := spec.Artifact.Digest; want != "" && !equalFold(digest, want) {
			_, uerr := r.Store.Update(id, epoch, func(rr *job.Record) error {
				rr.Delegation = nil
				rr.State = job.StatePending
				rr.Error = fmt.Sprintf("%v: delegate delivered %s, want %s", ErrDigestMismatch, digest, want)
				return rr.SetCheckpoint(Checkpoint{VerifiedPrefix: 0})
			})
			if uerr != nil {
				return uerr
			}
			return fmt.Errorf("%w: delegate delivered %s, want %s", ErrDigestMismatch, digest, want)
		}
		_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Progress.Done = total
			rr.Progress.UpdatedAt = job.At(time.Now())
			rr.State = job.StateTransferred
			rr.Error = ""
			rr.Delegation.Delivered = true
			return rr.SetCheckpoint(Checkpoint{VerifiedPrefix: total})
		})
		return err
	}
	return fmt.Errorf("download: delegate reported unknown state %q", st.State)
}

// ReconcileAll walks every delegated download and brings each up to date. This
// is what a service runs on a timer, and on start after a reboot.
func (r *Runner) ReconcileAll(ctx context.Context) (int, error) {
	all, err := r.Store.List()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, rec := range all {
		if rec.Kind != Kind || !rec.Delegated() || rec.State.Terminal() {
			continue
		}
		// Nothing to catch up on, and asking would undo it — see Reconcile.
		if rec.Delegation.Delivered || rec.State == job.StateTransferred {
			continue
		}
		if err := r.Reconcile(ctx, rec.ID); err != nil {
			continue // one unreachable delegate must not stop the others
		}
		n++
	}
	return n, nil
}
