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
	// Suspended distinguishes "stopped because somebody asked" from "running",
	// which State deliberately does not: to a supervisor deciding whether to
	// take work back, a suspended job is not failed and not finished, so it maps
	// to Running and always did.
	//
	// It is here rather than on the record because it is an observation, not a
	// decision. What somebody WANTS lives in Intent; this is what the delegate
	// happens to be doing about it, and adding it to the record would have meant
	// a schema change across three languages to carry a fact that is re-read on
	// every poll anyway.
	Suspended bool
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

// Without returns these delegators minus one system, so a supervisor can be run
// deliberately one tier lower than the machine would otherwise choose.
//
// This is for the operator, not for applications, and the difference is the
// whole design. An application must never say "not the NAS" — it does not know
// what a NAS is. A person standing at the machine saying "show me what happens
// without it" is asking a legitimate question about their own computer, and
// answering it is how the tiers become demonstrable rather than asserted.
//
// Deliberately negative-only. There is no Only(system): forcing a tier that is
// not configured or not reachable cannot work, and a flag that silently does
// nothing is worse than no flag.
func (d *Delegators) Without(system string) *Delegators {
	if d == nil {
		return NewDelegators()
	}
	kept := make([]Delegator, 0, len(d.all))
	for _, x := range d.all {
		if x.System() != system {
			kept = append(kept, x)
		}
	}
	return &Delegators{all: kept}
}

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
	if d == nil {
		return nil, false
	}
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

	// Work already proven must not be handed to something that will start over.
	//
	// Measured, not assumed: a 16 MiB transfer interrupted with 6,586,368 bytes
	// checkpointed was delegated to BITS, which issued a GET with no Range header
	// and fetched the whole file again. The digest still matched, so nothing was
	// corrupt — it was simply thrown away, which on a 40 GB model over a slow
	// link is the entire complaint this project exists to answer.
	//
	// So a delegate that cannot resume is only eligible from zero. When none
	// qualifies, this returns ErrNoDelegator, the job stays unclaimed, and the
	// supervisor's own adoption pass runs it here — where the checkpoint IS
	// honoured. Slower than a NAS, and it keeps the bytes.
	requires := rec.Requires
	if cp.VerifiedPrefix > 0 {
		requires = append(append([]string{}, requires...), string(CapResume))
	}

	for _, src := range spec.Sources {
		d, ok := r.Delegators.For(src, requires)
		if !ok {
			continue
		}
		one := spec
		one.Sources = []Source{src}
		// A delegate is a different process, sometimes a different account, and
		// it has no idea where our store lives. Hand it paths that are already
		// real on the machine it runs on.
		one.Sink.Partial, one.Sink.Final = LocalSink(r.Store, spec.Sink)
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

	// Nothing here can take it, so let go of the lease claimed at the top.
	//
	// Holding it would make the job invisible to the very sweep that should run
	// it: Adopt only takes orphans, and a job leased by a supervisor that is not
	// working on it is not an orphan. The result was a livelock — delegate,
	// fail, hold, expire, delegate again — that never moved a byte. It was
	// unreachable while every delegator claimed it could resume, and became the
	// normal path the moment they stopped lying.
	r.Store.Release(id, epoch)
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

	// What somebody wants, before asking the delegate how it is getting on.
	//
	// This was missing entirely, and the gap was the worst possible shape: the
	// in-process runner honoured intent at every checkpoint, so pause and cancel
	// worked — on the tier that is used when nothing better is available. Every
	// job that went to BITS or a NAS ignored both. On Windows that is the normal
	// path, so the pause button worked exactly where it did not matter and did
	// nothing where it did.
	//
	// Found by asking whether any of this transfers to a real application, which
	// is not a question the unit tests were ever going to answer.
	if want := rec.Wants(); want != job.WantRun {
		return r.honourDelegated(ctx, rec, want)
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

	// The other half of pausing, and it was missing: something has to start it
	// again. The intent above says run, so if the delegate is still stopped
	// because it was asked to stop, ask it to carry on.
	//
	// This is why Status carries Suspended. To a supervisor deciding whether to
	// take work back, a suspended job is neither failed nor finished, so it maps
	// to Running and must — which leaves nothing to distinguish "paused" from
	// "going", and a resumed job sat still forever with every check passing.
	if st.Suspended {
		s, ok := d.(Suspendable)
		if !ok {
			return fmt.Errorf("%w: %s left a transfer suspended and cannot resume it",
				ErrNoDelegator, d.System())
		}
		if err := s.Resume(ctx, rec.Delegation.ExternalID); err != nil {
			return err
		}
		// Nothing else to do this sweep. The next poll sees it moving.
		return nil
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
		_, final := LocalSink(r.Store, spec.Sink)

		// A delegated download is not one transfer, it is three phases, and only
		// the first was ever visible. Saying which one is happening is the
		// difference between "this finished ten minutes ago and is doing
		// nothing" and "this is copying 40 GB back across a share".
		const phases = 3
		r.step(id, epoch, &job.Step{
			Name:    "copying from " + rec.Delegation.System,
			Ordinal: 2,
			Of:      phases,
		})
		if rf, ok := d.(ReportingFinalizer); ok {
			err = rf.FinalizeReporting(ctx, rec.Delegation.ExternalID, final,
				func(done, total int64) {
					r.step(id, epoch, &job.Step{
						Name:    "copying from " + rec.Delegation.System,
						Ordinal: 2,
						Of:      phases,
						Done:    done,
						Total:   total,
					})
				})
		} else {
			err = d.Finalize(ctx, rec.Delegation.ExternalID, final)
		}
		if err != nil {
			return err
		}

		// Now the file is ours, and now we check it — because the delegate
		// mostly did not, and even the one that did sent the bytes over a second
		// network to get here. Re-hashing gigabytes is not instant either, and
		// it was the second half of the silence.
		r.step(id, epoch, &job.Step{Name: "verifying", Ordinal: 3, Of: phases})
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
			// Finished work is not on a step. Leaving the last one set would
			// leave a completed download reading "verifying" forever, which is
			// the same class of lie as a finished one reading "paused".
			rr.Progress.Step = nil
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

// DelegateAll offers every unclaimed job to the tiers this process has, and it
// is the second hop of the chain.
//
// Without it the chain stopped one link short. An application handed work to the
// supervisor, and the supervisor — which is the process that knows about BITS
// and a NAS — downloaded everything itself, because its sweep only reconciled
// what was already delegated and adopted what was stranded. Nothing ever asked
// "should this go somewhere better?". The NAS was configured, reachable,
// registered, and never used.
//
// Order matters in the sweep that calls this: reconcile, then delegate, then
// adopt. Delegating before adopting is what stops the supervisor grabbing a job
// it should have passed on; adopting last means anything nobody wanted still
// gets done here rather than sitting forever.
func (r *Runner) DelegateAll(ctx context.Context) (int, error) {
	if r.Delegators == nil || len(r.Delegators.all) == 0 {
		return 0, nil
	}
	candidates, err := r.Store.Orphans()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, o := range candidates {
		if o.Kind != Kind || o.Delegated() {
			continue
		}
		// ErrNoDelegator is the ordinary answer for a job no registered tier can
		// serve — a file: source when only BITS is present, say — and it means
		// "leave it for Adopt", not "something went wrong".
		if err := r.Delegate(ctx, o.ID); err != nil {
			continue
		}
		n++
	}
	return n, nil
}

// Suspendable is an OPTIONAL capability of a Delegator: work it can stop and
// take up again without losing what it has.
//
// Optional because it is genuinely not universal. BITS has it natively — it
// already creates every job suspended and resumes it as a separate step. A
// delegate that cannot suspend has only one honest answer, and it is not to
// carry on quietly.
type Suspendable interface {
	Suspend(ctx context.Context, externalID string) error
	Resume(ctx context.Context, externalID string) error
}

// ReportingFinalizer is an OPTIONAL capability: a delegate whose Finalize does
// real work can say how that work is going.
//
// Most delegates finish instantly. BITS was told where to put the file when the
// job started and has been holding it there, so its Finalize is a call into the
// service and nothing more. A NAS is the opposite: the bytes are on the far side
// of a share and Finalize copies every one of them across, which for a 40 GB
// model is minutes of real transfer.
//
// Without this the record showed the delegate's numbers throughout that copy, so
// a person watched a download that said 100% and did nothing, twice over --
// once for the copy and again for the re-hash. The second transfer was real and
// entirely invisible.
//
// Optional rather than part of Delegator for the reason every capability here
// is: a delegate that finishes instantly has nothing to report and should not be
// made to implement a callback it would call once with the same number.
type ReportingFinalizer interface {
	// FinalizeReporting is Finalize, with progress. report may be called from
	// any goroutine and must be cheap; done and total are bytes, and total is 0
	// when the delegate cannot say.
	FinalizeReporting(ctx context.Context, externalID, dest string, report func(done, total int64)) error
}

// honourDelegated carries out what somebody asked for, on work this process is
// not performing.
//
// The rules are the job layer's, not this layer's invention: cancel must be
// honoured by everything, because stopping is universal; pause must be honoured
// only by implementations that advertise it, and one that cannot must fail the
// job with a stated reason rather than continue as though nobody had asked.
func (r *Runner) honourDelegated(ctx context.Context, rec *job.Record, want job.Want) error {
	d, ok := r.Delegators.BySystem(rec.Delegation.System)
	if !ok {
		// Nothing here understands that delegate's handle, so nothing here can
		// act on it. Some other machine's supervisor will.
		return nil
	}
	claimed, err := r.Store.Claim(rec.ID, r.Owner, r.LeaseTTL)
	if err != nil {
		return err
	}
	epoch := claimed.Lease.Epoch

	switch want {
	case job.WantCancel:
		// Abandon first, then record it. The other order can leave an external
		// transfer running with nothing pointing at it — BITS would keep the job
		// in its queue for 90 days, downloading a file nobody is waiting for.
		if err := d.Abandon(ctx, rec.Delegation.ExternalID); err != nil {
			return err
		}
		_, err := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
			rr.State = job.StateCancelled
			rr.Delegation = nil
			rr.Error = ""
			return nil
		})
		return err

	case job.WantPause:
		s, canSuspend := d.(Suspendable)
		if !canSuspend {
			// The contract says say so rather than pretend. A pause that
			// silently does nothing is worse than a refusal, because a person
			// watching a progress bar keep moving has no way to tell which of
			// the two happened.
			_, err := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
				rr.Error = fmt.Sprintf("%s cannot pause a transfer it has taken over", d.System())
				return nil
			})
			if err != nil {
				return err
			}
			r.Store.Release(rec.ID, epoch)
			return nil
		}
		if err := s.Suspend(ctx, rec.Delegation.ExternalID); err != nil {
			return err
		}
		// Let the lease go: paused means nobody is working on it, and Orphans
		// already excludes paused jobs so releasing does not invite a sweep to
		// start it again.
		r.Store.Release(rec.ID, epoch)
		return nil
	}
	r.Store.Release(rec.ID, epoch)
	return nil
}

// step records which phase this job is in, best-effort.
//
// Best-effort on purpose: a step is advisory, and failing to write one must
// never fail the transfer it is describing. The lease is already held by the
// caller, so this is one small write on a record it owns.
func (r *Runner) step(id string, epoch int64, st *job.Step) {
	_, _ = r.Store.Update(id, epoch, func(rr *job.Record) error {
		rr.Progress.Step = st
		rr.Progress.UpdatedAt = job.At(time.Now())
		return nil
	})
}
