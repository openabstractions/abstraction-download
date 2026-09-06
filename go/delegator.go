package download

import (
	"context"
	"errors"
	"fmt"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
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
	// ErrStrandedHere is a record already in somebody's hands, naming a system
	// THIS process cannot speak to. Distinct from ErrNoDelegator, which is this
	// process having nowhere to send a job in the first place: there the work is
	// still ours to do, and here it is not ours to touch at all.
	//
	// It wraps ErrNoDelegator so that a caller asking only the coarse question
	// gets the same answer it always did.
	ErrStrandedHere = fmt.Errorf("%w: nothing here understands", ErrNoDelegator)
)

// Delegators picks a Delegator for a source, the same way Fetchers does for
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

// Selective is an OPTIONAL capability: a delegate that can tell, from the job
// itself, that it could never do this one.
//
// Scheme and capabilities are not enough, and the gap strands work silently. A
// NAS serves "https" and promises to survive process exit, so it was handed a
// job whose source was http://127.0.0.1 -- an address that means THIS machine,
// and is unreachable from any other. The far side could never fetch it, the
// record sat running forever, and every sweep reported reconciling it.
//
// Only the delegate can answer this, which is the same argument this project
// already makes about intent: "asking for something the current owner cannot do
// is not an error here, because only the owner knows what it can do".
//
// Optional, so a delegate with nothing to refuse implements nothing.
type Selective interface {
	// CanServe reports whether this delegate could perform this job at all.
	// It is about POSSIBILITY, not preference or load: answering false means
	// the work would be impossible here, not merely inconvenient.
	CanServe(spec Spec) bool
}

// ForSpec picks a delegate for a whole job, so one that can refuse gets to see
// what it is being offered rather than only the source's scheme.
func (d *Delegators) ForSpec(spec Spec, src Source, requires []string) (Delegator, bool) {
	for _, x := range d.all {
		if !hasScheme(x.Schemes(), src.Scheme) {
			continue
		}
		if !hasAllCaps(x.Capabilities(), requires) {
			continue
		}
		// The delegate's own veto, last, because it is the most specific thing
		// anybody knows about the job.
		if sel, ok := x.(Selective); ok && !sel.CanServe(spec) {
			continue
		}
		return x, true
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
	// Bytes this store has already proven are a local copy, and a delegate
	// would fetch them again from the network and hand them back across a share.
	if len(proven(r.Store, spec.Artifact.Digest, id)) > 0 {
		r.Store.Release(id, epoch)
		return ErrNoDelegator
	}

	// A delegate is a different process, sometimes a different account, and it
	// has no idea where our store lives. Hand it paths that are already real on
	// the machine it runs on.
	//
	// Resolved before any delegate is consulted, because this is the crossing
	// where the record's author and the writer's authority come apart: the
	// submitter chose the destination and the delegate does the writing. A sink
	// that leaves the store root is refused here and no delegate hears about the
	// job at all.
	sinkPartial, sinkFinal, err := LocalSink(r.Store, id, spec.Sink)
	if err != nil {
		r.Store.Release(id, epoch)
		return err
	}

	for _, src := range spec.Sources {
		// A delegate opens the socket in a process this policy cannot reach
		// into, so the question is asked before it is handed the work.
		if r.Reach.check(HostOf(src.Locator)) != nil {
			continue
		}
		one := spec
		one.Sources = []Source{src}
		// The whole job, not just the scheme, so a delegate that can tell it
		// could never do this one gets to say so before it is handed the work.
		d, ok := r.Delegators.ForSpec(one, src, requires)
		if !ok {
			continue
		}
		one.Sink.Partial, one.Sink.Final = sinkPartial, sinkFinal
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
		// A terminal job that still holds a delegation handle has work running
		// somewhere else that nobody will ever collect.
		//
		// This returned nil, and the cost was measured rather than imagined: a
		// pause that reached the record as a cancel left a NAS fetching 3.1 GB
		// for a job the local side had already given up on. It ran to
		// completion, and the finished file sat on the share as a transferred
		// job with no requester. The delegate was never told, because the only
		// code that would have told it stopped one line above.
		//
		// The interface has always had Abandon for exactly this — "a job left
		// unacknowledged sits in the BITS queue for 90 days" — and nothing
		// called it on this path.
		if rec.Delegation.Delivered {
			return nil
		}
		return r.abandonDelegated(ctx, rec)
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
		return r.stranded(rec)
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
			// The FIRST phase, which is the long one and was the one still
			// missing: steps were being set for the copy back and the verify,
			// so a job spent its whole download saying nothing and only started
			// reporting phases once the bytes were already fetched.
			rr.Progress.Step = &job.Step{
				Name:    "fetching on " + rec.Delegation.System,
				Ordinal: 1,
				Of:      3,
				Done:    st.Done,
				Total:   st.Total,
			}
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
			// A handle that evaporated is not an attempt at these bytes. Error
			// means "the last try at fetching this failed, and here is why",
			// and three things read it that way: a waiting caller ends on it,
			// `dl status` prints it, and the retry backoff counts it. A store
			// that was cleaned out has told us nothing about the download, so
			// writing there makes all three wrong to gain a line jobd already
			// prints from what Reconcile returns. A delegate that tried and
			// failed is the opposite, and keeps its reason.
			if st.Err != "" {
				rr.Error = st.Err
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
		// The delegate is about to be told where to put the bytes, so the same
		// refusal that guarded Delegate guards the finalise.
		_, final, err := LocalSink(r.Store, claimed.ID, spec.Sink)
		if err != nil {
			return err
		}

		// A delegated download is not one transfer, it is three phases, and only
		// the first was ever visible. Saying which one is happening is the
		// difference between "this finished ten minutes ago and is doing
		// nothing" and "this is copying 40 GB back across a share".
		const phases = 3

		// Both phases below are proportional to the size of the file — a copy
		// across a share, then a hash of every byte that arrived — and nothing
		// here renewed the lease claimed a few lines above. So the terminal
		// Update at the bottom was refused for any artifact that took longer
		// than LeaseTTL, thirty seconds by default, to bring across and verify.
		// The NAS proofs passed because 112 MB fits inside that window. A
		// multi-gigabyte model does not, and it failed quietly first: the steps
		// stopped landing and only the last write said anything.
		//
		// The keeper renews on a timer for as long as the work keeps reporting,
		// and stops renewing once it has been silent for the budget. The bound
		// is not optional — an owner that renewed on a bare timer would hold a
		// lease forever while a delegate held a connection and sent nothing.
		// keeper.go and docs/stall-detection.md have the argument.
		ctx, keep := r.keep(ctx, id, epoch)
		defer keep.stop()

		r.step(keep, &job.Step{
			Name:    "copying from " + rec.Delegation.System,
			Ordinal: 2,
			Of:      phases,
		})
		if rf, ok := d.(ReportingFinalizer); ok {
			err = rf.FinalizeReporting(ctx, rec.Delegation.ExternalID, final,
				func(done, total int64) {
					r.step(keep, &job.Step{
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
			// A copy this owner's own watchdog cancelled surfaces as "context
			// canceled", which describes the mechanism and not the reason. The
			// reason is on the keeper, and it is the one a person needs.
			if ferr := keep.fenced(); ferr != nil {
				return fmt.Errorf("copying from %s: %w", rec.Delegation.System, ferr)
			}
			return err
		}

		// Now the file is ours, and now we check it — because the delegate
		// mostly did not, and even the one that did sent the bytes over a second
		// network to get here. Re-hashing gigabytes is not instant either, and
		// it was the second half of the silence.
		//
		// It is also the phase the silence budget must not stop. Hashing is
		// local CPU with nothing to report to any network and it legitimately
		// runs for minutes on a large model, so it reports: every chunk to the
		// watchdog, which costs nothing, and to the record no more often than a
		// checkpoint would have been written, because a person cannot read a
		// number that changes fifty times a second.
		r.step(keep, &job.Step{Name: "verifying", Ordinal: 3, Of: phases})
		said := time.Now()
		total, digest, err := hashFile(ctx, final, func(done, of int64) {
			keep.beat()
			if r.PersistInterval > 0 && time.Since(said) < r.PersistInterval {
				return
			}
			said = time.Now()
			r.step(keep, &job.Step{
				Name:    "verifying",
				Ordinal: 3,
				Of:      phases,
				Done:    done,
				Total:   of,
			})
		})
		if err != nil {
			if ferr := keep.fenced(); ferr != nil {
				return fmt.Errorf("verifying what %s delivered: %w", rec.Delegation.System, ferr)
			}
			return err
		}

		// The fence, before the two writes that are not idempotent. An owner
		// whose lease lapsed while it hashed must not tell the store this file
		// is delivered, and must not reset the checkpoint on a mismatch: a
		// successor may have claimed the job and be writing that path right now.
		//
		// Stopping rather than only checking, and stopping HERE rather than in
		// the deferred stop above: a renewal still in flight re-writes a record
		// it loaded moments ago, and would undo the terminal write below.
		if err := keep.stop(); err != nil {
			return err
		}
		if want := spec.Artifact.Digest; want != "" && !sameDigest(digest, want) {
			_, uerr := r.Store.Update(id, epoch, func(rr *job.Record) error {
				rr.Delegation = nil
				rr.State = job.StatePending
				rr.Error = fmt.Sprintf("%v: delegate delivered %s, want %s", ErrDigestMismatch, digest, want)
				return setCheckpoint(rr, Checkpoint{})
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
			return setCheckpoint(rr, Checkpoint{VerifiedPrefix: total})
		})
		return err
	}
	return fmt.Errorf("download: delegate reported unknown state %q", st.State)
}

// stranded is a delegated record naming a system this process cannot speak to.
//
// Repeating the complaint on every sweep — which is what this used to do, for
// good — is the one answer that is certainly wrong. Such a record is either
// somebody else's business, in which case saying anything is noise, or work
// nobody here can advance, in which case it wants saying once and then leaving
// alone. It cannot be told from the record which of the two it is: a delegation
// handle carries the delegate's name and nothing about who created it.
//
// So the note goes on the RECORD, and only if it is not already there. That is
// what makes "once" mean once. The `said` map in the supervisor cannot: a
// supervisor installed as a scheduled task is a fresh process on every sweep
// and remembers nothing, which is precisely how the same line came to be
// written for good. The store is the only memory this design has ever had.
//
// It is also the other half of the defect. The only place this surfaced was a
// log line, so a job stuck this way said nothing about being stuck, and the
// record read exactly like one a delegate was busy on. An absent record means
// refusal here as everywhere else: a supervisor that cannot move a job must not
// leave it looking healthy.
//
// Nothing else changes, and deliberately. The state is left alone because the
// delegate may be holding every byte, so this is waiting rather than failure,
// and a process that DOES understand the handle — the same machine with that
// tier linked in, or the one that delegated it — must still be able to finish
// the job and clear the note. Adopt skips delegated records, so nothing here
// starts fetching the same bytes a second time either.
func (r *Runner) stranded(rec *job.Record) error {
	system := rec.Delegation.System
	err := fmt.Errorf("%w %q", ErrStrandedHere, system)

	// Which of the two problems a person has, because the next thing they do
	// differs: a tier that is linked but did not come up is something to fix on
	// this machine, and one that was never linked is a job for another process.
	why := "no such tier is linked into this program"
	if linkedHere(system) {
		why = "that tier is linked here but did not come up"
	}
	note := fmt.Sprintf("waiting for a process that understands %q: %s", system, why)
	if rec.Error == note {
		return err
	}

	claimed, cerr := r.Store.Claim(rec.ID, r.Owner, r.LeaseTTL)
	if cerr != nil {
		// Somebody holds the lease, so somebody is working on it and this
		// process has nothing to add. Not a second failure to report.
		return err
	}
	epoch := claimed.Lease.Epoch
	_, uerr := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
		rr.Error = note
		return nil
	})
	r.Store.Release(rec.ID, epoch)
	if uerr != nil {
		return uerr
	}
	return err
}

// linkedHere reports whether a tier by this name is compiled into this program
// at all.
//
// The distinction RegisteredTiers was added for: "linked, but not available
// here" and "not linked at all" are very different problems and a user needs to
// tell them apart. It is asked of the program rather than of the runner on
// purpose — a runner may have been handed a shorter list by an operator running
// `--without bits`, and that is still a tier this build understands.
func linkedHere(system string) bool {
	for _, name := range RegisteredTiers() {
		if name == system {
			return true
		}
	}
	return false
}

// ReconcileAll walks every delegated download and brings each up to date. This
// is what a service runs on a timer, and on start after a reboot.
func (r *Runner) ReconcileAll(ctx context.Context) (int, error) {
	all, err := r.Store.List()
	if err != nil {
		return 0, err
	}
	n := 0
	var failed []error
	for _, rec := range all {
		if rec.Kind != Kind || !rec.Delegated() {
			continue
		}
		// Nothing to catch up on, and asking would undo it — see Reconcile.
		//
		// A terminal job is NOT skipped merely for being terminal, only for
		// having been delivered. Skipping it made the abandon-on-terminal path
		// unreachable: Reconcile knew to tell the delegate, and this loop never
		// called it, so a cancelled job left a NAS fetching 3.1 GB to completion
		// for nobody. Terminal means "this side is finished with it", which is
		// exactly when the other side needs telling.
		if rec.Delegation.Delivered || rec.State == job.StateTransferred {
			continue
		}
		if err := r.Reconcile(ctx, rec.ID); err != nil {
			// A delegate this process cannot speak to is not a problem the
			// sweep can report its way out of, and it is not a problem that
			// changes between sweeps. Reconcile has already put the reason
			// where a person will find it — on the record — so raising it here
			// as well is the line that printed on every sweep for good.
			//
			// It is not counted either. A job nothing here can move was not
			// reconciled, and saying it was is the same lie as the healthy
			// `reconciled=2` below.
			if errors.Is(err, ErrStrandedHere) {
				continue
			}
			// One unreachable delegate must not stop the others -- but it must
			// not vanish either. This was a bare `continue`, and the silence
			// cost real time: a job that could not progress looked exactly like
			// a job nobody had to touch, so a supervisor printed a healthy
			// `reconciled=2` every five seconds while one transfer was stuck
			// and another was destroying its own bytes.
			failed = append(failed, fmt.Errorf("%s: %w", rec.ID, err))
			continue
		}
		n++
	}
	// The count AND what went wrong. A caller that only reads the count learns
	// nothing about the jobs that could not be reconciled, which is the state
	// worth knowing about.
	return n, errors.Join(failed...)
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

// step records which phase this job is in, and doubles as this owner's proof
// that it still holds the lease.
//
// The write is still advisory: a step nobody could record must never fail the
// transfer it was only describing. But the ERROR is not advisory, and throwing
// it away is what hid the bug the keeper exists to fix. This was `_, _ =`, so
// once the lease lapsed under a long finalise the steps silently stopped
// landing and the first thing anyone heard about it was the terminal update
// failing, minutes later, with nothing to say which write had been the first to
// be refused.
//
// So a refusal for a stale epoch or a lapsed lease goes to the keeper, which
// remembers it and stops the work — that is the fence clause of the lease
// protocol, and this is one of the two places an owner learns it has been
// fenced. Anything else is ignored, as before.
func (r *Runner) step(k *keeper, st *job.Step) {
	_, err := r.Store.Update(k.id, k.epoch, func(rr *job.Record) error {
		rr.Progress.Step = st
		rr.Progress.UpdatedAt = job.At(time.Now())
		return nil
	})
	if err != nil {
		k.refused(err)
		return
	}
	// A phase that got as far as writing the record is a phase that moved.
	k.beat()
}

// abandonDelegated tells a delegate to stop work the local record has already
// given up on.
//
// Once a record is terminal it cannot be claimed, and so it cannot be updated to
// record that this was done — which is correct, because finished work is history
// and history does not change. The consequence is that this would run on every
// sweep forever, so a process remembers what it has abandoned. Forgetting across
// a restart is harmless: Abandon on a handle the delegate has already dropped is
// a no-op by contract.
func (r *Runner) abandonDelegated(ctx context.Context, rec *job.Record) error {
	if rec.Delegation == nil {
		return nil
	}
	handle := rec.Delegation.System + "\x00" + rec.Delegation.ExternalID
	if _, done := r.abandoned.Load(handle); done {
		return nil
	}
	d, ok := r.Delegators.BySystem(rec.Delegation.System)
	if !ok {
		// The tier is not linked into this process. Someone else's business,
		// and remembering would be a lie: a build that HAS that tier should
		// still get to abandon it.
		return nil
	}
	if err := d.Abandon(ctx, rec.Delegation.ExternalID); err != nil {
		return fmt.Errorf("download: abandoning %s on %s: %w", rec.ID, rec.Delegation.System, err)
	}
	r.abandoned.Store(handle, true)
	return nil
}
