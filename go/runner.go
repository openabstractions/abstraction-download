package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// Runner executes one download job: claim it, get the bytes, prove them, deliver
// them.
//
// Everything that must be identical across implementations lives here rather
// than in the Fetchers — hashing, resume, progress persistence, lease renewal,
// the final rename. That is what lets a transfer begun by one implementation be
// finished by a different one, which is the entire premise of the job layer.
type Runner struct {
	// abandoned remembers delegate handles this process has already told to
	// stop. A terminal record cannot be claimed, so there is nowhere durable to
	// write that fact -- and without it, every sweep would abandon the same job
	// again forever.
	abandoned sync.Map

	Store    job.Store
	Fetchers *Fetchers
	// Delegators are the implementations that do the work elsewhere — a system
	// service, a NAS daemon. Empty by default: nothing is delegated to unless
	// somebody registers something that can be delegated to.
	Delegators *Delegators

	// Credentials turns a credential NAME from a source into the headers that
	// authenticate it. The record only ever holds the name; see credentials.go.
	Credentials Credentials

	// Reach is asked for every host before a connection is opened to it, at the
	// same last moment a credential is resolved. Nil reaches everything.
	Reach Reach

	// SharedStore says the store this runner works may be written by machines
	// other than this one — a NAS share, an SMB mount. On such a store an
	// absolute sink is refused, because an absolute path names THIS machine's
	// filesystem and the record naming it was written by somebody else: the same
	// confused deputy the relative-sink containment closed, one spelling over.
	// A relative sink resolves under the store root and stays contained; an
	// absolute one is only ever legitimate for a caller writing to its own
	// machine, which is `dl -o` and never an adopted record. A supervisor sets
	// this; a bare runner leaves it off, which is what `dl` and a test want.
	SharedStore bool

	Owner string

	// LeaseTTL is how long a claim lasts. Short on purpose: a crashed owner's
	// job becomes available again after roughly this long, and the only cost of
	// a short TTL is more frequent renewals, which are one small file write.
	LeaseTTL time.Duration

	// PersistEvery is how much data may be transferred before the checkpoint is
	// written down. Everything past the last checkpoint is thrown away if this
	// process dies, so it trades re-downloaded bytes against record writes.
	// 8 MiB of re-download after a crash is cheap; fsyncing per chunk is not.
	PersistEvery int64

	// PersistInterval is the same trade measured in time, and it exists because
	// a byte threshold alone is silently wrong on a slow link: a real 313 MB
	// download killed after 12 seconds had 2 MB on disk and had checkpointed
	// NOTHING, because it had not yet reached 8 MiB. Ten minutes on a bad
	// connection would have saved nothing either — and a slow connection is
	// exactly when resuming matters most.
	//
	// It also keeps the lease alive. Renewal rides the same callback, so
	// without a time bound a transfer that is progressing slowly would let its
	// own lease expire and be adopted as an orphan while still running.
	PersistInterval time.Duration

	// Connections is how many ranges of one artifact this owner fetches at once.
	// One is the old behaviour exactly: one connection, appending, hashing as it
	// writes. Anything above one is still ONE owner holding ONE lease — see
	// parallel.go.
	Connections int

	// SilenceBudget is how long a phase that reports nothing may run before this
	// owner stops holding the lease.
	//
	// It exists because the lease TTL cannot answer two questions at once. Thirty
	// seconds is the right answer to "how fast do we notice a dead owner" and the
	// wrong answer by an order of magnitude to "how long is a silence
	// acceptable" — a 40 GB verify is minutes of local CPU with nothing to
	// report to a network, and it is not a stall. See keeper.go and
	// docs/stall-detection.md.
	SilenceBudget time.Duration
}

func NewRunner(store job.Store, owner string) *Runner {
	return &Runner{
		Store:        store,
		Fetchers:     DefaultFetchers(),
		Delegators:   NewDelegators(),
		Credentials:  EnvCredentials{},
		Owner:        owner,
		LeaseTTL:     30 * time.Second,
		PersistEvery: 8 << 20,
		Connections:  DefaultConnections,
		// Comfortably inside LeaseTTL, so the lease is renewed several times
		// over before it could lapse.
		PersistInterval: 5 * time.Second,
		SilenceBudget:   DefaultSilenceBudget,
	}
}

// Run claims the job and takes it as far as it can. A stop is not a catastrophe:
// the checkpoint is on disk, the lease will expire, and the next runner picks
// the job up from the last proven byte.
func (r *Runner) Run(ctx context.Context, id string) error {
	rec, err := r.Store.Claim(id, r.Owner, r.LeaseTTL)
	if err != nil {
		return err
	}
	epoch := rec.Lease.Epoch
	defer job.KeepAwake(r.Store, rec).Release()

	if err := r.run(ctx, rec, epoch); err != nil {
		// Record why, so a human reading the job later does not have to find
		// the log of a process that no longer exists — and record whether this
		// is over. A refusal that stays adoptable is fetched again on every
		// sweep for as long as the store exists, and nothing waiting on the
		// record can ever stop waiting.
		r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Error = err.Error()
			if Permanent(err) {
				rr.State = job.StateFailed
			}
			return nil
		})
		// And let go. This owner has stopped working, so holding the lease until
		// it lapses only makes the job unadoptable while nobody is moving any
		// bytes — and it makes a waiting requester unable to tell "failed" from
		// "still going", because both look like a job somebody holds.
		r.Store.Release(id, epoch)
		return err
	}
	// Let go. The bytes are delivered and proven, and the only thing left is for
	// whoever wanted them to say so — which means claiming this job. Holding the
	// lease until it lapses blocks exactly that, and it blocked it silently: the
	// requester's acknowledgement was refused, gave up quietly, and a finished
	// job sat on a NAS marked "waiting to be taken delivery of" forever.
	r.Store.Release(rec.ID, epoch)
	return nil
}

func (r *Runner) run(ctx context.Context, rec *job.Record, epoch int64) error {
	// Somebody may ask this job to stop while it is running, from a process that
	// holds no lease and never will. Honouring that is not optional: the job
	// layer's contract says an owner observes intent at least as often as it
	// checkpoints and moves toward it, and an owner that reads a record and
	// ignores the field is not an implementation of the abstraction.
	//
	// The check therefore lives at the checkpoint, where the record is being
	// written anyway — no extra reads, and the interval a person waits is the
	// interval they already accepted for progress.
	// Before anything is fetched, because this owner may have just adopted a job
	// whose predecessor died between the pause being asked for and the pause
	// being carried out. That record is left running under a lapsed lease, and
	// the only way out of it is for the next owner to honour what was asked
	// rather than start moving bytes and find out at its first checkpoint.
	if want := rec.Wants(); want != job.WantRun {
		return r.honour(want, rec.ID, epoch)
	}

	ctx, stop := context.WithCancel(ctx)
	defer stop()
	var asked job.Want
	intent := func(w job.Want) { asked = w; stop() }

	spec, err := SpecOf(rec)
	if err != nil {
		return err
	}
	// Whatever this store has already proven goes ahead of every source the
	// record names. On the machine that adopts a delegated job that is the
	// delegate's own earlier delivery, which the submitter cannot see and the
	// record therefore cannot name.
	spec.Sources = append(proven(r.Store, spec.Artifact.Digest, rec.ID), spec.Sources...)

	// The record's paths may be relative to the store, which is what lets the
	// same record be worked on by this machine or by a NAS that mounts the store
	// somewhere else entirely. Resolve once, here, and everything below deals in
	// paths that are real on this machine.
	partial, final, err := LocalSink(r.Store, rec.ID, spec.Sink)
	if err != nil {
		return err
	}
	if err := r.refuseUnportableSink(spec.Sink); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		return err
	}

	total, got, seen, err := r.transfer(ctx, rec, spec, epoch, partial, intent)
	if asked != "" {
		// Stopping because somebody asked is not a failure, and must not be
		// recorded as one — the cancelled context surfaces here as an error, and
		// letting it through would write "context canceled" into a record a
		// person is looking at to see that their own button worked.
		//
		// Nothing is lost: the callback that noticed the intent had just synced
		// and checkpointed, so the proven bytes are durable to the byte.
		return r.honour(asked, rec.ID, epoch)
	}
	if err != nil {
		r.keepProven(rec.ID, epoch, total, seen)
		return err
	}

	// Length before digest: a short transfer has a more useful error message
	// than "the hash is wrong".
	if spec.Artifact.Size > 0 && total != spec.Artifact.Size {
		r.keepProven(rec.ID, epoch, total, seen)
		return fmt.Errorf("%w: got %d bytes, expected %d", ErrShortTransfer, total, spec.Artifact.Size)
	}

	if want := spec.Artifact.Digest; want != "" {
		// Compare the hex, not the label.
		//
		// A real 1.5 GB download of CORRECT bytes was rejected here because
		// Lemonade wrote the digest bare and this side builds "sha256:<hex>":
		// the error read "got sha256:1fc70f… want 1fc70f…", the same digest
		// twice. Spec.Validate refuses a bare digest at submission, but an
		// application that writes records through its own implementation never
		// passes through it — and the job layer must not parse a spec to check,
		// because that opacity is what lets download evolve without a schema
		// change in three languages.
		//
		// So the writer is wrong and is being fixed, and this is liberal anyway.
		// Throwing away bytes that match, over a prefix, is the worst possible
		// trade: the check exists to refuse wrong bytes, and refusing right ones
		// costs exactly what it was built to save.
		if !sameDigest(got, want) {
			// Do not keep bytes that failed. Leaving them would mean the next
			// runner resumes onto a prefix already known to be wrong.
			os.Remove(partial)
			r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
				rr.Progress.Done = 0
				return setCheckpoint(rr, Checkpoint{})
			})
			return fmt.Errorf("%w: got %s, want %s", ErrDigestMismatch, got, want)
		}
	}

	if err := deliver(partial, final); err != nil {
		return err
	}

	// Transferred, not complete: the bytes are here and proven, but whoever
	// wanted them has not said so yet. See job.StateTransferred.
	_, err = r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
		rr.Progress.Done = total
		rr.Progress.UpdatedAt = job.At(time.Now())
		rr.State = job.StateTransferred
		rr.Error = ""
		return setCheckpoint(rr, Checkpoint{VerifiedPrefix: total, Validators: seen})
	})
	return err
}

// keepProven writes down what a failed attempt reached, before the failure is
// recorded.
//
// The periodic checkpoint fires on a byte count or an interval, so a transfer
// that stops before the first one has proven nothing as far as the store is
// concerned — and the next owner re-fetches bytes that are sitting on the disk
// in front of it. That is a rounding error on a 64-byte scenario and the entire
// product on a 40 GB model over a link that drops every ten minutes.
//
// Never downwards, which is what makes this safe for a plan that filled holes:
// Progress.Done is the total of every proven range and this offers only a
// prefix, so a checkpoint that already knows more keeps what it knows.
func (r *Runner) keepProven(id string, epoch, prefix int64, seen Validators) {
	if prefix <= 0 {
		return
	}
	r.Store.Update(id, epoch, func(rr *job.Record) error {
		if rr.Progress.Done >= prefix {
			return nil
		}
		rr.Progress.Done = prefix
		rr.Progress.UpdatedAt = job.At(time.Now())
		return setCheckpoint(rr, Checkpoint{VerifiedPrefix: prefix, Validators: seen})
	})
}

// transfer gets the bytes and returns how many of the artifact are now on disk,
// with its digest if the transfer was in a position to compute one.
//
// A run that appends to the end of the file hashes as it writes, which is what
// this layer has always done. A gapped plan cannot: sha256 is order-dependent
// and a range landing at 1 GB says nothing about the bytes before it. That one
// is hashed in a second pass, here, where the choice between the two is made
// and both answers come out the same shape.
func (r *Runner) transfer(ctx context.Context, rec *job.Record, spec Spec, epoch int64, partial string, intent func(job.Want)) (int64, string, Validators, error) {
	serving, p, ok := r.rangePlan(ctx, rec, spec)
	var (
		total int64
		sum   string
		seen  Validators
		err   error
	)
	if ok {
		var have Ranges
		if have, err = resumable(rec, partial); err != nil {
			return 0, "", seen, err
		}
		total, err = r.parallel(ctx, rec, epoch, partial, serving, p, have, intent)
		if err != nil || total != p.size {
			return total, "", seen, err
		}
	} else if total, sum, seen, err = r.stream(ctx, rec, spec, epoch, partial, intent); err != nil {
		return total, "", seen, err
	}
	if sum != "" || spec.Artifact.Digest == "" {
		return total, sum, seen, nil
	}
	hctx, keep := r.keep(ctx, rec.ID, epoch)
	_, sum, err = hashFile(hctx, partial, func(int64, int64) { keep.beat() })
	if ferr := keep.stop(); ferr != nil {
		return total, "", seen, ferr
	}
	return total, sum, seen, err
}

// stream is one connection per gap, appending or filling holes, and hashing as
// it writes when the plan allows.
//
// The plan decides everything: which bytes are already proven, which version of
// the artifact they came from, how much of the tail has to be cut off, and
// which holes are left to ask for. See planResume.
func (r *Runner) stream(ctx context.Context, rec *job.Record, spec Spec, epoch int64, partial string, onIntent func(job.Want)) (int64, string, Validators, error) {
	var seen Validators
	cp, err := CheckpointOf(rec)
	if err != nil {
		return 0, "", seen, err
	}
	plan, err := planResume(partial, cp, spec.Artifact.Size)
	if err != nil {
		return 0, "", seen, err
	}

	// Cut the partial back to the highest proven offset, not to the byte the
	// next request starts at. Those were the same number while progress was a
	// prefix and are not the same number now: cutting at the resume point would
	// delete every proven range past the first hole, and a length check could
	// never notice afterwards, because the finished file would still be exactly
	// the right length.
	if err := truncate(partial, plan.Trim); err != nil {
		return 0, "", seen, err
	}

	proven := plan.Have
	if rec.Progress.Done != proven.Total() {
		r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
			rr.Progress.Done = proven.Total()
			return setCheckpoint(rr, Checkpoint{Verified: proven, Validators: plan.Validators})
		})
	}

	// Rebuild the rolling hash over what we are keeping. This is the cost of
	// resuming honestly: a sequential read of what we already have, at disk
	// speed, instead of re-downloading it at network speed.
	//
	// Only for a plan that is one stream to the end, because sha256 is
	// order-dependent: a plan that fills holes writes bytes out of order, and a
	// rolling hash over that order is not the artifact's digest and never will
	// be. That plan hashes the finished file instead, in transfer.
	rolling := plan.oneStream()
	h := sha256.New()
	if rolling && proven.VerifiedPrefix() > 0 {
		// Local CPU work proportional to the file, with nothing to report to
		// the store. A 40 GB partial re-read at disk speed is minutes and the
		// lease claimed a moment ago lasts thirty seconds, so the first
		// checkpoint after a resume was once refused for a lease that lapsed
		// while this owner sat reading its own file. Hold it on a timer, fed by
		// the read itself.
		hctx, keep := r.keep(ctx, rec.ID, epoch)
		err := hashPrefix(hctx, partial, proven.VerifiedPrefix(), h, keep.beat)
		if ferr := keep.stop(); ferr != nil {
			return 0, "", seen, ferr
		}
		if err != nil {
			return 0, "", seen, err
		}
	}

	f, err := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return 0, "", seen, err
	}
	closed := false
	defer func() {
		if !closed {
			f.Close()
		}
	}()

	// seen follows the transfer: what the source says about the version it is
	// serving, updated as responses arrive and cleared if the stream restarts.
	seen = plan.Validators

	// How far the artifact reaches contiguously from byte zero. Seeded from what
	// is already proven rather than left at nought, because a plan can have no
	// gaps at all: a transfer whose last hole was filled by an owner that died
	// before it could deliver has everything on disk and nothing to ask for.
	total := proven.VerifiedPrefix()
	for _, gap := range plan.Gaps {
		var restarted bool
		total, restarted, err = r.fetch(ctx, rec, spec, epoch, f, h, rolling, gap, &proven, &seen, onIntent)
		if err != nil {
			break
		}
		if restarted {
			// A source answered with a different artifact and the file has been
			// rewritten from byte zero by the stream that just finished. Every
			// remaining gap was a hole in a file that no longer exists.
			break
		}
	}

	// Close before verifying, not after. Windows refuses to delete or rename a
	// file that is still open, so a mismatch discovered while the handle is held
	// would leave the bad partial on disk for the next runner to resume onto.
	if serr := f.Sync(); serr != nil && err == nil {
		return total, "", seen, serr
	}
	if cerr := f.Close(); cerr != nil && err == nil {
		return total, "", seen, cerr
	}
	closed = true
	if err != nil {
		return total, "", seen, err
	}
	if !rolling {
		// The holes were filled out of order, so the rolling hash is not the
		// artifact's digest. transfer hashes the finished file instead.
		return total, "", seen, nil
	}
	return total, "sha256:" + hex.EncodeToString(h.Sum(nil)), seen, nil
}

// honour carries out what somebody asked for, once the transfer has stopped.
//
// This is the half of the contract that makes Intent a contract rather than a
// field: the job layer says an owner must converge on what was asked, and this
// is where an owner does it.
func (r *Runner) honour(want job.Want, id string, epoch int64) error {
	switch want {
	case job.WantCancel:
		_, err := r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.State = job.StateCancelled
			rr.Error = ""
			return nil
		})
		return err
	case job.WantPause:
		// Release rather than hold. Paused means nobody is working on it, and a
		// held lease tells every reader the opposite — including the status line
		// a person is watching to see whether their pause took effect.
		//
		// Releasing is safe precisely because Orphans excludes paused jobs, so
		// letting go does not invite the next sweep to start it again. Those two
		// decisions only work as a pair.
		return r.Store.Release(id, epoch)
	}
	return nil
}

// ErrUnportableSink is an absolute sink met by a runner over a shared store.
//
// Not permanent: the record is not wrong, it is wrong HERE. An absolute path is
// valid for the machine whose filesystem it names — the caller that ran `dl -o`
// — and this refusal is a machine's policy about a store several machines can
// write, so the job stays adoptable exactly like ErrForeignPath and ErrUnreachable.
var ErrUnportableSink = errors.New("download: a shared store's record may only name a relative sink")

// refuseUnportableSink stops a runner over a shared store from writing an
// absolute sink chosen by a record another machine wrote. Lexical containment
// already refuses a relative sink that climbs out of the root; this closes the
// half it never covered, where the sink names a filesystem directly and never
// touches the root at all.
func (r *Runner) refuseUnportableSink(s Sink) error {
	if !r.SharedStore {
		return nil
	}
	for _, p := range []string{s.Final, s.Partial} {
		if p != "" && !relativeEverywhere(p) {
			return fmt.Errorf("%w: %s", ErrUnportableSink, p)
		}
	}
	return nil
}

func (r *Runner) fetch(ctx context.Context, rec *job.Record, spec Spec, epoch int64, f *os.File, h hash.Hash, rolling bool, gap fetchRange, proven *Ranges, seen *Validators, onIntent func(job.Want)) (int64, bool, error) {
	sources := append([]Source(nil), spec.Sources...)
	sort.SliceStable(sources, func(i, j int) bool { return sources[i].Priority < sources[j].Priority })

	from := gap.From
	to := gap.To
	restarted := false

	// fold records what this gap has proven so far. Everything before `at` in
	// this gap is on disk and framed by the transport, which is the same
	// standard the prefix was held to; it is now held per range instead of to
	// one tail.
	fold := func(at int64) (Ranges, error) { return proven.Add(from, at) }

	// restart is the one place the byte stream begins again with the file
	// already open and a rolling hash already part-filled — every other way of
	// starting at zero is settled before either exists, in run.
	//
	// A source has told us the artifact it holds is not the one
	// these bytes came from, so everything derived from those bytes goes: the
	// file back to nothing, the hash back to empty, the recorded prefix and the
	// validators that identified it back to zero. `from` goes with them, so that
	// the offsets this function reports, the checkpoints it writes and any
	// remaining source all agree the transfer now starts at zero.
	//
	// The persistence bookkeeping is reset too. It counts from the offset the
	// attempt began at, and left alone it would hold a number the file no
	// longer reaches — so nothing would be checkpointed until the transfer had
	// re-covered the ground it just threw away.
	lastPersist := from
	lastPersistAt := time.Now()
	restart := func() error {
		if err := f.Truncate(0); err != nil {
			return err
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		h.Reset()
		from = 0
		to = 0
		restarted = true
		*proven = nil
		*seen = Validators{}
		lastPersist = 0
		lastPersistAt = time.Now()
		_, err := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
			rr.Progress.Done = 0
			rr.Progress.UpdatedAt = job.At(time.Now())
			return setCheckpoint(rr, Checkpoint{})
		})
		return err
	}

	var lastErr error
	for _, src := range sources {
		fetcher, ok := r.Fetchers.For(src, rec.Requires)
		if !ok {
			lastErr = fmt.Errorf("%w: scheme %q", ErrNoFetcher, src.Scheme)
			continue
		}
		if err := r.Reach.check(HostOf(src.Locator)); err != nil {
			lastErr = err
			continue
		}
		if _, err := f.Seek(from, io.SeekStart); err != nil {
			return proven.VerifiedPrefix(), restarted, err
		}

		// Resolve the credential now, so the secret exists only for the length
		// of this request and never reaches the record or the Fetcher's source.
		headers, err := headersFor(src, r.Credentials)
		if err != nil {
			lastErr = err
			continue
		}

		// Everything written goes through the hash as well as the file, so the
		// bytes are proven as they land rather than by a second pass later.
		// Except when the bytes are landing out of order — then the hash means
		// nothing and the file is read back at the end instead. See run.
		var w io.Writer = f
		if rolling {
			w = io.MultiWriter(f, h)
		}
		lastPersist = from
		lastPersistAt = time.Now()
		res, err := fetcher.Fetch(ctx, Request{
			Source:     src,
			From:       from,
			To:         to,
			Validators: *seen,
			Out:        w,
			Headers:    headers,
			Reach:      r.Reach,
			Restart:    restart,
			Observed:   func(v Validators) { *seen = v },
			Report: func(written, total int64) {
				at := from + written
				// Bytes OR time, whichever comes first. The byte threshold
				// keeps a fast link from writing the record constantly; the
				// interval keeps a slow one from never writing it at all.
				enough := at-lastPersist >= r.PersistEvery
				overdue := r.PersistInterval > 0 && time.Since(lastPersistAt) >= r.PersistInterval
				if !enough && !overdue {
					return
				}
				lastPersist = at
				lastPersistAt = time.Now()
				// Durability before the claim: recording "the first N bytes are
				// proven" while N of them are still in a buffer would make a
				// crash resume from bytes that were never written.
				if err := f.Sync(); err != nil {
					return
				}
				next, ferr := fold(at)
				if ferr != nil {
					return
				}
				updated, err := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
					// Proven bytes, not an offset. The two are the same number
					// for a prefix and only the first means anything once a
					// transfer has holes in it — a sparse file reaches its
					// furthest written byte and says nothing about what is
					// between here and there.
					rr.Progress.Done = next.Total()
					// Only ever fill an unknown size in. A caller that supplied
					// one at submission — modelget resolves it from the registry
					// before any byte moves — has better information than a
					// response header, and a source that lies about its length
					// must not be able to overwrite the number the digest was
					// chosen against.
					if rr.Progress.Total == 0 && total > 0 {
						rr.Progress.Total = total
					}
					rr.Progress.UpdatedAt = job.At(time.Now())
					// The validators go down with the proven bytes, in the same
					// write. A checkpoint that records how far it got but not
					// WHICH version it got that far through is the checkpoint
					// this whole change exists to stop existing.
					return setCheckpoint(rr, Checkpoint{Verified: next, Validators: *seen})
				})
				if err != nil {
					return
				}
				*proven = next
				// The record was just read and written, so what somebody wants
				// is in hand at no extra cost. Stopping here rather than at the
				// end of the transfer is the difference between a pause button
				// that works and one that takes effect in forty minutes.
				if w := updated.Wants(); w != job.WantRun {
					onIntent(w)
					return
				}
				r.Store.Renew(rec.ID, epoch, r.LeaseTTL)
			},
		})
		// Whatever landed is proven to the same standard the prefix was held to,
		// so fold it in before answering — success or failure. On failure that
		// is the difference between a stop that keeps its bytes and one that
		// throws them away, and it is why the record is written here rather
		// than only on the periodic callback.
		if got, ferr := fold(from + res.Written); ferr == nil {
			*proven = got
		}

		if err == nil {
			return proven.VerifiedPrefix(), restarted, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Cancellation is not a source failure; do not burn the other
			// sources on it.
			return proven.VerifiedPrefix(), restarted, err
		}
		this := fmt.Errorf("source %s: %w", src.Scheme, err)

		// A failed source may still have contributed bytes. Stop rather than
		// letting the next source write over a hole this one left mid-write.
		if res.Written > 0 {
			return proven.VerifiedPrefix(), restarted, this
		}
		// A source that says no does not speak for a mirror that merely dropped
		// the connection. Only the answer that comes back decides whether the
		// job is over, so a retryable failure anywhere in the list outranks a
		// refusal that happened to come last.
		if lastErr == nil || Permanent(lastErr) || !Permanent(this) {
			lastErr = this
		}
	}
	if lastErr == nil {
		lastErr = ErrNoFetcher
	}
	return proven.VerifiedPrefix(), restarted, lastErr
}

// How long a job that recorded a failure is left alone before anybody tries it
// again, and the ceiling that wait grows to.
const (
	RetryDelay = 15 * time.Second
	RetryMax   = 15 * time.Minute
)

// RetryAfter is the earliest a job that failed may be picked up again.
//
// The attempt count is the lease epoch. It rises by one every time an owner
// claims the job, it is already in the record in all three languages, and it
// costs nothing to read — so backing off needs no new field and no change to a
// contract three implementations have to agree on forever.
//
// Only a job that recorded a failure waits. An owner that was killed never got
// to write one, so the crash-and-resume case — the case this project exists for
// — is adopted as fast as it always was.
func RetryAfter(rec *job.Record) time.Time {
	if rec.Error == "" {
		return time.Time{}
	}
	wait := RetryDelay
	for i := int64(1); i < rec.Lease.Epoch && wait < RetryMax; i++ {
		wait *= 2
	}
	if wait > RetryMax {
		wait = RetryMax
	}
	return rec.UpdatedAt.Time.Add(wait)
}

// Adopt runs every download nobody is working on. This is the reclamation path:
// a service calls it on start and finds the transfers that were in flight when
// the machine went to sleep, the app was closed, or the power went out.
func (r *Runner) Adopt(ctx context.Context) (int, error) {
	orphans, err := r.Store.Orphans()
	if err != nil {
		return 0, err
	}
	n := 0
	var failed []error
	for _, o := range orphans {
		// Someone else's kind of job is none of our business.
		if o.Kind != Kind {
			continue
		}
		// Neither is someone else's job. A record holding a delegation handle is
		// in another system's hands; Reconcile is the path for it, and running it
		// here would fetch the same bytes a second time while the first transfer
		// is still going. If the handle has gone stale, Reconcile is what clears
		// the delegation and returns the job to pending — and only then is it
		// ours to adopt.
		if o.Delegated() {
			continue
		}
		if time.Now().Before(RetryAfter(o)) {
			continue
		}
		if err := r.Run(ctx, o.ID); err != nil {
			// One bad job must not stop the rest being rescued -- and must not
			// disappear either. A bare `continue` here meant a job that failed
			// every single sweep was indistinguishable from one nobody needed
			// to touch.
			failed = append(failed, fmt.Errorf("%s: %w", o.ID, err))
			continue
		}
		n++
	}
	return n, errors.Join(failed...)
}

// hashFile reads a whole file and returns its size and digest. Used after a
// delegate reports success, because delegates do not check content: BITS
// verifies size and timestamp only, and says so in its own documentation.
//
// It reports as it goes, and that is not decoration. Verifying a delivered file
// is the longest phase of a delegated download after the transfer itself, it is
// local CPU with no network progress to ride, and on a multi-gigabyte model it
// legitimately runs for minutes. Something has to say that it is advancing, or
// the only two parties who care — the person watching a progress bar, and the
// owner's own watchdog — both read it as nothing happening.
//
// report may be nil. ctx stops it between chunks, which is what makes a fenced
// owner stop hashing a file it may no longer act on.
func hashFile(ctx context.Context, path string, report func(done, total int64)) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	total := int64(0)
	if st, serr := f.Stat(); serr == nil {
		total = st.Size()
	}
	h := sha256.New()
	n, err := copyReporting(ctx, h, f, total, report)
	if err != nil {
		return 0, "", err
	}
	return n, "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// copyReporting is io.Copy that says how it is getting on and can be stopped.
//
// A megabyte per chunk. io.Copy's own 32 KiB would call back forty thousand
// times for every gigabyte, and the callers here are feeding a watchdog and a
// progress bar rather than counting bytes — 40,000 calls for a 40 GB file is
// enough for both and cheap for neither party to ignore.
func copyReporting(ctx context.Context, dst io.Writer, src io.Reader, total int64, report func(done, total int64)) (int64, error) {
	buf := make([]byte, 1<<20)
	var done int64
	for {
		if err := ctx.Err(); err != nil {
			return done, err
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return done, werr
			}
			done += int64(n)
			if report != nil {
				report(done, total)
			}
		}
		if rerr == io.EOF {
			return done, nil
		}
		if rerr != nil {
			return done, rerr
		}
	}
}

func truncate(path string, size int64) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if size == 0 {
			return nil
		}
		return fmt.Errorf("download: cannot resume from %d, %s does not exist", size, path)
	}
	return os.Truncate(path, size)
}

// hashPrefix rebuilds the rolling hash over bytes an earlier owner proved.
//
// beat is called per chunk and writes nothing. That is the distinction the
// keeper exists to draw: whether the work is moving is an in-process question
// answered thousands of times a second for free, and whether the record should
// say so is a separate one answered every few seconds at the cost of a file
// write. Conflating them is what put lease renewal on the data path.
func hashPrefix(ctx context.Context, path string, n int64, h hash.Hash, beat func()) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	copied, err := copyReporting(ctx, h, io.LimitReader(f, n), n, func(int64, int64) {
		if beat != nil {
			beat()
		}
	})
	if err != nil {
		return err
	}
	if copied < n {
		// The same answer io.CopyN gave: the prefix is shorter than the
		// checkpoint claimed, and resuming onto it would be resuming onto bytes
		// that are not there.
		return io.EOF
	}
	return nil
}

// deliver moves the proven bytes into place. Rename is atomic within a volume;
// across volumes it is not available at all, so fall back to a copy — Lemonade's
// downloader reached the same conclusion and does the same thing.
func deliver(partial, final string) error {
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	if err := os.Rename(partial, final); err == nil {
		return nil
	}
	in, err := os.Open(partial)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(final)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	in.Close()
	return os.Remove(partial)
}

// TakeDelivery is the requester saying "I have it".
//
// StateTransferred means the bytes are here and proven and nobody has claimed
// them yet. That second half existed in the design from the start — BITS forces
// it, and the survey found the same shape everywhere the worker and the consumer
// are different processes — but nothing in this layer ever performed it. So
// finished jobs accumulated in the store forever, `dl watch` listed downloads
// that had completed days earlier, and the state that was supposed to mean
// "waiting for you" meant "waiting for nobody".
//
// It is idempotent and it is not an error to call on a job somebody else has
// already acknowledged: taking delivery twice is the same as taking it once.
func (r *Runner) TakeDelivery(id string) error {
	rec, err := r.Store.Load(id)
	if err != nil {
		return err
	}
	if rec.State == job.StateComplete {
		return nil
	}
	if rec.State != job.StateTransferred {
		return fmt.Errorf("download: %s is %s, not %s", id, rec.State, job.StateTransferred)
	}
	claimed, err := r.Store.Claim(id, r.Owner, r.LeaseTTL)
	if err != nil {
		// Somebody else is mid-delivery. The bytes are still there and still
		// proven, so this is not a failure of ours.
		return nil
	}
	epoch := claimed.Lease.Epoch
	// No release afterwards. COMPLETE is terminal, so the store refuses every
	// write from this epoch including the one that would hand the lease back —
	// and there is nothing to hand it back to, because nothing may claim a
	// finished job either. The lease lapses on its own.
	_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
		rr.State = job.StateComplete
		return nil
	})
	return err
}

// TakeDeliveryAll closes out finished work whose bytes are demonstrably present.
//
// # Why this has to exist
//
// TRANSFERRED means the bytes arrived and were proven, and COMPLETE means
// somebody said "I have them". The two-phase ending is not bureaucracy: it is
// the only way to express "the service finished this while your application was
// closed", and BITS enforces the same shape by refusing to release a file until
// Complete is called.
//
// But nothing was performing the second half. Every other state survives its
// owner dying, because the lease lapses and a supervisor adopts the job.
// TRANSFERRED is deliberately excluded from that sweep — adopting it would
// re-download a finished file, which once cost a NAS 313 MB on a loop — and the
// exclusion that prevents the loop is the same one that strands the record.
//
// The result was visible in a real download manager: three rows sitting at 100%
// labelled "paused", for files that were complete on disk. A person seeing that
// reasonably clicks resume, and resume does nothing, because there is nothing
// left to fetch.
//
// # What it will and will not close
//
// Only jobs whose destination is actually there, at the size that was proven.
// A transferred job whose file is MISSING is a real pending transition — the
// bytes may be on a NAS and still have to cross — and that is the one case a
// person should ever see, because it is the only one where something still has
// to happen.
//
// It does not re-hash. The digest was checked when those bytes were proven, and
// rehashing gigabytes on every sweep would spend real time re-answering a
// question already answered.
func (r *Runner) TakeDeliveryAll(ctx context.Context) (int, error) {
	all, err := r.Store.List()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, rec := range all {
		if rec.Kind != Kind || rec.State != job.StateTransferred {
			continue
		}
		spec, err := SpecOf(rec)
		if err != nil {
			continue
		}
		_, final, err := LocalSink(r.Store, rec.ID, spec.Sink)
		if err != nil {
			// A record whose sink points out of the store is left where it is,
			// exactly like one whose spec will not decode. This sweep takes
			// delivery; it is not the place to adjudicate a bad record, and the
			// runner that tries to act on it will refuse out loud.
			continue
		}
		st, err := os.Stat(final)
		switch {
		case err == nil && !st.IsDir():
			// The bytes are where they were asked to go. Size is the cheap half
			// of the proof and the half that catches a truncated or replaced
			// file; zero total means the source never said, in which case the
			// file being present is all there is to go on.
			if want := rec.Progress.Total; want > 0 && st.Size() != want {
				continue
			}

		case rec.Delegated() && !rec.Delegation.Delivered:
			// A delegate still holds these bytes and they have yet to cross.
			// That is a real pending transition and the one case a person
			// should see, because something still has to happen.
			continue

		default:
			// Delivered, and since moved or consumed by whoever wanted it.
			//
			// TRANSFERRED is only ever set AFTER the bytes reach their final
			// path — the in-process runner delivers before marking it, and the
			// delegated path finalises first. So a missing file here does not
			// mean the delivery failed; it means it succeeded and something
			// then used the result. Lemonade downloads a backend zip to %TEMP%,
			// extracts it, and deletes the zip: correct behaviour that left a
			// record no file-existence test could ever satisfy, sitting in a
			// download manager as a finished-looking row labelled "paused",
			// forever.
			//
			// Requiring the file to still be there confused "did this arrive"
			// with "is it still where it landed", and only the first is this
			// layer's business.
		}
		if err := r.TakeDelivery(rec.ID); err != nil {
			continue
		}
		n++
	}
	return n, nil
}
