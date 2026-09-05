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
	"strings"
	"sync"
	"time"

	job "github.com/ReinisLusis/abstraction/job/go"
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
	Fetchers *Registry
	// Delegators are the implementations that do the work elsewhere — a system
	// service, a NAS daemon. Empty by default: nothing is delegated to unless
	// somebody registers something that can be delegated to.
	Delegators *Delegators

	// Credentials turns a credential NAME from a source into the headers that
	// authenticate it. The record only ever holds the name; see credentials.go.
	Credentials Credentials

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
}

func NewRunner(store job.Store, owner string) *Runner {
	return &Runner{
		Store:        store,
		Fetchers:     DefaultRegistry(),
		Delegators:   NewDelegators(),
		Credentials:  EnvCredentials{},
		Owner:        owner,
		LeaseTTL:     30 * time.Second,
		PersistEvery: 8 << 20,
		// Comfortably inside LeaseTTL, so the lease is renewed several times
		// over before it could lapse.
		PersistInterval: 5 * time.Second,
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

	if err := r.run(ctx, rec, epoch); err != nil {
		// Record why, so a human reading the job later does not have to find
		// the log of a process that no longer exists.
		r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Error = err.Error()
			return nil
		})
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
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	var asked job.Want

	spec, err := SpecOf(rec)
	if err != nil {
		return err
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		return err
	}

	// The record's paths may be relative to the store, which is what lets the
	// same record be worked on by this machine or by a NAS that mounts the store
	// somewhere else entirely. Resolve once, here, and everything below deals in
	// paths that are real on this machine.
	partial, final := LocalSink(r.Store, spec.Sink)

	// Resume position: the smaller of what the checkpoint says was proven and
	// what is actually on disk. They differ after a crash — the checkpoint is
	// written periodically, so the file can be ahead of it, and a partial can
	// also be truncated or missing entirely. Trusting either one alone is how a
	// resumed download ends up the right length and the wrong bytes.
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		return err
	}
	onDisk := int64(0)
	if st, err := os.Stat(partial); err == nil {
		onDisk = st.Size()
	}
	from := cp.VerifiedPrefix
	if onDisk < from {
		from = onDisk
	}
	if err := truncate(partial, from); err != nil {
		return err
	}

	// Rebuild the rolling hash over the prefix we are keeping. This is the cost
	// of resuming honestly: a sequential read of what we already have, at disk
	// speed, instead of re-downloading it at network speed. It is also the only
	// way the digest check at the end covers bytes an earlier owner wrote.
	h := sha256.New()
	if from > 0 {
		if err := hashPrefix(partial, from, h); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			f.Close()
		}
	}()
	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return err
	}

	total, err := r.fetch(ctx, rec, spec, epoch, f, h, from, func(w job.Want) { asked = w; stop() })
	if asked != "" {
		// Stopping because somebody asked is not a failure, and must not be
		// recorded as one — the cancelled context surfaces here as an error, and
		// letting it through would write "context canceled" into a record a
		// person is looking at to see that their own button worked.
		//
		// Nothing is lost: the callback that noticed the intent had just synced
		// and checkpointed, so the proven prefix is durable to the byte.
		return r.honour(asked, rec.ID, epoch)
	}
	if err != nil {
		return err
	}

	// Close before verifying, not after. Windows refuses to delete or rename a
	// file that is still open, so a mismatch discovered while the handle is held
	// would leave the bad partial on disk for the next runner to resume onto —
	// the exact outcome the check exists to prevent.
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	closed = true

	// Length before digest: a short transfer has a more useful error message
	// than "the hash is wrong".
	if spec.Artifact.Size > 0 && total != spec.Artifact.Size {
		return fmt.Errorf("%w: got %d bytes, expected %d", ErrShortTransfer, total, spec.Artifact.Size)
	}

	if want := spec.Artifact.Digest; want != "" {
		got := "sha256:" + hex.EncodeToString(h.Sum(nil))
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
				return rr.SetCheckpoint(Checkpoint{VerifiedPrefix: 0})
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
		return rr.SetCheckpoint(Checkpoint{VerifiedPrefix: total})
	})
	return err
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

// fetch tries the sources in priority order and returns the total size of the
// partial file when one of them succeeds.
func (r *Runner) fetch(ctx context.Context, rec *job.Record, spec Spec, epoch int64, f *os.File, h hash.Hash, from int64, onIntent func(job.Want)) (int64, error) {
	sources := append([]Source(nil), spec.Sources...)
	sort.SliceStable(sources, func(i, j int) bool { return sources[i].Priority < sources[j].Priority })

	var lastErr error
	for _, src := range sources {
		fetcher, ok := r.Fetchers.For(src, rec.Requires)
		if !ok {
			lastErr = fmt.Errorf("%w: scheme %q", ErrNoFetcher, src.Scheme)
			continue
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
		w := io.MultiWriter(f, h)
		lastPersist := from
		lastPersistAt := time.Now()
		res, err := fetcher.Fetch(ctx, Request{
			Source:  src,
			From:    from,
			Out:     w,
			Headers: headers,
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
				updated, err := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
					rr.Progress.Done = at
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
					return rr.SetCheckpoint(Checkpoint{VerifiedPrefix: at})
				})
				if err != nil {
					return
				}
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
		if err == nil {
			return from + res.Written, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Cancellation is not a source failure; do not burn the other
			// sources on it.
			return from + res.Written, err
		}
		lastErr = fmt.Errorf("source %s: %w", src.Scheme, err)

		// A failed source may still have contributed bytes. Stop rather than
		// letting the next source append to a prefix this one left mid-write.
		if res.Written > 0 {
			return from + res.Written, lastErr
		}
	}
	if lastErr == nil {
		lastErr = ErrNoFetcher
	}
	return from, lastErr
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
func hashFile(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return n, "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func equalFold(a, b string) bool { return strings.EqualFold(a, b) }

func truncate(path string, size int64) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if size == 0 {
			return nil
		}
		return fmt.Errorf("download: cannot resume from %d, %s does not exist", size, path)
	}
	return os.Truncate(path, size)
}

func hashPrefix(path string, n int64, h hash.Hash) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.CopyN(h, f, n)
	return err
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
	_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
		rr.State = job.StateComplete
		return nil
	})
	r.Store.Release(id, epoch)
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
		_, final := LocalSink(r.Store, spec.Sink)
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
