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
	"time"

	job "github.com/ReinisLusis/abstraction-job"
)

// Runner executes one download job: claim it, get the bytes, prove them, deliver
// them.
//
// Everything that must be identical across implementations lives here rather
// than in the Fetchers — hashing, resume, progress persistence, lease renewal,
// the final rename. That is what lets a transfer begun by one implementation be
// finished by a different one, which is the entire premise of the job layer.
type Runner struct {
	Store    *job.FileStore
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

func NewRunner(store *job.FileStore, owner string) *Runner {
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
	partial, final := spec.Sink.Resolve(r.Store.Root())

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

	total, err := r.fetch(ctx, rec, spec, epoch, f, h, from)
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
		if !strings.EqualFold(got, want) {
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

// fetch tries the sources in priority order and returns the total size of the
// partial file when one of them succeeds.
func (r *Runner) fetch(ctx context.Context, rec *job.Record, spec Spec, epoch int64, f *os.File, h hash.Hash, from int64) (int64, error) {
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
			Report: func(written int64) {
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
				if _, err := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
					rr.Progress.Done = at
					rr.Progress.UpdatedAt = job.At(time.Now())
					return rr.SetCheckpoint(Checkpoint{VerifiedPrefix: at})
				}); err != nil {
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
			continue // one bad job must not stop the rest being rescued
		}
		n++
	}
	return n, nil
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
