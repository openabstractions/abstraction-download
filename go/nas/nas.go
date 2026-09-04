// Package nas delegates a download to a supervisor running somewhere else.
//
// It is the third implementation tier, and it needed no new wire format and no
// network protocol. The PC does not talk to the NAS; it writes a job record
// into a store the NAS can also see, and a jobd over there notices, claims it,
// downloads it and proves it. Delegating and polling are ordinary file
// operations on a share.
//
// # Why this is a Delegator and not a Fetcher
//
// A Fetcher streams bytes through this process and dies with it — the one
// property a NAS exists to avoid. The NAS keeps going with every application
// closed and the laptop asleep, and hands back nothing but a handle. That is
// the Delegator shape exactly, and it is the shape BITS has. Whoever configures
// a NAS gets it at every call site that already knew how to delegate.
//
// # Which is why the caller does not choose
//
// There is no "send this one to the NAS" argument anywhere in this package. An
// application asks for a model; the Runner offers the job to whatever is
// registered and capable, and registration comes from configuration. That is
// the delegation chain the project is for: in-process gives way to the system
// service, the system service gives way to the NAS, and nothing above knows
// which one answered.
package nas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	download "github.com/ReinisLusis/abstraction-download"
	job "github.com/ReinisLusis/abstraction-job"
)

// System is the name recorded in job.Delegation.System.
const System = "nas"

// EnvStore names the shared store. Configuration, not a flag: whether a NAS
// exists is a property of the machine, not of the download somebody asked for.
const EnvStore = "ABSTRACTION_NAS_STORE"

// DefaultDir is where delivered files land inside the remote store when nobody
// says otherwise. "files", not "models": this is the generic download tier.
const DefaultDir = "files"

// Delegator hands work to a supervisor watching a store on a shared filesystem.
type Delegator struct {
	// Root is the remote store as THIS machine sees it — a UNC path, a mapped
	// drive, an NFS mount. The machine on the other side sees the same
	// directory as something else entirely, and neither has to know that,
	// because records in it name their files relative to the store.
	Root string

	// Dir is where finished files land inside the remote store. Relative,
	// because it goes into a record the other machine reads.
	//
	// The default is deliberately not "models". This package moves bytes and has
	// no opinion about what they are; a layer that does know — the AI model layer
	// above — sets this to something meaningful for its domain. The moment the
	// generic tier starts naming directories after one kind of payload, it has
	// stopped being the generic tier.
	Dir string

	store job.Store
}

// New returns a delegator for a store on a shared filesystem.
func New(root string) (*Delegator, error) {
	if root == "" {
		return nil, errors.New("nas: no store configured")
	}
	s, err := job.NewFileStore(root)
	if err != nil {
		return nil, fmt.Errorf("nas: %s is not usable as a store: %w", root, err)
	}
	return &Delegator{Root: root, Dir: DefaultDir, store: s}, nil
}

// FromEnv builds a delegator from configuration, and returns nil when there is
// none. Nil is not an error: most machines have no NAS, and the Runner simply
// has one fewer implementation to offer work to.
func FromEnv() *Delegator {
	d, err := New(os.Getenv(EnvStore))
	if err != nil {
		return nil
	}
	return d
}

var _ download.Delegator = (*Delegator)(nil)

func (d *Delegator) System() string { return System }

// Schemes: whatever the far side can fetch. It runs this same download layer,
// so the answer is the list its own Registry would give.
func (d *Delegator) Schemes() []string { return []string{"http", "https"} }

// Capabilities. CapVerifies is claimed here and, unusually, it is true — the far
// side is this same code and hashes against the digest in the spec before it
// reports transferred. The Runner still verifies after delivery, because the
// bytes cross a network twice and the second crossing corrupts just as well as
// the first.
func (d *Delegator) Capabilities() []download.Capability {
	return []download.Capability{
		// NOT CapResume. The far side resumes its OWN partials perfectly well —
		// it runs the same runner over the same checkpoint — but that is not
		// what this capability promises. It promises that work already proven
		// HERE can be continued THERE, and it cannot: Start submits a fresh
		// record to the remote store, and the bytes this machine proved are on
		// this machine's disk, where the NAS cannot reach them. Copying them
		// across would cost roughly what re-fetching costs.
		//
		// Claiming it anyway meant a locally interrupted download was silently
		// restarted from zero on the NAS. Declining it means such a job stays
		// here and finishes here.
		download.CapSurvivesProcessExit,
		download.CapVerifies,
		download.CapDelegates,
	}
}

// Available reports whether the shared store can be reached right now. A NAS
// that is switched off, or a share that is not mounted, must not be delegated
// to: the job would be written into a directory nobody is watching, and it
// would look exactly like a download that had started.
func (d *Delegator) Available() error {
	if d == nil {
		return errors.New("nas: not configured")
	}
	if err := os.MkdirAll(filepath.Join(d.Root, "jobs"), 0o755); err != nil {
		return fmt.Errorf("nas: %s is not reachable: %w", d.Root, err)
	}
	return nil
}

// Start writes the job into the remote store and returns its id there.
//
// `from` is ignored deliberately. It counts bytes proven on THIS machine's
// disk, and the far side cannot see them — it has its own store, its own
// partial and its own checkpoint. Handing it a resume offset for a file it does
// not have is how a download ends up the right length and the wrong bytes.
func (d *Delegator) Start(ctx context.Context, spec download.Spec, from int64) (string, error) {
	if err := d.Available(); err != nil {
		return "", err
	}
	remote := spec
	// Relative and slash-separated, so the machine on the other side resolves it
	// against its own view of the store, whatever its separator is.
	remote.Sink = download.Sink{
		Final: path.Join(d.Dir, filepath.Base(filepath.FromSlash(spec.Sink.Final))),
	}
	id, err := download.Submit(d.store, remote)
	if err != nil {
		return "", fmt.Errorf("nas: %w", err)
	}
	return id, nil
}

// Poll reads the remote record — an ordinary file read that any process on any
// machine can do, including one that never started anything.
//
// Reads across SMB can lag: the far side writes records through its local
// filesystem, so Samba is not told the file changed and the client may serve a
// cached copy. In practice successive polls against a live NAS tracked a real
// download accurately, while an isolated probe on the same share stayed stale
// for 30s+, and the difference is not understood. See TestLiveRecordFreshness
// for what has been ruled out. Correctness does not depend on it — claiming is a
// write, and writes are not cached.
func (d *Delegator) Poll(ctx context.Context, externalID string) (download.Status, error) {
	rec, err := d.store.Load(externalID)
	if err != nil {
		// A handle that no longer resolves is a normal outcome, not an
		// exception: stores get cleaned up, NAS boxes get rebuilt, shares get
		// remounted. The Runner knows how to return such a job to pending with
		// its sources and checkpoint intact.
		return download.Status{State: download.DelegateGone}, nil
	}
	st := download.Status{Done: rec.Progress.Done, Total: rec.Progress.Total, Err: rec.Error}
	// Whether the far side has been asked to stop, which State deliberately
	// does not say: to a supervisor deciding whether to take work back, a
	// suspended job is neither failed nor finished, so it maps to Running.
	// Without this a paused transfer polls as an ordinary running one, and the
	// local side has no way to tell "stopped because somebody asked" from
	// "still going" — which is how a pause button lies.
	st.Suspended = rec.Paused()
	switch rec.State {
	case job.StateTransferred, job.StateComplete:
		st.State = download.DelegateTransferred
	case job.StateFailed, job.StateCancelled:
		st.State = download.DelegateFailed
	default:
		st.State = download.DelegateRunning
	}
	return st, nil
}

// Finalize brings the finished file to where the local job said it should be.
//
// Often that is nothing at all: when the destination is on the share, the far
// side already wrote it there and this is a stat. When the destination is a
// local disk the bytes have to cross the network, and they are copied rather
// than moved — a rename across a mount is not atomic and frequently not even
// possible, and the far side's copy is worth keeping until ours is verified.
func (d *Delegator) Finalize(ctx context.Context, externalID, dest string) error {
	return d.FinalizeReporting(ctx, externalID, dest, nil)
}

// FinalizeReporting is Finalize with progress, because on this delegate
// finalising is not a formality.
//
// BITS was told where to put the file when the job started and has been holding
// it there, so its Finalize is a call into the service. Here the bytes are on
// the far side of a share and every one of them has to cross, which for a large
// model is minutes of real transfer that the record used to describe as "done".
func (d *Delegator) FinalizeReporting(ctx context.Context, externalID, dest string,
	report func(done, total int64)) error {
	rec, err := d.store.Load(externalID)
	if err != nil {
		return fmt.Errorf("nas: %w", err)
	}
	spec, err := download.SpecOf(rec)
	if err != nil {
		return err
	}
	_, src := download.LocalSink(d.store, spec.Sink)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("nas: the far side reported success but %s is not there: %w", src, err)
	}
	if dest != "" && !sameFile(src, dest) {
		if err := copyFile(src, dest, report); err != nil {
			return fmt.Errorf("nas: bringing %s across: %w", filepath.Base(src), err)
		}
	}
	return d.acknowledge(externalID)
}

// acknowledge marks the remote job complete, which is the entire reason
// StateTransferred exists: the far side finished, and this is the requester
// saying so. Until it happens the remote record correctly reads "done, waiting
// to be taken delivery of", and jobd over there leaves it alone.
func (d *Delegator) acknowledge(id string) error {
	rec, err := d.store.Claim(id, owner(), 30*time.Second)
	if err != nil {
		// Someone else is mid-delivery. The state on the share is still true and
		// the file is still there, so this is not a failure of ours.
		return nil
	}
	_, err = d.store.Update(id, rec.Lease.Epoch, func(r *job.Record) error {
		r.State = job.StateComplete
		return nil
	})
	return err
}

// Abandon cancels the remote job and deliberately leaves whatever arrived where
// it is. BITS's Cancel deletes completed files along with partial ones, which
// surprised everybody who met it; this does not copy that.
func (d *Delegator) Abandon(ctx context.Context, externalID string) error {
	// Read before claiming, because a job that is already cancelled cannot be
	// claimed and its bytes still have to go. Abandon is called again on a
	// retry, and it has to finish the job it started.
	rec, err := d.store.Load(externalID)
	if err != nil {
		return nil // a handle nobody knows is already abandoned
	}

	// The bytes, not just the bookkeeping.
	//
	// This cancelled the record and left the file, and the contract on the
	// Delegator interface says the opposite in as many words: "Abandon cancels
	// the work and cleans up after it. Note that BITS's Cancel deletes completed
	// files as well as partial ones." So the same operation meant two different
	// things depending on which tier answered — which is the one thing a facade
	// may never allow.
	//
	// Measured, not theorised: a pause that reached the record as a cancel left
	// a complete 3.1 GB model sitting on a share as a transferred job with no
	// requester, and nothing was ever going to remove it.
	if spec, serr := download.SpecOf(rec); serr == nil {
		partial, final := download.LocalSink(d.store, spec.Sink)

		// The partial is named after this job's id, so it belongs to this job
		// and nothing else can want it.
		remove(externalID, partial)

		// The FINAL is not. Two runs fetching the same artifact name the same
		// file -- that is the identity rule this whole layer is built on -- so a
		// finished job and an abandoned one routinely point at one path.
		//
		// Deleting it unconditionally destroyed a completed download: an old
		// cancelled job was abandoned, took the 3.1 GB the CURRENT job had just
		// finished fetching, and did it again on every sweep, so the transfer
		// could never be finalised. The file was gone before anyone could
		// collect it.
		//
		// So the bytes go only when nobody else is still counting on them.
		if !claimedByAnother(d.store, rec.ID, spec.Sink.Final) {
			remove(externalID, final)
		}
	}

	if rec.State.Terminal() {
		return nil // already cancelled; the bytes above are the part that was left
	}
	claimed, err := d.store.Claim(externalID, owner(), 30*time.Second)
	if err != nil {
		return nil
	}
	_, err = d.store.Update(externalID, claimed.Lease.Epoch, func(r *job.Record) error {
		r.State = job.StateCancelled
		return nil
	})
	return err
}

func owner() string {
	h, _ := os.Hostname()
	return fmt.Sprintf("nas-delegator@%s:%d", h, os.Getpid())
}

func sameFile(a, b string) bool {
	if a == b {
		return true
	}
	fa, err := os.Stat(a)
	if err != nil {
		return false
	}
	fb, err := os.Stat(b)
	if err != nil {
		return false
	}
	return os.SameFile(fa, fb)
}

// copyFile writes through a .partial and renames, so an interrupted copy never
// leaves a short file sitting at the destination looking finished.
func copyFile(src, dest string, report func(done, total int64)) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	total := int64(0)
	if st, serr := in.Stat(); serr == nil {
		total = st.Size()
	}

	tmp := dest + ".partial"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, &reporting{r: in, total: total, report: report}); err != nil {
		out.Close()
		// The partial is NOT removed. Deleting it threw away every byte that had
		// crossed and made the next attempt start from zero — on a 40 GB model
		// over SMB that is the difference between a retry and an afternoon. It
		// is replaced wholesale by the next attempt, so keeping it costs one
		// file and risks nothing.
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dest)
}

// reporting counts bytes as they are read, so a copy across a share can say how
// far it has got. The same shape the Fetcher interface already uses for a
// download, because it is the same question asked about a second transfer.
type reporting struct {
	r      io.Reader
	total  int64
	done   int64
	report func(done, total int64)
	// Reported at most every 8 MiB. A share copy moves fast enough that
	// reporting every read would write the record thousands of times to say
	// something a person cannot see change.
	lastAt int64
}

func (x *reporting) Read(p []byte) (int, error) {
	n, err := x.r.Read(p)
	x.done += int64(n)
	if x.report != nil && (x.done-x.lastAt >= 8<<20 || err != nil) {
		x.lastAt = x.done
		x.report(x.done, x.total)
	}
	return n, err
}

// Suspend and Resume make pause mean something on this tier.
//
// The delegate here is not a black box: it is another supervisor watching a
// store both machines can see, running the same code, honouring intent at every
// checkpoint. So pausing it is not a new mechanism — it is the mechanism this
// project already built, used across a share.
//
// SetIntent is the one write that needs no lease, and this is exactly the case
// it was designed for: the far side holds the lease and is moving the bytes,
// and somebody here wants it to stop. Requiring a lease would mean stealing the
// job in order to pause it, which is the single thing the lease prevents.
//
// Without this the NAS was the one tier that could not pause. That is allowed —
// honourDelegated fails the job with a reason rather than pretending — but "in
// process can pause, BITS can pause, the NAS cannot" is a facade whose semantics
// depend on which tier happened to be chosen, and that is the thing this project
// exists to stop.
func (d *Delegator) Suspend(ctx context.Context, externalID string) error {
	_, err := d.store.SetIntent(externalID, job.WantPause, owner())
	if err != nil {
		return fmt.Errorf("nas: pausing %s: %w", externalID, err)
	}
	return nil
}

func (d *Delegator) Resume(ctx context.Context, externalID string) error {
	_, err := d.store.SetIntent(externalID, job.WantRun, owner())
	if err != nil {
		return fmt.Errorf("nas: resuming %s: %w", externalID, err)
	}
	return nil
}

// remove deletes one of a job's own files, best-effort. Failing to tidy up must
// never stop the record being cancelled, or the far side keeps working.
func remove(externalID, path string) {
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "nas: abandoning %s: could not remove %s: %v\n",
			externalID, filepath.Base(path), err)
	}
}

// claimedByAnother reports whether some OTHER job that is not finished with this
// artifact still names the same destination.
//
// Terminal records are ignored deliberately, with one exception: TRANSFERRED is
// not terminal and is exactly the state that means "the bytes are here and
// nobody has collected them yet", which is the strongest possible claim on a
// file. COMPLETE is terminal but still counts, because a completed job's bytes
// are the result somebody asked for.
//
// Erring toward keeping the file. The cost of a stale blob on a share is disk;
// the cost of deleting one another job is waiting for is the whole download.
func claimedByAnother(store job.Store, self, final string) bool {
	if final == "" {
		return false
	}
	all, err := store.List()
	if err != nil {
		return true // cannot tell, so do not delete
	}
	for _, other := range all {
		if other.ID == self || other.Kind != download.Kind {
			continue
		}
		if other.State.Terminal() && other.State != job.StateComplete {
			continue // failed or cancelled: it is not waiting for anything
		}
		spec, serr := download.SpecOf(other)
		if serr != nil {
			continue
		}
		if spec.Sink.Final == final {
			return true
		}
	}
	return false
}
