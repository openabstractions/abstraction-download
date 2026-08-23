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

// Delegator hands work to a supervisor watching a store on a shared filesystem.
type Delegator struct {
	// Root is the remote store as THIS machine sees it — a UNC path, a mapped
	// drive, an NFS mount. The machine on the other side sees the same
	// directory as something else entirely, and neither has to know that,
	// because records in it name their files relative to the store.
	Root string

	// Dir is where finished files land inside the remote store. Relative,
	// because it goes into a record the other machine reads.
	Dir string

	store *job.FileStore
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
	return &Delegator{Root: root, Dir: "models", store: s}, nil
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
		download.CapResume,
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
// What it reads may be minutes out of date, and that is not a bug here. The far
// side writes records through its local filesystem, so Samba never learns the
// file changed and never breaks the SMB read lease this client is holding. See
// TestLiveRecordFreshness: correctness survives it (claiming is a write, and
// writes are not cached) but observation does not.
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
	rec, err := d.store.Load(externalID)
	if err != nil {
		return fmt.Errorf("nas: %w", err)
	}
	spec, err := download.SpecOf(rec)
	if err != nil {
		return err
	}
	_, src := spec.Sink.Resolve(d.store.Root())
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("nas: the far side reported success but %s is not there: %w", src, err)
	}
	if dest != "" && !sameFile(src, dest) {
		if err := copyFile(src, dest); err != nil {
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
	rec, err := d.store.Claim(externalID, owner(), 30*time.Second)
	if err != nil {
		return nil
	}
	_, err = d.store.Update(externalID, rec.Lease.Epoch, func(r *job.Record) error {
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
func copyFile(src, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	tmp := dest + ".partial"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
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
