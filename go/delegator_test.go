package download

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	job "github.com/ReinisLusis/abstraction-job"
)

// fakeDelegate behaves the way BITS does, including the parts that are
// inconvenient: it writes the file itself, it does not release it until
// Finalize, and it does not check content — "BITS guarantees that the version of
// the file it transfers is consistent based on the file size and time stamp, not
// content".
type fakeDelegate struct {
	mu sync.Mutex
	// jobs maps an external handle to its state.
	jobs map[string]*fakeJob
	// deliver is what the delegate will actually write. Set it to something
	// other than the real payload to simulate a delegate that transferred
	// successfully and delivered the wrong bytes.
	deliver []byte
	// startErr makes Start fail, as it would where BITS is unavailable.
	startErr error
	next     int
}

type fakeJob struct {
	spec     Spec
	state    DelegateState
	done     int64
	total    int64
	tempPath string
	gone     bool
}

func newFakeDelegate(deliver []byte) *fakeDelegate {
	return &fakeDelegate{jobs: map[string]*fakeJob{}, deliver: deliver}
}

func (f *fakeDelegate) System() string    { return "fake-service" }
func (f *fakeDelegate) Schemes() []string { return []string{"https", "http", "smb", "file"} }
func (f *fakeDelegate) Capabilities() []Capability {
	// The whole reason to delegate. A delegator that cannot claim this is not
	// worth the indirection.
	return []Capability{CapResume, CapSurvivesProcessExit, CapDelegates}
}

func (f *fakeDelegate) Start(ctx context.Context, spec Spec, from int64) (string, error) {
	if f.startErr != nil {
		return "", f.startErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	id := fmt.Sprintf("{fake-guid-%d}", f.next)
	// Like BITS, the file goes to a temporary path under the service's control
	// and is not the caller's until Finalize.
	f.jobs[id] = &fakeJob{
		spec:     spec,
		state:    DelegateRunning,
		total:    int64(len(f.deliver)),
		tempPath: spec.Sink.Partial + ".fake-service",
	}
	return id, nil
}

func (f *fakeDelegate) Poll(ctx context.Context, id string) (Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return Status{State: DelegateGone}, nil
	}
	if j.gone {
		return Status{State: DelegateGone}, nil
	}
	return Status{State: j.state, Done: j.done, Total: j.total}, nil
}

func (f *fakeDelegate) Finalize(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return errors.New("no such job")
	}
	if j.state != DelegateTransferred {
		return errors.New("not transferred yet")
	}
	// Now, and only now, the bytes become the caller's.
	return os.WriteFile(j.spec.Sink.Final, f.deliver, 0o644)
}

func (f *fakeDelegate) Abandon(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.jobs, id)
	return nil
}

// advance moves a fake job along, the way the real service would while nobody
// was watching.
func (f *fakeDelegate) advance(id string, done int64, state DelegateState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j, ok := f.jobs[id]; ok {
		j.done = done
		j.state = state
	}
}

func (f *fakeDelegate) vanish(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j, ok := f.jobs[id]; ok {
		j.gone = true
	}
}

func (f *fakeDelegate) handleOf(t *testing.T, store *job.FileStore, id string) string {
	t.Helper()
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Delegated() {
		t.Fatal("job is not delegated")
	}
	return rec.Delegation.ExternalID
}

func newDelegatingRunner(t *testing.T, deliver []byte) (*Runner, *job.FileStore, *fakeDelegate, string) {
	t.Helper()
	r, store, root := newRunner(t)
	fd := newFakeDelegate(deliver)
	r.Delegators = NewDelegators(fd)
	return r, store, fd, root
}

// TestDelegateRecordsHandleAndLetsGo: after handing work over, this process must
// be free to exit. Holding the lease would stop anyone else polling or
// finalising, which would make the delegation pointless.
func TestDelegateRecordsHandleAndLetsGo(t *testing.T) {
	body, digest := payload(t, 8<<10)
	r, store, _, root := newDelegatingRunner(t, body)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})

	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	rec, _ := store.Load(id)
	if !rec.Delegated() {
		t.Fatal("no delegation was recorded; nothing could ever find this work again")
	}
	if rec.Delegation.System != "fake-service" || rec.Delegation.ExternalID == "" {
		t.Fatalf("delegation is unusable: %+v", rec.Delegation)
	}
	if !store.Claimable(rec) {
		t.Fatal("the lease was still held after delegating; nobody else could poll or finalise")
	}
}

// TestReconcileTracksProgressWithoutHoldingLease: a process that did not start
// the work can watch it. This is the property a callback cannot have, because a
// callback dies with the process that registered it.
func TestReconcileTracksProgressWithoutHoldingLease(t *testing.T) {
	body, digest := payload(t, 8<<10)
	r, store, fd, root := newDelegatingRunner(t, body)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})
	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	fd.advance(fd.handleOf(t, store, id), 4096, DelegateRunning)

	// A completely separate runner, as a service on a timer would be.
	watcher := NewRunner(store, "watcher")
	watcher.Delegators = NewDelegators(fd)
	if err := watcher.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec, _ := store.Load(id)
	if rec.Progress.Done != 4096 {
		t.Fatalf("progress = %d, want 4096", rec.Progress.Done)
	}
	if !store.Claimable(rec) {
		t.Fatal("the watcher kept the lease")
	}
}

// TestReconcileFinalizesAndVerifies is the two-phase completion BITS forces and
// the verification it does not do. The delegate says "transferred"; the file is
// not ours until Finalize; and the digest is checked by us afterwards.
func TestReconcileFinalizesAndVerifies(t *testing.T) {
	body, digest := payload(t, 16<<10)
	r, store, fd, root := newDelegatingRunner(t, body)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})
	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	handle := fd.handleOf(t, store, id)

	// Before Finalize the file does not exist at its final path at all.
	if _, err := os.Stat(finalOf(t, store, id)); err == nil {
		t.Fatal("the delegate released the file before it was asked to")
	}

	fd.advance(handle, int64(len(body)), DelegateTransferred)
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	rec, _ := store.Load(id)
	if rec.State != job.StateTransferred {
		t.Fatalf("state = %s, want transferred", rec.State)
	}
	if !rec.Delegation.Delivered {
		t.Fatal("delivery was not recorded; the job would be finalised twice")
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatal("delivered bytes differ from the source")
	}
}

// TestReconcileTwiceDoesNotUndoDelivery: a delegate destroys its job when told
// to hand the file over — BITS removes it from the queue on Complete() — so
// polling again reports the handle as unknown. Unknown means "take the work
// back", which would re-download a file already sitting correct at its final
// path. A delivered job must be left alone.
//
// Found by the BITS binding, not by the fake: StateTransferred is deliberately
// not terminal, because the consumer has still to acknowledge it, so the
// terminal check did not cover this.
func TestReconcileTwiceDoesNotUndoDelivery(t *testing.T) {
	body, digest := payload(t, 8<<10)
	r, store, fd, root := newDelegatingRunner(t, body)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})
	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	handle := fd.handleOf(t, store, id)
	fd.advance(handle, int64(len(body)), DelegateTransferred)
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	// The delegate forgets the job, exactly as BITS does after Complete().
	fd.Abandon(context.Background(), handle)

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	rec, _ := store.Load(id)
	if rec.State != job.StateTransferred {
		t.Fatalf("state = %s after a second reconcile; a delivered file was thrown away", rec.State)
	}
	if _, err := os.Stat(finalOf(t, store, id)); err != nil {
		t.Fatalf("the delivered file is gone: %v", err)
	}

	// And a sweep must leave it alone too.
	if _, err := r.ReconcileAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec, _ = store.Load(id)
	if rec.State != job.StateTransferred {
		t.Fatalf("state = %s after ReconcileAll; the sweep undid the delivery", rec.State)
	}
}

// TestDelegateDeliveringWrongBytesIsRefused is the reason this layer hashes even
// when the delegate reported success. BITS verifies size and timestamp only and
// says so; a delegate reporting "transferred" is not evidence the file is right.
func TestDelegateDeliveringWrongBytesIsRefused(t *testing.T) {
	body, digest := payload(t, 16<<10)
	wrong, _ := payload(t, 16<<10) // same length, different bytes — the shape no length check can see
	r, store, fd, root := newDelegatingRunner(t, wrong)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})
	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	handle := fd.handleOf(t, store, id)
	fd.advance(handle, int64(len(wrong)), DelegateTransferred)

	err := r.Reconcile(context.Background(), id)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Reconcile = %v, want ErrDigestMismatch", err)
	}
	rec, _ := store.Load(id)
	if rec.State == job.StateTransferred {
		t.Fatal("a job the delegate got wrong was accepted as transferred")
	}
	if rec.Delegated() {
		t.Fatal("the failed delegation was left in place; nothing would retry the work")
	}
	if rec.Error == "" {
		t.Fatal("nothing recorded why the delegate's delivery was rejected")
	}
}

// TestDelegateVanishingFallsBackToUs: BITS reaps jobs after 90 days, its queue
// database can be discarded wholesale when corrupt, and machines get rebuilt. A
// handle that no longer resolves is a normal outcome, and the work must survive
// it — the sources are still in the spec and we can do it ourselves.
func TestDelegateVanishingFallsBackToUs(t *testing.T) {
	body, digest := payload(t, 8<<10)
	r, store, fd, root := newDelegatingRunner(t, body)

	// A real local source, so the fallback can actually complete.
	src := filepath.Join(root, "mirror.bin")
	os.WriteFile(src, body, 0o644)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "file", Locator: src})

	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	fd.vanish(fd.handleOf(t, store, id))

	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec, _ := store.Load(id)
	if rec.Delegated() {
		t.Fatal("a vanished delegation was kept; the job is now unreachable")
	}
	if rec.State != job.StatePending {
		t.Fatalf("state = %s, want pending so somebody picks it up", rec.State)
	}

	// And it really is recoverable: an ordinary in-process run finishes it.
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("fallback Run: %v", err)
	}
	got, _ := os.ReadFile(finalOf(t, store, id))
	if string(got) != string(body) {
		t.Fatal("the fallback did not deliver the artifact")
	}
}

// TestReconcileAllSweeps is what a service runs on a timer and on start after a
// reboot: find every delegated job and bring it up to date.
func TestReconcileAllSweeps(t *testing.T) {
	r, store, fd, root := newDelegatingRunner(t, nil)
	body, digest := payload(t, 4<<10)
	fd.deliver = body

	ids := make([]string, 3)
	for i := range ids {
		ids[i] = submit(t, store, root, digest, int64(len(body)),
			Source{Scheme: "https", Locator: fmt.Sprintf("https://example.invalid/%d", i)})
		if err := r.Delegate(context.Background(), ids[i]); err != nil {
			t.Fatal(err)
		}
		fd.advance(fd.handleOf(t, store, ids[i]), 2048, DelegateRunning)
	}

	n, err := r.ReconcileAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("reconciled %d, want 3", n)
	}
	for _, id := range ids {
		rec, _ := store.Load(id)
		if rec.Progress.Done != 2048 {
			t.Fatalf("job %s progress = %d, want 2048", id, rec.Progress.Done)
		}
	}
}

// TestNoDelegatorMeansNoDelegation: with nothing registered, Delegate refuses
// rather than pretending. A caller that needs work to outlive its process must
// find out now, not discover it after being closed.
func TestNoDelegatorMeansNoDelegation(t *testing.T) {
	body, digest := payload(t, 1<<10)
	r, store, root := newRunner(t) // no delegators registered
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})

	if err := r.Delegate(context.Background(), id); !errors.Is(err, ErrNoDelegator) {
		t.Fatalf("Delegate = %v, want ErrNoDelegator", err)
	}
}

// TestReconcileRefusesUndelegatedJob: reconciling something nobody handed away
// is a caller error, not a no-op to be swallowed.
func TestReconcileRefusesUndelegatedJob(t *testing.T) {
	body, digest := payload(t, 1<<10)
	r, store, _, root := newDelegatingRunner(t, body)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "file", Locator: "nowhere"})

	if err := r.Reconcile(context.Background(), id); !errors.Is(err, ErrNotDelegated) {
		t.Fatalf("Reconcile = %v, want ErrNotDelegated", err)
	}
	_ = time.Now
}
