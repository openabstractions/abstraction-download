package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// A delegate whose Finalize takes real time and reports none of it — which is
// every delegate that has to bring a real model across a share, and is also the
// honest worst case, because nothing about holding a lease may depend on a
// delegate choosing to report.
type slowFinalise struct {
	body []byte
	take time.Duration
}

func (d *slowFinalise) System() string    { return "slow-nas" }
func (d *slowFinalise) Schemes() []string { return []string{"https"} }
func (d *slowFinalise) Capabilities() []Capability {
	return []Capability{CapResume, CapSurvivesProcessExit}
}

func (d *slowFinalise) Start(ctx context.Context, spec Spec, from int64) (string, error) {
	return "external-slow", nil
}

func (d *slowFinalise) Poll(ctx context.Context, externalID string) (Status, error) {
	n := int64(len(d.body))
	return Status{State: DelegateTransferred, Done: n, Total: n}, nil
}

func (d *slowFinalise) Finalize(ctx context.Context, externalID, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	select {
	case <-time.After(d.take):
	case <-ctx.Done():
		return ctx.Err()
	}
	return os.WriteFile(dest, d.body, 0o644)
}

func (d *slowFinalise) Abandon(ctx context.Context, externalID string) error { return nil }

// The finalise of a delegated download is a copy across a share and then a hash
// of every byte that arrived, both proportional to the size of the file. It ran
// under a lease claimed for LeaseTTL and renewed by nothing, so an owner holding
// the right epoch, displaced by nobody, was refused when it recorded work it had
// actually done:
//
//	claim --ttl 2      -> epoch=1 state=running
//	(4 seconds of honest work)
//	progress --epoch 1 -> jobctl: job: lease has expired
//
// The NAS proofs passed because 112 MB fits inside a thirty-second window. A
// multi-gigabyte model does not, so delegated download of anything large could
// not complete — and it failed quietly first, because the progress steps were
// refused for the same reason and their errors were thrown away.
func TestADelegatedFinaliseOutlastsItsOwnLease(t *testing.T) {
	dir := t.TempDir()
	store, err := job.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	body := []byte("a real one would be forty gigabytes of exactly this")
	sum := sha256.Sum256(body)
	final := filepath.Join(dir, "out", "model.bin")
	id, err := Submit(store, Spec{
		Artifact: Artifact{Size: int64(len(body)), Digest: "sha256:" + hex.EncodeToString(sum[:])},
		Sources:  []Source{{Scheme: "https", Locator: "https://example.invalid/model.bin"}},
		Sink:     Sink{Final: final, Partial: final + ".part"},
	})
	if err != nil {
		t.Fatal(err)
	}

	r := NewRunner(store, "test-runner")
	// Thirty seconds against a forty-gigabyte copy, scaled so a test can run it
	// in real time. Nothing here is injected: the reproduction is that real time
	// passes while an owner does honest work.
	r.LeaseTTL = 300 * time.Millisecond
	r.Delegators = NewDelegators(&slowFinalise{body: body, take: 1200 * time.Millisecond})

	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("an owner holding the right epoch, displaced by nobody, was refused: %v", err)
	}

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != job.StateTransferred {
		t.Fatalf("state %q, want %q — the terminal update never landed", rec.State, job.StateTransferred)
	}
	if !rec.Delegation.Delivered {
		t.Fatal("the delegate was never marked delivered, so the next sweep will fetch it all again")
	}
	if rec.Progress.Done != int64(len(body)) {
		t.Fatalf("progress done %d, want %d", rec.Progress.Done, len(body))
	}
	if got, err := os.ReadFile(final); err != nil || string(got) != string(body) {
		t.Fatalf("the file is not at its final path: %v", err)
	}
}

// A delegate that holds on and says nothing, forever. This is the hole a bare
// renewal timer would open: without a bound it would be renewed for as long as
// the process lived, and nobody could ever take the work.
type silentFinalise struct{ cap time.Duration }

func (d *silentFinalise) System() string    { return "silent-nas" }
func (d *silentFinalise) Schemes() []string { return []string{"https"} }
func (d *silentFinalise) Capabilities() []Capability {
	return []Capability{CapResume, CapSurvivesProcessExit}
}

func (d *silentFinalise) Start(ctx context.Context, spec Spec, from int64) (string, error) {
	return "external-silent", nil
}

func (d *silentFinalise) Poll(ctx context.Context, externalID string) (Status, error) {
	return Status{State: DelegateTransferred, Done: 8, Total: 8}, nil
}

func (d *silentFinalise) Finalize(ctx context.Context, externalID, dest string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d.cap):
		// Nothing stopped it. The cap is here so this fails as a test rather
		// than hanging as one.
		return nil
	}
}

func (d *silentFinalise) Abandon(ctx context.Context, externalID string) error { return nil }

// Renewal on a timer without a bound is the stall hole, so the timer is bounded
// by a silence budget: an owner renews while it is within the budget and stops
// renewing when the budget is spent, whether or not it managed to unblock
// itself. Stopping is what lets the work move.
func TestASilentFinaliseStopsHoldingTheLease(t *testing.T) {
	dir := t.TempDir()
	store, err := job.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := Submit(store, Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/model.bin"}},
		Sink:    Sink{Final: filepath.Join(dir, "out", "model.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}

	r := NewRunner(store, "test-runner")
	r.LeaseTTL = 400 * time.Millisecond
	r.SilenceBudget = 250 * time.Millisecond
	r.Delegators = NewDelegators(&silentFinalise{cap: 5 * time.Second})

	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatalf("delegate: %v", err)
	}

	began := time.Now()
	err = r.Reconcile(context.Background(), id)
	took := time.Since(began)
	if err == nil {
		t.Fatal("a finalise that reported nothing for its whole budget was allowed to carry on")
	}
	if !errors.Is(err, ErrStalled) {
		t.Fatalf("stopped for the wrong reason: %v", err)
	}
	if took > 3*time.Second {
		t.Fatalf("the watchdog did not stop it; the delegate's own cap did, after %s", took.Truncate(time.Millisecond))
	}

	// And the point of stopping is that somebody else can have it. The lease was
	// not broken by anyone — this owner stopped renewing and let it lapse.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, cerr := store.Claim(id, "somebody-else", time.Second); cerr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the lease never lapsed, so the stalled work is stranded on this owner forever")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The two halves, on the keeper itself, because the hash phase is the case that
// decides the design and it cannot be made slow on demand in a test.
//
// A multi-gigabyte verify is local CPU with no network progress to ride, it runs
// for minutes, and it is not a stall. What makes it survive is that it reports
// — hashFile calls back per chunk — and what a beat buys is exactly this: the
// lease is held for as long as the work keeps moving, however long that is, and
// dropped once it stops.
func TestAKeeperHoldsTheLeaseOnlyWhileTheWorkReports(t *testing.T) {
	dir := t.TempDir()
	store, err := job.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := Submit(store, Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/model.bin"}},
		Sink:    Sink{Final: filepath.Join(dir, "out", "model.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(id, "test-runner", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	epoch := claimed.Lease.Epoch

	r := NewRunner(store, "test-runner")
	r.LeaseTTL = 200 * time.Millisecond
	r.SilenceBudget = 500 * time.Millisecond

	ctx, keep := r.keep(context.Background(), id, epoch)

	// Four times the lease and well past the budget, all of it reporting.
	for i := 0; i < 20; i++ {
		keep.beat()
		time.Sleep(40 * time.Millisecond)
	}
	if ferr := keep.fenced(); ferr != nil {
		t.Fatalf("work that was reporting throughout was stopped anyway: %v", ferr)
	}
	if _, uerr := store.Update(id, epoch, func(*job.Record) error { return nil }); uerr != nil {
		t.Fatalf("the lease lapsed under work that was reporting: %v", uerr)
	}

	// And it ends when the reporting does.
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("a keeper silent for its whole budget was still holding the lease")
	}
	if ferr := keep.stop(); !errors.Is(ferr, ErrStalled) {
		t.Fatalf("stopped for the wrong reason: %v", ferr)
	}
}

// Verifying a delivered file was the second half of the silence: gigabytes of
// local hashing that ticked nothing, so the record could not say why `done` had
// stopped moving and the owner's own watchdog could not tell it from a stall.
func TestVerifyingSaysHowItIsGettingOn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivered.bin")
	body := make([]byte, 3<<20+7)
	for i := range body {
		body[i] = byte(i)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}

	calls := 0
	var last int64
	n, digest, err := hashFile(context.Background(), path, func(done, total int64) {
		calls++
		if done <= last {
			t.Fatalf("progress went backwards: %d after %d", done, last)
		}
		last = done
		if total != int64(len(body)) {
			t.Fatalf("total %d, the file is %d", total, len(body))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls < 3 {
		t.Fatalf("a %d-byte verify reported %d times; a 40 GB one has to say more than that", len(body), calls)
	}
	if n != int64(len(body)) || last != int64(len(body)) {
		t.Fatalf("hashed %d and last reported %d, file is %d", n, last, len(body))
	}
	sum := sha256.Sum256(body)
	if want := "sha256:" + hex.EncodeToString(sum[:]); digest != want {
		t.Fatalf("digest %s, want %s", digest, want)
	}

	// And a fenced owner stops hashing a file it may no longer act on, rather
	// than finishing and then being refused.
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := hashFile(stopped, path, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled verify returned %v", err)
	}
}
