package download

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

func clientOn(t *testing.T) (Client, job.Store) {
	t.Helper()
	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return idleClient(NewRunner(store, "test-owner")), store
}

// idleClient records submissions and starts no worker. Submit would otherwise
// start a goroutine on context.Background() that no test can stop, which left
// partial files open and made Windows refuse to remove the temp directory.
// Nothing here completes, so every job stays in flight and the deduplication is
// what is being measured.
func idleClient(r *Runner) *client {
	c := NewClient(r).(*client)
	c.idle = true
	return c
}

// Repeating the command is how a person resumes an interrupted download. It
// must not start a second transfer of the same bytes to the same path.
func TestAskingTwiceIsOneJob(t *testing.T) {
	svc, store := clientOn(t)
	spec := Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/x.bin"}},
		Sink:    Sink{Final: "out/x.bin"},
	}

	first, err := svc.Submit(spec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Submit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID() != second.ID() {
		t.Fatalf("two ids for one request: %s and %s", first.ID(), second.ID())
	}
	all, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("%d records in the store, want 1 — a repeat started a second transfer", len(all))
	}
}

// "Download it again" is a real request. A finished job is history, not a claim
// on the destination.
func TestAFinishedJobDoesNotBlockANewOne(t *testing.T) {
	svc, store := clientOn(t)
	spec := Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/y.bin"}},
		Sink:    Sink{Final: "out/y.bin"},
	}
	first, err := svc.Submit(spec)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := store.Claim(first.ID(), "someone", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(first.ID(), rec.Lease.Epoch, func(r *job.Record) error {
		r.State = job.StateComplete
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	second, err := svc.Submit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID() == first.ID() {
		t.Fatal("a completed job was reused; asking again must start fresh work")
	}
}

// A different destination is different work, even from the same source.
func TestSameSourceDifferentDestinationIsTwoJobs(t *testing.T) {
	svc, _ := clientOn(t)
	src := []Source{{Scheme: "https", Locator: "https://example.invalid/z.bin"}}
	a, err := svc.Submit(Spec{Sources: src, Sink: Sink{Final: "out/one.bin"}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.Submit(Spec{Sources: src, Sink: Sink{Final: "out/two.bin"}})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() == b.ID() {
		t.Fatal("two destinations collapsed into one job")
	}
}

// The hang, in one test.
//
// Every waiter this layer has ended on something the record SAYS: a terminal
// state, or an error somebody wrote down. A worker that dies before it records
// anything — a supervisor that stopped after the nudge, a claim that was never
// won — writes nothing at all, and a waiter with only those two exits waits for
// a transfer that is not happening until somebody kills it. That is 90 minutes
// of a test suite on somebody's machine.
func TestDeliverEndsWhenNobodyIsWorkingTheJob(t *testing.T) {
	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "test-owner")
	r.LeaseTTL = 200 * time.Millisecond
	// A client that records the job and never works it, which is what a worker
	// killed before its first write leaves behind.
	svc := idleClient(r)
	h, err := svc.Submit(Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/x.bin"}},
		Sink:    Sink{Final: "out/x.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := svc.Deliver(ctx, h.ID()); err == nil {
		t.Fatal("Deliver returned success for a job nobody ever worked")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Deliver waited until the test's own deadline — it would have waited forever")
	}
}

// A source that answers 404 is not a transfer that stumbled. Left adoptable it
// is fetched again on every sweep for as long as the store exists.
func TestARefusedSourceEndsTheJob(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "gone", http.StatusNotFound)
	}))
	defer srv.Close()

	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "test-owner")
	id, err := Submit(store, Spec{
		Sources: []Source{{Scheme: "http", Locator: srv.URL + "/model.bin"}},
		Sink:    Sink{Final: "out/model.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); err == nil {
		t.Fatal("a 404 was reported as a successful download")
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != job.StateFailed {
		t.Fatalf("state = %s, want %s — a refusal that stays adoptable is retried forever", rec.State, job.StateFailed)
	}
	if n, err := r.Adopt(context.Background()); err != nil || n != 0 {
		t.Fatalf("Adopt = %d (err %v); the job was picked up again", n, err)
	}
}

// A source that is merely down is the opposite: the job stays adoptable, and
// what stops it being fetched every thirty seconds forever is the wait between
// attempts.
func TestAFailedJobWaitsBeforeItIsTriedAgain(t *testing.T) {
	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "test-owner")
	// Failed runs explicitly release ownership; retry backoff needs no lease expiry.
	id, err := Submit(store, Spec{
		Sources: []Source{{Scheme: "http", Locator: "http://127.0.0.1:9/down"}},
		Sink:    Sink{Final: "out/down.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); err == nil {
		t.Fatal("a connection refused was reported as a successful download")
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if LastFailure(rec) == nil || !time.Now().Before(RetryAfter(rec)) {
		t.Fatal("the failed attempt did not persist a future retry deadline")
	}
	if rec.Lease.Owner != "" {
		t.Fatalf("failed attempt retained lease owner %q", rec.Lease.Owner)
	}
	if rec.State.Terminal() {
		t.Fatalf("state = %s; a source that is down is not a source that said no", rec.State)
	}
	orphans, err := store.Orphans()
	if err != nil || len(orphans) != 1 {
		t.Fatalf("orphans = %d (err %v); the job must stay adoptable", len(orphans), err)
	}
	if n, err := r.Adopt(context.Background()); err != nil || n != 0 {
		t.Fatalf("Adopt = %d (err %v); it was tried again at once", n, err)
	}
	if !time.Now().Before(RetryAfter(rec)) {
		t.Fatal("RetryAfter is in the past, so the sweep would retry immediately")
	}

	// A person asking again is not the sweep asking again: submitting clears the
	// last error, and a job with no error waits for nothing.
	if got := RetryAfter(&job.Record{Lease: job.Lease{Epoch: 40}}); !got.IsZero() {
		t.Fatalf("RetryAfter on a record with no error = %v, want zero", got)
	}
}
