package download

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	r := NewRunner(store, "test-owner")
	// Pretend a supervisor is watching. Two reasons, both real: it is the tier
	// most machines actually run, and it means Submit hands the work over rather
	// than starting a goroutine on context.Background() that no test can stop —
	// which is what left partial files open and made Windows refuse to remove
	// the temp directory. Nothing here completes, so every job stays in flight
	// and the deduplication is what is being measured.
	//
	// The sinks in these tests are relative for the same reason: an absolute one
	// names a path only this machine has, so Submit works it here whatever is
	// watching. See TestAnAbsoluteSinkIsNotHandedToASupervisor.
	if err := Heartbeat(store, "test-supervisor@host:1", "here", time.Minute); err != nil {
		t.Fatal(err)
	}
	return NewClient(r), store
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
	// A supervisor that answers the heartbeat and then does nothing, which is
	// exactly what a killed jobd leaves behind for as long as its last beat
	// stays fresh.
	if err := Heartbeat(store, "gone@host:1", "here", time.Minute); err != nil {
		t.Fatal(err)
	}
	svc := NewClient(r)
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
	r.LeaseTTL = 100 * time.Millisecond
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

// A supervisor on a NAS handed a job whose sink is `C:\ComfyUI\models\x.safetensors`
// would write to a directory that exists here and not there — the bytes land
// somewhere useless, or nowhere, and the application waits for a file that was
// never coming. The ComfyUI node used to avoid this by never delegating at all,
// which is an application deciding something the layer knows better.
func TestAnAbsoluteSinkIsNotHandedToASupervisor(t *testing.T) {
	svc, store := clientOn(t) // a supervisor IS watching
	// And it is watching from somewhere else, which is what makes the sink
	// unreachable to it. A supervisor sharing this filesystem can deliver an
	// absolute path perfectly well and is handed the job; see onThisMachine.
	elsewhere(t, store)
	h, err := svc.Submit(Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/z.bin"}},
		Sink:    Sink{Final: filepath.Join(t.TempDir(), "z.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Nothing else is running, so an attempt that reaches the record at all can
	// only have been made in this process.
	deadline := time.Now().Add(10 * time.Second)
	for {
		rec, err := store.Load(h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if rec.Error != "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nobody worked it: state %s, error %q — it was left to a "+
				"supervisor that cannot reach the sink", rec.State, rec.Error)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// elsewhere rewrites the staged heartbeat so it names another machine.
//
// Heartbeat stamps the host it is called on, which is the honest thing for it
// to do and the wrong thing for a test about a supervisor that cannot see this
// filesystem.
func elsewhere(t *testing.T, store job.Store) {
	t.Helper()
	sup, live := SupervisorOf(store)
	if !live {
		t.Fatal("no supervisor to move")
	}
	sup.Host = "a-machine-that-is-not-this-one"
	b, err := json.Marshal(sup)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(heartbeatPath(store), b, 0o644); err != nil {
		t.Fatal(err)
	}
}
