package download

import (
	"context"
	"crypto/sha256"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	job "github.com/ReinisLusis/abstraction-job"
)

// trickle serves a payload slowly enough that a person could press a button
// during it, which is the situation the whole of Intent exists for.
func trickle(t *testing.T, payload []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := int64(0)
		if rng := r.Header.Get("Range"); rng != "" {
			spec := strings.TrimSuffix(strings.TrimPrefix(rng, "bytes="), "-")
			if n, err := strconv.ParseInt(spec, 10, 64); err == nil {
				start = n
				w.Header().Set("Content-Range", "bytes "+itoa(start)+"-"+itoa(int64(len(payload)-1))+"/"+itoa(int64(len(payload))))
				w.Header().Set("Content-Length", itoa(int64(len(payload))-start))
				w.WriteHeader(http.StatusPartialContent)
			}
		} else {
			w.Header().Set("Content-Length", itoa(int64(len(payload))))
			w.WriteHeader(http.StatusOK)
		}
		buf := payload[start:]
		for i := 0; i < len(buf); i += 32 << 10 {
			end := i + 32<<10
			if end > len(buf) {
				end = len(buf)
			}
			if _, err := w.Write(buf[i:end]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			time.Sleep(40 * time.Millisecond)
		}
	}))
}

// The case schema 4 was added for, end to end: a live transfer, and somebody
// with no lease stopping it.
func TestPauseStopsALiveTransferAndKeepsWhatWasProven(t *testing.T) {
	payload := make([]byte, 3<<20)
	rand.New(rand.NewSource(7)).Read(payload)
	want := sha256.Sum256(payload)

	srv := trickle(t, payload)
	defer srv.Close()

	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "worker@host:1")
	// Checkpoint often, so the test is about intent rather than about waiting.
	r.PersistInterval = 200 * time.Millisecond
	r.PersistEvery = 64 << 10

	dest := t.TempDir() + "/out.bin"
	id, err := Submit(store, Spec{
		Sources: []Source{{Scheme: "http", Locator: srv.URL + "/p.bin"}},
		Sink:    Sink{Final: dest},
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background(), id) }()

	// Wait until it is genuinely moving, so the pause lands mid-transfer.
	waitFor(t, store, id, func(rec *job.Record) bool { return rec.Progress.Done > 0 })

	// The whole point: a different party, holding no lease and unable to get
	// one, asks it to stop.
	if _, err := store.SetIntent(id, job.WantPause, "a-person"); err != nil {
		t.Fatalf("set intent: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a pause was reported as a failure: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the owner never noticed the intent — it must check at least as often as it checkpoints")
	}

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State.Terminal() {
		t.Fatalf("state %q — pausing must not finish a job", rec.State)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if cp.VerifiedPrefix == 0 {
		t.Fatal("nothing was proven; a pause must keep the work already done")
	}
	if cp.VerifiedPrefix >= int64(len(payload)) {
		t.Fatal("it finished before the pause landed; make the payload slower")
	}
	if rec.Lease.Held(time.Now()) {
		t.Fatal("the lease is still held, so every reader is told somebody is working on a paused job")
	}
	// And it must not be swept up and restarted a moment later.
	orphans, err := store.Orphans()
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatalf("a paused job appeared as an orphan; a supervisor would resume it")
	}
	t.Logf("paused with %d of %d bytes proven, lease released", cp.VerifiedPrefix, len(payload))

	// Resume: intent back to run, and the work continues from the proven byte.
	if _, err := store.SetIntent(id, job.WantRun, "a-person"); err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("resume: %v", err)
	}
	got, err := readFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != want {
		t.Fatal("digest mismatch after pause and resume")
	}
	t.Logf("resumed and delivered %d bytes, digest matches", len(got))
}

// Cancelling a live transfer stops it and marks it cancelled, from outside.
func TestCancelStopsALiveTransfer(t *testing.T) {
	payload := make([]byte, 3<<20)
	rand.New(rand.NewSource(9)).Read(payload)
	srv := trickle(t, payload)
	defer srv.Close()

	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "worker@host:1")
	r.PersistInterval = 200 * time.Millisecond
	r.PersistEvery = 64 << 10

	id, err := Submit(store, Spec{
		Sources: []Source{{Scheme: "http", Locator: srv.URL + "/p.bin"}},
		Sink:    Sink{Final: t.TempDir() + "/out.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background(), id) }()
	waitFor(t, store, id, func(rec *job.Record) bool { return rec.Progress.Done > 0 })

	if err := job.Open(store, id, "a-person").Cancel(); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a cancellation was reported as a failure: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the owner never noticed the cancellation")
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != job.StateCancelled {
		t.Fatalf("state %q, want %q", rec.State, job.StateCancelled)
	}
	if rec.Error != "" {
		t.Fatalf("error %q recorded for a deliberate cancellation", rec.Error)
	}
}

func waitFor(t *testing.T, s job.Store, id string, ok func(*job.Record) bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if rec, err := s.Load(id); err == nil && ok(rec) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition never became true")
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func readFile(p string) ([]byte, error) { return os.ReadFile(p) }

// Repeating the command right after a kill must actually resume, and this is
// the case that hung.
//
// A killed process does not release its lease — that is the design. So the
// second command arrives INSIDE the dead owner's lease window and its claim is
// refused. Before the retry, that refusal went nowhere: the goroutine returned,
// nothing ran, and the command waited forever for a transfer nobody had started.
func TestRepeatingTheCommandInsideADeadOwnersLeaseStillResumes(t *testing.T) {
	payload := make([]byte, 2<<20)
	rand.New(rand.NewSource(11)).Read(payload)
	want := sha256.Sum256(payload)
	srv := trickle(t, payload)
	defer srv.Close()

	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "second-run@host:2")
	// Short, so the test waits seconds rather than half a minute — but the
	// behaviour under test is the same at any TTL.
	r.LeaseTTL = 3 * time.Second
	r.PersistInterval = 200 * time.Millisecond
	r.PersistEvery = 64 << 10
	svc := NewService(r)

	dest := t.TempDir() + "/out.bin"
	spec := Spec{
		Sources: []Source{{Scheme: "http", Locator: srv.URL + "/p.bin"}},
		Sink:    Sink{Final: dest},
	}

	// A first owner that dies without releasing anything, exactly as a kill
	// leaves it: the lease is written and never given back.
	id, err := Submit(store, spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(id, "killed@host:1", r.LeaseTTL); err != nil {
		t.Fatal(err)
	}

	// The person runs the same command again.
	h, err := svc.Submit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if h.ID() != id {
		t.Fatalf("started a new job %s instead of resuming %s", h.ID(), id)
	}

	deadline := time.After(60 * time.Second)
	for {
		rec, err := store.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		if rec.State == job.StateTransferred || rec.State == job.StateComplete {
			break
		}
		if rec.State.Terminal() {
			t.Fatalf("ended %s: %s", rec.State, rec.Error)
		}
		select {
		case <-deadline:
			t.Fatal("it never resumed — the dead owner's lease was never waited out")
		case <-time.After(100 * time.Millisecond):
		}
	}
	got, err := readFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != want {
		t.Fatal("digest mismatch after resuming")
	}
}
