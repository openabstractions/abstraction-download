package download

import (
	"testing"
	"time"

	job "github.com/ReinisLusis/abstraction-job"
)

func serviceOn(t *testing.T) (Service, job.Store) {
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
	if err := Heartbeat(store, "test-supervisor@host:1", "here", time.Minute); err != nil {
		t.Fatal(err)
	}
	return NewService(r), store
}

// Repeating the command is how a person resumes an interrupted download. It
// must not start a second transfer of the same bytes to the same path.
func TestAskingTwiceIsOneJob(t *testing.T) {
	svc, store := serviceOn(t)
	spec := Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/x.bin"}},
		Sink:    Sink{Final: "/tmp/x.bin"},
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
	svc, store := serviceOn(t)
	spec := Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/y.bin"}},
		Sink:    Sink{Final: "/tmp/y.bin"},
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
	svc, _ := serviceOn(t)
	src := []Source{{Scheme: "https", Locator: "https://example.invalid/z.bin"}}
	a, err := svc.Submit(Spec{Sources: src, Sink: Sink{Final: "/tmp/one.bin"}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.Submit(Spec{Sources: src, Sink: Sink{Final: "/tmp/two.bin"}})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() == b.ID() {
		t.Fatal("two destinations collapsed into one job")
	}
}
