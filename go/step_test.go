package download

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// A delegate whose Finalize does real work, and reports it.
type slowDelegate struct{}

func (d slowDelegate) System() string    { return "pretend-nas" }
func (d slowDelegate) Schemes() []string { return []string{"https", "http"} }
func (d slowDelegate) Capabilities() []Capability {
	return []Capability{CapResume, CapSurvivesProcessExit}
}

func (d slowDelegate) Start(ctx context.Context, spec Spec, from int64) (string, error) {
	return "external-1", nil
}

func (d slowDelegate) Poll(ctx context.Context, externalID string) (Status, error) {
	return Status{State: DelegateTransferred, Done: 6, Total: 6}, nil
}

func (d slowDelegate) Finalize(ctx context.Context, externalID, dest string) error {
	return d.FinalizeReporting(ctx, externalID, dest, nil)
}

func (d slowDelegate) FinalizeReporting(ctx context.Context, externalID, dest string,
	report func(done, total int64)) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	if report != nil {
		report(3, 6)
	}
	return os.WriteFile(dest, []byte("abcdef"), 0o644)
}

func (d slowDelegate) Abandon(ctx context.Context, externalID string) error { return nil }

// stepRecorder keeps every step the record passed through, taken from the write
// rather than from a reader sampling the file.
//
// A sampler sees only the phases that outlive its interval, so what it proves
// is how slow the machine is: this test polled every 2 ms and passed on Windows
// for a fortnight, then failed on Linux the first time it ran there, because
// hashing six bytes on a tmpfs is over before the first sample. Every phase
// change is an Update, so watching Update is the same observation with no clock
// in it.
type stepRecorder struct {
	*job.FileStore
	mu   sync.Mutex
	seen []string
}

func (s *stepRecorder) Update(id string, epoch int64, mutate func(*job.Record) error) (*job.Record, error) {
	r, err := s.FileStore.Update(id, epoch, mutate)
	if r == nil || r.Progress.Step == nil {
		return r, err
	}
	label := fmt.Sprintf("%d/%d %s", r.Progress.Step.Ordinal, r.Progress.Step.Of, r.Progress.Step.Name)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) == 0 || s.seen[len(s.seen)-1] != label {
		s.seen = append(s.seen, label)
	}
	return r, err
}

func (s *stepRecorder) steps() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

// A delegated download is three phases and only the first was ever visible. The
// far side reports done, and then this machine copies every byte back across a
// share and re-hashes them — with the record, before this, still showing the
// first transfer's numbers throughout. A person watched a finished-looking
// download do nothing, twice over.
func TestADelegatedDownloadSaysWhichPhaseItIsIn(t *testing.T) {
	dir := t.TempDir()
	fs, err := job.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store := &stepRecorder{FileStore: fs}

	final := filepath.Join(dir, "out", "model.bin")
	spec := Spec{
		Artifact: Artifact{Size: 6},
		Sources:  []Source{{Scheme: "https", Locator: "https://example.invalid/model.bin"}},
		Sink:     Sink{Final: final, Partial: final + ".part"},
	}
	id, err := Submit(store, spec)
	if err != nil {
		t.Fatal(err)
	}

	d := slowDelegate{}
	r := NewRunner(store, "test-runner")
	r.Delegators = NewDelegators(d)

	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := store.steps()
	if len(got) == 0 {
		t.Fatal("the record never said which phase it was in; the copy back is invisible again")
	}
	var sawCopy, sawVerify bool
	for _, label := range got {
		if label == "2/3 copying from pretend-nas" {
			sawCopy = true
		}
		if label == "3/3 verifying" {
			sawVerify = true
		}
	}
	if !sawCopy {
		t.Fatalf("no copying phase was reported, saw %v", got)
	}
	if !sawVerify {
		t.Fatalf("no verifying phase was reported, saw %v", got)
	}

	// And a finished job is not on a step. Leaving the last one set would leave
	// a completed download reading "verifying" forever, which is the same class
	// of lie as a finished one reading "paused".
	done, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if done.State != job.StateTransferred {
		t.Fatalf("state %q", done.State)
	}
	if done.Progress.Step != nil {
		t.Fatalf("a finished job is still on a step: %+v", done.Progress.Step)
	}
}
