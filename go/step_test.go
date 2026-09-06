package download

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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

// A delegated download is three phases and only the first was ever visible. The
// far side reports done, and then this machine copies every byte back across a
// share and re-hashes them — with the record, before this, still showing the
// first transfer's numbers throughout. A person watched a finished-looking
// download do nothing, twice over.
func TestADelegatedDownloadSaysWhichPhaseItIsIn(t *testing.T) {
	dir := t.TempDir()
	store, err := job.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

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

	// Watch every step the record passes through, by reading it the way any
	// other process would.
	var seen []string
	var mu sync.Mutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if rec, err := store.Load(id); err == nil && rec.Progress.Step != nil {
				label := fmt.Sprintf("%d/%d %s", rec.Progress.Step.Ordinal,
					rec.Progress.Step.Of, rec.Progress.Step.Name)
				mu.Lock()
				if len(seen) == 0 || seen[len(seen)-1] != label {
					seen = append(seen, label)
				}
				mu.Unlock()
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	d := slowDelegate{}
	r := NewRunner(store, "test-runner")
	r.Delegators = NewDelegators(d)

	if err := r.Delegate(context.Background(), id); err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if err := r.Reconcile(context.Background(), id); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	close(stop)
	wg.Wait()

	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()

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
