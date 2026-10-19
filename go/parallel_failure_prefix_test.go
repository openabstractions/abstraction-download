package download

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// failOneUpdate refuses the first record write that when matches, and passes
// every other write to the file store.
type failOneUpdate struct {
	*job.FileStore
	mu    sync.Mutex
	fired bool
	when  func(*job.Record) bool
}

var errInjectedUpdate = errors.New("injected checkpoint failure")

func (s *failOneUpdate) Update(id string, epoch int64, mutate func(*job.Record) error) (*job.Record, error) {
	return s.FileStore.Update(id, epoch, func(rr *job.Record) error {
		if err := mutate(rr); err != nil {
			return err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.fired && s.when(rr) {
			s.fired = true
			return errInjectedUpdate
		}
		return nil
	})
}

// memoryRanges serves one artifact by range. While hold is set, the range
// starting at hold waits for the run to stop and lands nothing.
type memoryRanges struct {
	body []byte
	hold atomic.Int64
}

func (*memoryRanges) Schemes() []string          { return []string{"http"} }
func (*memoryRanges) Capabilities() []Capability { return []Capability{CapResume} }
func (*memoryRanges) Fetch(context.Context, Request) (Result, error) {
	return Result{}, errors.New("memoryRanges serves ranges only")
}
func (m *memoryRanges) Ranged(context.Context, Source, map[string]string) (int64, bool, error) {
	return int64(len(m.body)), true, nil
}
func (m *memoryRanges) FetchRange(ctx context.Context, req RangeRequest) error {
	if h := m.hold.Load(); h > 0 && req.Range.Start == h {
		<-ctx.Done()
		return ctx.Err()
	}
	_, err := req.Out.WriteAt(m.body[req.Range.Start:req.Range.End], req.Range.Start)
	return err
}

// A parallel run whose last range checkpoint fails must not record its range
// count as a prefix. Ranges [0,R) and [2R,3R) were proven, [R,2R) was not, and
// the total 2R written as a prefix claimed the hole; without a digest the next
// run delivered it as zeros.
func TestAParallelFailureDoesNotRecordItsTotalAsAPrefix(t *testing.T) {
	const R = minRangeSize
	body, _ := payload(t, 3*R)
	root := t.TempDir()
	files, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	store := &failOneUpdate{FileStore: files, when: func(rr *job.Record) bool {
		return rr.Progress.Done == 2*R
	}}
	src := &memoryRanges{body: body}
	src.hold.Store(R)
	r := NewRunner(store, "test-runner")
	r.Fetchers = NewFetchers(src)
	r.Connections = 2
	id, err := Submit(store, Spec{
		Artifact: Artifact{Size: int64(len(body))},
		Sources:  []Source{{Scheme: "http", Locator: "http://127.0.0.1/fixture"}},
		Sink:     Sink{Final: filepath.Join(root, "final.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Run(context.Background(), id); !errors.Is(err, errInjectedUpdate) {
		t.Fatalf("run = %v, want the injected checkpoint failure", err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	have, err := cp.Proven()
	if err != nil {
		t.Fatal(err)
	}
	if have.Missing(R, 2*R).Total() != R {
		t.Fatalf("checkpoint %v claims bytes of the range that never landed [%d,%d)", have, R, 2*R)
	}

	src.hold.Store(0)
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("resume: %v", err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("the delivered artifact has a hole the checkpoint called proven")
	}
}
