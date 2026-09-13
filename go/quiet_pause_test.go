package download

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

type quietPauseFetcher struct {
	payload []byte
	tail    int
	written chan struct{}
	starts  chan int64
	first   bool
}

func (*quietPauseFetcher) Schemes() []string          { return []string{"http"} }
func (*quietPauseFetcher) Capabilities() []Capability { return []Capability{CapResume} }
func (f *quietPauseFetcher) Fetch(ctx context.Context, req Request) (Result, error) {
	f.starts <- req.From
	end := len(f.payload)
	quiet := !f.first
	f.first = true
	if quiet {
		end = f.tail
	}
	n, err := req.Out.Write(f.payload[req.From:end])
	if req.Report != nil {
		req.Report(int64(n), int64(len(f.payload)))
	}
	result := Result{Written: int64(n), Total: int64(len(f.payload))}
	if err != nil {
		return result, err
	}
	if quiet {
		close(f.written)
		<-ctx.Done()
		return result, ctx.Err()
	}
	return result, nil
}

func TestQuietPauseRetainsSyncedTailAndResumesAfterIt(t *testing.T) {
	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("quiet-tail"), 1024)
	fetcher := &quietPauseFetcher{payload: payload, tail: 1234, written: make(chan struct{}), starts: make(chan int64, 2)}
	r := NewRunner(store, "quiet-pause-test")
	r.Fetchers = NewFetchers(fetcher)
	r.PersistEvery = 1 << 20
	r.PersistInterval = time.Hour
	dest := filepath.Join(t.TempDir(), "result")
	digest := sha256.Sum256(payload)
	id, err := Submit(store, Spec{Artifact: Artifact{Size: int64(len(payload)), Digest: fmt.Sprintf("sha256:%x", digest)}, Sources: []Source{{Scheme: "http", Locator: "http://127.0.0.1/fixture"}}, Sink: Sink{Final: dest}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, id) }()
	select {
	case <-fetcher.written:
	case <-ctx.Done():
		t.Fatal("fixture did not write its tail")
	}
	if start := <-fetcher.starts; start != 0 {
		t.Fatalf("initial offset %d", start)
	}
	if _, err = store.SetIntent(id, job.WantPause, "test-caller"); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("quiet pause did not finish")
	}
	record, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(record)
	if err != nil {
		t.Fatal(err)
	}
	ranges, err := cp.Proven()
	if err != nil {
		t.Fatal(err)
	}
	if got := ranges.VerifiedPrefix(); got != int64(fetcher.tail) {
		t.Errorf("durable prefix = %d, want synced tail %d", got, fetcher.tail)
	}
	if record.Progress.Done != int64(fetcher.tail) {
		t.Errorf("durable progress = %d, want %d", record.Progress.Done, fetcher.tail)
	}
	if record.Lease.Held(time.Now()) {
		t.Fatal("paused runner retained lease")
	}
	if _, err = store.SetIntent(id, job.WantRun, "test-caller"); err != nil {
		t.Fatal(err)
	}
	if err = r.Run(ctx, id); err != nil {
		t.Fatal(err)
	}
	if start := <-fetcher.starts; start != int64(fetcher.tail) {
		t.Errorf("resume fetched at %d, want %d without refetching synced tail", start, fetcher.tail)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("resumed payload differs")
	}
}
