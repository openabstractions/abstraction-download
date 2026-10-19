package download

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// A small file submitted after a large one finishes while the large one is
// still transferring: Adopt runs up to Workers jobs at once, each under its own
// claim, and a job submitted during the sweep starts in a free slot.
func TestASmallJobFinishesWhileALargeOneRuns(t *testing.T) {
	r, store, root := newRunner(t)
	r.DeliverEach = true
	large, largeDigest := payload(t, 1<<20)
	small, smallDigest := payload(t, 4<<10)
	release := make(chan struct{})
	var largeStarted sync.Once
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/large":
			largeStarted.Do(func() { close(started) })
			w.Header().Set("Content-Length", strconv.Itoa(len(large)))
			w.Write(large[:1024])
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-req.Context().Done():
				return
			}
			w.Write(large[1024:])
		case "/small":
			w.Header().Set("Content-Length", strconv.Itoa(len(small)))
			w.Write(small)
		}
	}))
	t.Cleanup(srv.Close)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	largeID, err := Submit(store, Spec{
		Artifact: Artifact{Digest: largeDigest, Size: int64(len(large))},
		Sources:  []Source{{Scheme: "https", Locator: srv.URL + "/large"}},
		Sink:     Sink{Final: filepath.Join(root, "large.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := r.Adopt(context.Background())
		done <- result{n, err}
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the large job never started")
	}
	// Submitted while the large job holds a worker.
	smallID, err := Submit(store, Spec{
		Artifact: Artifact{Digest: smallDigest, Size: int64(len(small))},
		Sources:  []Source{{Scheme: "https", Locator: srv.URL + "/small"}},
		Sink:     Sink{Final: filepath.Join(root, "small.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		rec, err := store.Load(smallID)
		if err != nil {
			t.Fatal(err)
		}
		if rec.State == job.StateComplete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the small job is %s while the large one runs", rec.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if rec, err := store.Load(largeID); err != nil || rec.State == job.StateComplete || rec.State == job.StateTransferred {
		t.Fatalf("the large job finished before it was released: %v %v", rec.State, err)
	}
	close(release)
	select {
	case got := <-done:
		if got.err != nil || got.n != 2 {
			t.Fatalf("Adopt = %d, %v; want both jobs run", got.n, got.err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Adopt did not return")
	}
	if rec, err := store.Load(largeID); err != nil || rec.State != job.StateComplete {
		t.Fatalf("the large job: %v %v", rec, err)
	}
}

// Adopt never runs more than Workers jobs at once, and each job once.
func TestAdoptRunsAtMostWorkersJobsAtOnce(t *testing.T) {
	r, store, root := newRunner(t)
	r.Workers = 2
	var now, peak, hits atomic.Int32
	body, digest := payload(t, 4<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		hits.Add(1)
		cur := now.Add(1)
		for {
			p := peak.Load()
			if cur <= p || peak.CompareAndSwap(p, cur) {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
		now.Add(-1)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	for i := 0; i < 5; i++ {
		if _, err := Submit(store, Spec{
			Artifact: Artifact{Digest: digest, Size: int64(len(body))},
			Sources:  []Source{{Scheme: "https", Locator: srv.URL + "/f" + strconv.Itoa(i)}},
			Sink:     Sink{Final: filepath.Join(root, "f"+strconv.Itoa(i)+".bin")},
		}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := r.Adopt(context.Background())
	if err != nil || n != 5 {
		t.Fatalf("Adopt = %d, %v; want 5", n, err)
	}
	if p := peak.Load(); p != 2 {
		t.Fatalf("peak concurrent jobs %d, want 2", p)
	}
}
