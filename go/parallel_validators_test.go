package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// etagSource serves one body under a strong ETag in ranges. A range whose
// If-Range does not match is answered with the whole current body and a 200,
// as RFC 9110 section 13.1.5 says. With cut set, every range past the first
// waits for the first to be proven and is then refused, which interrupts a
// fresh parallel run with one range on record.
type etagSource struct {
	mu      sync.Mutex
	body    []byte
	etag    string
	cut     bool
	proven  func() bool
	ifRange map[int]string // range start -> If-Range sent
	first   chan struct{}
	once    sync.Once
}

func (s *etagSource) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	body, etag, cut := s.body, s.etag, s.cut
	s.mu.Unlock()
	w.Header().Set("ETag", etag)
	w.Header().Set("Accept-Ranges", "bytes")
	rg := r.Header.Get("Range")
	from, to := 0, len(body)-1
	if rg != "" {
		fmt.Sscanf(rg, "bytes=%d-%d", &from, &to)
	}
	if rg != "" && rg != "bytes=0-0" {
		s.mu.Lock()
		s.ifRange[from] = r.Header.Get("If-Range")
		s.mu.Unlock()
	}
	if ir := r.Header.Get("If-Range"); rg == "" || (ir != "" && ir != etag) {
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.Write(body)
		return
	}
	if to >= len(body) {
		to = len(body) - 1
	}
	if cut && from > 0 && rg != "bytes=0-0" {
		<-s.first
		for i := 0; i < 500 && !s.proven(); i++ {
			time.Sleep(10 * time.Millisecond)
		}
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(body)))
	w.Header().Set("Content-Length", fmt.Sprint(to-from+1))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(body[from : to+1])
	if cut && from == 0 && to+1 >= minRangeSize {
		s.once.Do(func() { close(s.first) })
	}
}

// interruptedFresh runs a fresh parallel download against s until the first
// range is proven and the rest are refused, and returns its checkpoint and the
// bytes on disk.
func interruptedFresh(t *testing.T, s *etagSource, url string) (Checkpoint, []byte) {
	t.Helper()
	r, store, root := newRunner(t)
	r.Connections = 4
	id := submit(t, store, root, "", int64(len(s.body)), Source{Scheme: "https", Locator: url})
	s.first = make(chan struct{})
	s.proven = func() bool {
		rec, err := store.Load(id)
		if err != nil {
			return false
		}
		cp, err := CheckpointOf(rec)
		return err == nil && len(cp.Verified) > 0
	}
	if err := r.Run(context.Background(), id); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v; the ranges past the first were meant to fail", err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.Verified) == 0 {
		t.Fatalf("the fresh run proved no range before the interruption: %+v", cp)
	}
	partial, err := os.ReadFile(partialOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	return cp, partial
}

// resumeFrom stages cp and partial on a new job and runs it to the end.
func resumeFrom(t *testing.T, url string, size int, cp Checkpoint, partial []byte) []byte {
	t.Helper()
	r, store, root := newRunner(t)
	r.Connections = 4
	id := submit(t, store, root, "", int64(size), Source{Scheme: "https", Locator: url})
	if err := os.WriteFile(partialOf(t, store, id), partial, 0o644); err != nil {
		t.Fatal(err)
	}
	stageAbandonedProgress(t, store, id, "dead-owner", func(rr *job.Record) error {
		rr.Progress.Done = cp.Verified.Total()
		return setCheckpoint(rr, cp)
	})
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestAFreshParallelRunRecordsItsETag is the DL-V1/DL-V2 falsifier on the
// parallel path. A fresh run has no predecessor's validators; it used to
// checkpoint its ranges with none, and the resume then asked for the rest with
// a bare Range that a replaced artifact answers from the new file.
func TestAFreshParallelRunRecordsItsETag(t *testing.T) {
	a := []byte(strings.Repeat("A", minParallel+7))
	s := &etagSource{body: a, etag: `"v1"`, cut: true, ifRange: map[int]string{}}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)

	cp, partial := interruptedFresh(t, s, srv.URL)
	if cp.Validators.ETag != `"v1"` {
		t.Fatalf("checkpoint validators = %+v; the fresh run's ranges carry no version", cp.Validators)
	}

	s.mu.Lock()
	s.cut = false
	s.ifRange = map[int]string{}
	s.mu.Unlock()
	got := resumeFrom(t, srv.URL, len(a), cp, partial)
	if !bytes.Equal(got, a) {
		t.Fatal("the resumed download is not the artifact")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ifRange) == 0 {
		t.Fatal("the resume fetched no range")
	}
	for start, ir := range s.ifRange {
		if ir != `"v1"` {
			t.Fatalf("resumed range at %d sent If-Range %q; want the recorded ETag", start, ir)
		}
	}
}

// TestAChangedETagRestartsTheParallelResume: the artifact is replaced between
// the interruption and the resume. The resume must deliver exactly the new
// version, never the old proven range spliced onto new ranges.
func TestAChangedETagRestartsTheParallelResume(t *testing.T) {
	a := []byte(strings.Repeat("A", minParallel+7))
	b := []byte(strings.Repeat("B", minParallel+7))
	s := &etagSource{body: a, etag: `"v1"`, cut: true, ifRange: map[int]string{}}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)

	cp, partial := interruptedFresh(t, s, srv.URL)

	s.mu.Lock()
	s.body, s.etag, s.cut = b, `"v2"`, false
	s.mu.Unlock()
	got := resumeFrom(t, srv.URL, len(b), cp, partial)
	if !bytes.Equal(got, b) {
		t.Fatalf("delivered %d bytes with %d of version A: the old version was spliced onto the new", len(got), bytes.Count(got, []byte("A")))
	}
}
