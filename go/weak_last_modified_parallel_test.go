package download

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The first actual range reports its weak metadata before the other ranges
// ask the same HTTP source. HTTP still owns the request, response and bytes.
type weakUpgradeHTTP struct {
	HTTP
	first            atomic.Bool
	seen             chan struct{}
	mu               sync.Mutex
	observationError error
	strong           bool
}

func (h *weakUpgradeHTTP) FetchRange(ctx context.Context, req RangeRequest) error {
	observed := req.Observed
	req.Observed = func(v Validators) error {
		err := observed(v)
		h.mu.Lock()
		defer h.mu.Unlock()
		if err != nil {
			h.observationError = err
		}
		if !v.Weak && v.LastModified != "" {
			h.strong = true
		}
		return err
	}
	if req.Range.Start == 0 && h.first.CompareAndSwap(false, true) {
		req.Headers = map[string]string{"X-Fixture-Weak": "true"}
		observed := req.Observed
		req.Observed = func(v Validators) error {
			err := observed(v)
			close(h.seen)
			return err
		}
	} else {
		select {
		case <-h.seen:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return h.HTTP.FetchRange(ctx, req)
}

func TestAWeakLastModifiedBecomesStrongWithoutRestart(t *testing.T) {
	const modified = "Wed, 21 Oct 2015 07:28:00 GMT"
	body := []byte(strings.Repeat("U", minParallel+7))
	var probes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, to := 0, len(body)-1
		if r.Header.Get("Range") == "bytes=0-0" {
			probes.Add(1)
		}
		if n, _ := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &from, &to); n == 0 {
			http.Error(w, "range required", 400)
			return
		}
		if to >= len(body) {
			to = len(body) - 1
		}
		w.Header().Set("Last-Modified", modified)
		w.Header().Set("Date", "Wed, 21 Oct 2015 07:28:01 GMT")
		if r.Header.Get("X-Fixture-Weak") != "" {
			w.Header().Set("Date", modified)
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(body)))
		w.Header().Set("Content-Length", fmt.Sprint(to-from+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[from : to+1])
	}))
	t.Cleanup(srv.Close)
	r, store, root := newRunner(t)
	r.Connections = 4
	fetcher := &weakUpgradeHTTP{seen: make(chan struct{})}
	r.Fetchers = NewFetchers(fetcher)
	id := submit(t, store, root, "", int64(len(body)), Source{Scheme: "https", Locator: srv.URL})
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil || string(got) != string(body) {
		t.Fatalf("delivered bytes: %v", err)
	}
	if n := probes.Load(); n != 1 {
		t.Fatalf("byte-zero probes=%d; same-version metadata caused a restart", n)
	}
	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()
	if fetcher.observationError != nil || !fetcher.strong {
		t.Fatalf("same-source upgrade strong=%v error=%v", fetcher.strong, fetcher.observationError)
	}
}

// TestAWeakLastModifiedParallelResumeNeverSplices is the DL-V4 falsifier on
// the parallel path. A prefix proven under version A's weak Last-Modified is
// on disk; the source now serves version B in ranges. The resume must deliver
// exactly B: a weak checkpoint proves no bytes to the range planner either.
func TestAWeakLastModifiedParallelResumeNeverSplices(t *testing.T) {
	const T = "Wed, 21 Oct 2015 07:28:00 GMT"
	a := []byte(strings.Repeat("A", minParallel+7))
	b := []byte(strings.Repeat("B", minParallel+7))
	var mu sync.Mutex
	var bare int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", T)
		w.Header().Set("Date", T)
		w.Header().Set("Accept-Ranges", "bytes")
		from, to := 0, len(b)-1
		if rg := r.Header.Get("Range"); rg != "" {
			if r.Header.Get("If-Range") == "" && rg != "bytes=0-0" {
				mu.Lock()
				bare++
				mu.Unlock()
			}
			if n, _ := fmt.Sscanf(rg, "bytes=%d-%d", &from, &to); n == 0 {
				http.Error(w, "bad range", http.StatusBadRequest)
				return
			}
			if to >= len(b) {
				to = len(b) - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(b)))
			w.Header().Set("Content-Length", fmt.Sprint(to-from+1))
			w.WriteHeader(http.StatusPartialContent)
			if r.Method != http.MethodHead {
				w.Write(b[from : to+1])
			}
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
		if r.Method != http.MethodHead {
			w.Write(b)
		}
	}))
	t.Cleanup(srv.Close)

	r, store, root := newRunner(t)
	r.Connections = 4
	id := submit(t, store, root, "", int64(len(b)), Source{Scheme: "https", Locator: srv.URL})
	stageResume(t, store, id, a[:minParallel/2+3], StrongValidators(http.Header{"Last-Modified": {T}, "Date": {T}}))

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(b) {
		t.Fatalf("delivered %d bytes with %d of version A: version A was spliced onto version B", len(got), strings.Count(string(got), "A"))
	}
	mu.Lock()
	defer mu.Unlock()
	if bare == 0 {
		t.Fatal("no range fetch beyond the capability probe: the parallel path was not taken")
	}
}

// TestAWeakRestartParallelProgressSurvivesInterruption is the resume of a
// resume. A weak checkpoint restarts the parallel run from byte zero; the
// ranges that restart proves were taken under its own validators, so an
// interruption after the first range must leave that range resumable. Carrying
// the predecessor's Weak flag onto them discarded it at every interruption.
func TestAWeakRestartParallelProgressSurvivesInterruption(t *testing.T) {
	const T = "Wed, 21 Oct 2015 07:28:00 GMT"
	const Strong = "Wed, 21 Oct 2015 07:00:00 GMT"
	b := []byte(strings.Repeat("B", minParallel+7))
	r, store, root := newRunner(t)
	r.Connections = 4
	var id string
	firstDone := make(chan struct{})
	var once sync.Once
	proven := func() bool {
		rec, err := store.Load(id)
		if err != nil {
			return false
		}
		cp, err := CheckpointOf(rec)
		return err == nil && len(cp.Verified) > 0
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", Strong)
		w.Header().Set("Date", T)
		w.Header().Set("Accept-Ranges", "bytes")
		from, to := 0, len(b)-1
		if rg := r.Header.Get("Range"); rg != "" {
			fmt.Sscanf(rg, "bytes=%d-%d", &from, &to)
			if to >= len(b) {
				to = len(b) - 1
			}
			if from > 0 {
				<-firstDone
				for i := 0; i < 500 && !proven(); i++ {
					time.Sleep(10 * time.Millisecond)
				}
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(b)))
			w.Header().Set("Content-Length", fmt.Sprint(to-from+1))
			w.WriteHeader(http.StatusPartialContent)
			if r.Method != http.MethodHead {
				w.Write(b[from : to+1])
				if to+1 >= minRangeSize {
					once.Do(func() { close(firstDone) })
				}
			}
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
		if r.Method != http.MethodHead {
			w.Write(b)
		}
	}))
	t.Cleanup(srv.Close)

	id = submit(t, store, root, "", int64(len(b)), Source{Scheme: "https", Locator: srv.URL})
	stageResume(t, store, id, b[:minParallel/2+3], StrongValidators(http.Header{"Last-Modified": {T}, "Date": {T}}))

	err := r.Run(context.Background(), id)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v; the second range was meant to fail", err)
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
		t.Fatalf("the restarted run proved no range before the interruption: %+v", cp)
	}
	if cp.Validators.LastModified != Strong || cp.Validators.Weak {
		t.Fatalf("restarted checkpoint validators = %+v; want the strong Last-Modified %q", cp.Validators, Strong)
	}
	got, err := resumable(rec, partialOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatalf("resumable = [] over verified %v with validators %+v: the restart's progress was discarded", cp.Verified, cp.Validators)
	}
}
