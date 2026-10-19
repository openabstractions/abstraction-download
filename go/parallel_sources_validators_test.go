package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

func TestWeakValidatorUpgradeRequiresSameSourceAndVersion(t *testing.T) {
	const date = "Wed, 21 Oct 2015 07:28:00 GMT"
	weak := Validators{LastModified: date, Weak: true}
	strong := Validators{LastModified: date}
	for _, c := range []struct {
		name         string
		held, served Validators
		changed      bool
	}{
		{"same version", weak, strong, false},
		{"different date", weak, Validators{LastModified: "Wed, 21 Oct 2015 07:29:00 GMT"}, true},
		{"different validator", weak, Validators{ETag: `"new"`}, true},
		{"legacy unknown timestamp", Validators{Weak: true}, strong, true},
		{"same weak metadata", weak, weak, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := &progress{validators: c.held, owner: "source-a"}
			err := p.served("source-a", c.served)
			if errors.Is(err, ErrVersionChanged) != c.changed {
				t.Fatalf("served=%v changed=%v", err, c.changed)
			}
			got := p.version()
			got.Source = ""
			want := c.served
			if c.changed {
				want = c.held
			}
			if got != want {
				t.Fatalf("held=%+v want=%+v", got, want)
			}
		})
	}
	p := &progress{validators: weak, owner: "source-a"}
	if err := p.served("source-b", strong); err != nil {
		t.Fatal(err)
	}
	if p.version().Weak != true || p.version().Source != "source-a" {
		t.Fatal("another source upgraded owner's weak proof")
	}
	if err := p.served("source-b", Validators{LastModified: "Wed, 21 Oct 2015 07:29:00 GMT"}); !errors.Is(err, ErrVersionChanged) {
		t.Fatalf("mirror changed version: %v", err)
	}
	p = &progress{validators: Validators{ETag: `"owner"`}, owner: "source-a", mirrors: map[string]Validators{"source-b": weak}}
	if err := p.served("source-b", strong); err != nil || p.mirrors["source-b"] != strong || p.validators.ETag != `"owner"` {
		t.Fatalf("mirror upgrade: %+v %v", p, err)
	}
	if got := weak.IfRange(); got != "" {
		t.Fatalf("weak If-Range=%q", got)
	}
	if got := (Validators{Weak: true}).IfRange(); got != "" {
		t.Fatalf("legacy weak If-Range=%q", got)
	}
}

// TestAMirrorWithItsOwnValidatorsCompletesTheRun: two sources serve the same
// bytes under different Last-Modified values. The first answers the first
// range and is "not now" for the rest, so they fail over to the mirror. The
// mirror's validators name the mirror, and the run must complete; the digest
// guards its bytes [DL-R12, DL-V2].
func TestAMirrorWithItsOwnValidatorsCompletesTheRun(t *testing.T) {
	body, digest := payload(t, minParallel+7)
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var from int
		if rg := r.Header.Get("Range"); rg != "" {
			fmt.Sscanf(rg, "bytes=%d-", &from)
		}
		if from > 0 {
			http.Error(w, "not now", http.StatusServiceUnavailable)
			return
		}
		http.ServeContent(w, r, "blob.bin", time.Unix(500, 0), bytes.NewReader(body))
	}))
	t.Cleanup(primary.Close)
	mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "blob.bin", time.Unix(1000, 0), bytes.NewReader(body))
	}))
	t.Cleanup(mirror.Close)

	r, store, root := newRunner(t)
	r.Connections = 4
	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources: []Source{
			{Scheme: "https", Locator: primary.URL + "/blob.bin", Priority: 0},
			{Scheme: "https", Locator: mirror.URL + "/blob.bin", Priority: 1},
		},
		Sink: Sink{Final: filepath.Join(root, "final.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("a mirror of the same bytes was taken for another version: %v", err)
	}
}

// TestASourceReplacedMidRunDeliversTheNewVersion: one source serves the first
// range under ETag v1 and every later range under v2 with other bytes. The run
// must restart and deliver exactly the new body.
func TestASourceReplacedMidRunDeliversTheNewVersion(t *testing.T) {
	a := []byte(strings.Repeat("A", minParallel+7))
	b := []byte(strings.Repeat("B", minParallel+7))
	var mu sync.Mutex
	replaced := make(chan struct{})
	var once sync.Once
	current := func() ([]byte, string) {
		select {
		case <-replaced:
			return b, `"v2"`
		default:
			return a, `"v1"`
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rg := r.Header.Get("Range")
		from, to := 0, len(a)-1
		if rg != "" {
			fmt.Sscanf(rg, "bytes=%d-%d", &from, &to)
		}
		if from > 0 {
			<-replaced
		}
		mu.Lock()
		body, etag := current()
		mu.Unlock()
		w.Header().Set("ETag", etag)
		w.Header().Set("Accept-Ranges", "bytes")
		if ir := r.Header.Get("If-Range"); rg == "" || (ir != "" && ir != etag) {
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			w.Write(body)
			return
		}
		if to >= len(body) {
			to = len(body) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(body)))
		w.Header().Set("Content-Length", fmt.Sprint(to-from+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(body[from : to+1])
		if from == 0 && rg != "bytes=0-0" {
			once.Do(func() { close(replaced) })
		}
	}))
	t.Cleanup(srv.Close)

	r, store, root := newRunner(t)
	r.Connections = 4
	id := submit(t, store, root, "", int64(len(b)), Source{Scheme: "https", Locator: srv.URL})
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, b) {
		t.Fatalf("delivered %d bytes with %d of version A", len(got), bytes.Count(got, []byte("A")))
	}
}

// TestARangeAnsweredWholeToIfRangeIsAVersionChange: a 200 to an If-Range is
// the whole new artifact, and none of it may land in the range.
func TestARangeAnsweredWholeToIfRangeIsAVersionChange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v2"`)
		w.Write(make([]byte, 8192))
	}))
	t.Cleanup(srv.Close)
	f, err := os.CreateTemp(t.TempDir(), "chunk")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	err = HTTP{}.FetchRange(context.Background(), RangeRequest{
		Source:     Source{Scheme: "https", Locator: srv.URL},
		Range:      job.Range{Start: 4096, End: 8192},
		Out:        f,
		Validators: Validators{ETag: `"v1"`},
	})
	if !errors.Is(err, ErrVersionChanged) {
		t.Fatalf("FetchRange = %v; want ErrVersionChanged", err)
	}
	if st, err := f.Stat(); err != nil || st.Size() != 0 {
		t.Fatalf("the other version was written: %v, %v", st, err)
	}
}

// TestAFreshWeakParallelRunCheckpointsWeak: a fresh run whose source names
// only a weak Last-Modified checkpoints Weak, so its resume restarts [DL-V4].
func TestAFreshWeakParallelRunCheckpointsWeak(t *testing.T) {
	const T = "Wed, 21 Oct 2015 07:28:00 GMT"
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
	var smu sync.Mutex
	cut := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		smu.Lock()
		b, cut := b, cut
		smu.Unlock()
		w.Header().Set("Last-Modified", T)
		w.Header().Set("Date", T)
		w.Header().Set("Accept-Ranges", "bytes")
		from, to := 0, len(b)-1
		if rg := r.Header.Get("Range"); rg != "" {
			fmt.Sscanf(rg, "bytes=%d-%d", &from, &to)
			if to >= len(b) {
				to = len(b) - 1
			}
			if from > 0 && cut {
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
	if len(cp.Verified) == 0 || !cp.Validators.Weak {
		t.Fatalf("checkpoint = %+v; want proven ranges recorded Weak", cp)
	}

	// The source replaces the artifact under the same weak date. The resume
	// restarts from byte zero and delivers only the current body.
	c := []byte(strings.Repeat("C", len(b)))
	smu.Lock()
	b, cut = c, false
	smu.Unlock()
	partial, err := os.ReadFile(partialOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if got := resumeFrom(t, srv.URL, len(c), cp, partial); !bytes.Equal(got, c) {
		t.Fatalf("delivered %d bytes with %d of the old body; want only the current one", len(got), bytes.Count(got, []byte("B")))
	}
}

// switchable serves through whichever handler was set last.
type switchable struct {
	mu sync.Mutex
	h  http.Handler
}

func (s *switchable) set(h http.Handler) { s.mu.Lock(); s.h = h; s.mu.Unlock() }

func (s *switchable) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	h := s.h
	s.mu.Unlock()
	h.ServeHTTP(w, r)
}

// TestAResumeSendsAValidatorOnlyToTheSourceThatNamedIt: a fresh run proves a
// range from the primary under ETag "a" and is interrupted. The resume finds
// the primary "not now" past byte zero and a mirror serving the same bytes
// under ETag "b". The checkpoint names the primary as the source of "a", so
// the mirror is asked without If-Range and the proven range is kept. Sending
// the mirror "a" made it answer 200, and the run discarded every proven range
// [DL-V2].
func TestAResumeSendsAValidatorOnlyToTheSourceThatNamedIt(t *testing.T) {
	body := []byte(strings.Repeat("M", minParallel+7))
	primary := &etagSource{body: body, etag: `"a"`, cut: true, ifRange: map[int]string{}}
	front := &switchable{h: primary}
	psrv := httptest.NewServer(front)
	t.Cleanup(psrv.Close)
	cp, partial := interruptedFresh(t, primary, psrv.URL)
	// validators.source is the source's locator as the job spells it.
	if cp.Validators.Source != psrv.URL || cp.Validators.ETag != `"a"` {
		t.Fatalf("checkpoint validators = %+v; want %q named as the source of \"a\"", cp.Validators, psrv.URL)
	}

	var zmu sync.Mutex
	var askedZero bool
	front.set(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rg := r.Header.Get("Range"); strings.HasPrefix(rg, "bytes=0-") && rg != "bytes=0-0" {
			zmu.Lock()
			askedZero = true
			zmu.Unlock()
		}
		http.Error(w, "not now", http.StatusServiceUnavailable)
	}))
	mirror := &etagSource{body: body, etag: `"b"`, ifRange: map[int]string{}}
	msrv := httptest.NewServer(mirror)
	t.Cleanup(msrv.Close)

	r, store, root := newRunner(t)
	r.Connections = 4
	id := submit(t, store, root, "", int64(len(body)),
		Source{Scheme: "https", Locator: psrv.URL, Priority: 0},
		Source{Scheme: "https", Locator: msrv.URL, Priority: 1})
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
	if !bytes.Equal(got, body) {
		t.Fatalf("delivered %d bytes; want the %d of the artifact", len(got), len(body))
	}
	mirror.mu.Lock()
	defer mirror.mu.Unlock()
	for from, ir := range mirror.ifRange {
		if ir == `"a"` {
			t.Fatalf("the mirror was asked for bytes %d- under the primary's ETag", from)
		}
		if from == 0 {
			t.Fatal("the resume discarded the proven range and asked the mirror for byte zero")
		}
	}
	zmu.Lock()
	defer zmu.Unlock()
	if askedZero {
		t.Fatal("the resume discarded the proven range and asked the primary for byte zero")
	}
}

// rangeOf reads the first byte position a request asks for, and whether it is
// the bytes=0-0 probe a parallel plan opens with.
func rangeOf(r *http.Request, size int) (from, to int, probe bool) {
	rg := r.Header.Get("Range")
	from, to = 0, size-1
	if rg != "" {
		fmt.Sscanf(rg, "bytes=%d-%d", &from, &to)
	}
	if to >= size {
		to = size - 1
	}
	return from, to, rg == "bytes=0-0"
}

// serveVersion answers r from body under etag. A request whose If-Range names
// another version, or one with no Range, gets the whole body and a 200, as RFC
// 9110 section 13.1.5 says.
func serveVersion(w http.ResponseWriter, r *http.Request, body []byte, etag string) {
	w.Header().Set("ETag", etag)
	w.Header().Set("Accept-Ranges", "bytes")
	if ir := r.Header.Get("If-Range"); r.Header.Get("Range") == "" || (ir != "" && ir != etag) {
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.Write(body)
		return
	}
	from, to, _ := rangeOf(r, len(body))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(body)))
	w.Header().Set("Content-Length", fmt.Sprint(to-from+1))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(body[from : to+1])
}

// awaitClose holds a request until ch closes, for at most ten seconds.
func awaitClose(ch <-chan struct{}) {
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
	}
}

// closed reports whether ch has been closed.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestAnOwnersVersionChangeRestartsTheRunFromZero: two sources, no
// digest. The primary serves the first range under ETag "a1" and then replaces
// the artifact under "a2"; the mirror already serves the new body under its
// own ETag "m". The primary's 200 to If-Range "a1" is a version change.
// Failing that range over to the mirror landed the new body beside the old
// first range, and nothing caught the splice. The run restarts from byte zero
// instead and delivers exactly one version [DL-V2]. By the time the restart
// asks for byte zero, the record no longer claims the old version's ranges,
// which a successor taking over would otherwise keep.
func TestAnOwnersVersionChangeRestartsTheRunFromZero(t *testing.T) {
	a := []byte(strings.Repeat("A", minParallel+7))
	b := []byte(strings.Repeat("B", minParallel+7))
	r, store, root := newRunner(t)
	r.Connections = 4
	var id string
	var smu sync.Mutex
	var stale job.Ranges
	replaced := make(chan struct{})
	var once sync.Once
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, _, probe := rangeOf(r, len(a))
		if from > 0 {
			awaitClose(replaced)
		}
		if closed(replaced) {
			// Only the restart asks for byte zero after the replacement, and
			// it has proved nothing at zero yet.
			if from == 0 && !probe {
				if rec, err := store.Load(id); err == nil {
					if cp, err := CheckpointOf(rec); err == nil {
						smu.Lock()
						for _, x := range cp.Verified {
							if x.Start == 0 {
								stale = append(stale, x)
							}
						}
						smu.Unlock()
					}
				}
			}
			serveVersion(w, r, b, `"a2"`)
			return
		}
		serveVersion(w, r, a, `"a1"`)
		if from == 0 && !probe {
			once.Do(func() { close(replaced) })
		}
	}))
	t.Cleanup(primary.Close)
	mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveVersion(w, r, b, `"m"`)
	}))
	t.Cleanup(mirror.Close)

	id = submit(t, store, root, "", int64(len(b)),
		Source{Scheme: "https", Locator: primary.URL, Priority: 0},
		Source{Scheme: "https", Locator: mirror.URL, Priority: 1})
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, b) {
		t.Fatalf("delivered %d bytes: %d of version A and %d of version B; want only B",
			len(got), bytes.Count(got, []byte("A")), bytes.Count(got, []byte("B")))
	}
	smu.Lock()
	defer smu.Unlock()
	if len(stale) > 0 {
		t.Fatalf("the restart began with the old version's ranges %v still in the checkpoint", stale)
	}
}

// TestAMirrorReplacedMidRunAfterTheOwnerWentDownRestarts: two sources, no
// digest. The primary serves the first range under ETag "p" and then refuses
// everything. The mirror serves its first range under "m1" and every later one
// from another body under "m2". The run keeps the first validators each source
// names, so the mirror's second version is a version change; the run restarts
// from byte zero and delivers exactly one version [DL-V2].
func TestAMirrorReplacedMidRunAfterTheOwnerWentDownRestarts(t *testing.T) {
	// Three ranges: the first from the primary, two from the mirror.
	x := []byte(strings.Repeat("X", minParallel+minRangeSize+7))
	y := []byte(strings.Repeat("Y", minParallel+minRangeSize+7))
	down := make(chan struct{})
	var once sync.Once
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, _, probe := rangeOf(r, len(x))
		if from > 0 {
			awaitClose(down)
		}
		if closed(down) {
			http.Error(w, "not now", http.StatusServiceUnavailable)
			return
		}
		serveVersion(w, r, x, `"p"`)
		if from == 0 && !probe {
			once.Do(func() { close(down) })
		}
	}))
	t.Cleanup(primary.Close)
	var mmu sync.Mutex
	served := 0
	mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, probe := rangeOf(r, len(x)); probe {
			serveVersion(w, r, x, `"m1"`)
			return
		}
		mmu.Lock()
		served++
		first := served == 1
		mmu.Unlock()
		if first {
			serveVersion(w, r, x, `"m1"`)
			return
		}
		serveVersion(w, r, y, `"m2"`)
	}))
	t.Cleanup(mirror.Close)

	r, store, root := newRunner(t)
	r.Connections = 4
	id := submit(t, store, root, "", int64(len(x)),
		Source{Scheme: "https", Locator: primary.URL, Priority: 0},
		Source{Scheme: "https", Locator: mirror.URL, Priority: 1})
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, y) {
		t.Fatalf("delivered %d bytes: %d of version X and %d of version Y; want only Y",
			len(got), bytes.Count(got, []byte("X")), bytes.Count(got, []byte("Y")))
	}
}
