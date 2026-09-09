package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

func TestGapsAreWhatIsLeftToFetch(t *testing.T) {
	got := gaps(50, job.Ranges{{Start: 20, End: 30}, {Start: 0, End: 10}})
	want := job.Ranges{{Start: 10, End: 20}, {Start: 30, End: 50}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("gaps = %v, want %v", got, want)
	}
	if n := gaps(50, job.Ranges{{Start: 0, End: 50}}); len(n) != 0 {
		t.Fatalf("a fully proven artifact still has gaps: %v", n)
	}
	if n := gaps(50, nil); fmt.Sprint(n) != fmt.Sprint(job.Ranges{{Start: 0, End: 50}}) {
		t.Fatalf("nothing proven should be one gap, got %v", n)
	}
}

// Ranges of different sizes have to nest, or two owners that chose differently
// disagree about where a boundary is and every resumed range straddles one.
func TestGridsNestRatherThanCross(t *testing.T) {
	small := gridPlan(int64(maxRanges) * minRangeSize)
	large := gridPlan(int64(maxRanges) * minRangeSize * 4)
	fine := map[int64]bool{}
	for _, c := range small.cuts {
		fine[c] = true
	}
	if len(large.cuts) == 0 || large.cuts[0] == small.cuts[0] {
		t.Fatalf("the two plans did not pick different range sizes")
	}
	for _, c := range large.cuts {
		if c < small.size && !fine[c] {
			t.Fatalf("cut %d of the coarse plan is not a cut of the fine one", c)
		}
	}
}

func TestRangesFollowThePublishedBoundaries(t *testing.T) {
	src := Source{Attrs: map[string]string{BoundariesAttr: "10,25,60"}}
	p, ok := publishedPlan(src, 100)
	if !ok {
		t.Fatal("a source that published boundaries was read as publishing none")
	}
	got := p.rangesOf(job.Range{Start: 0, End: 100})
	want := job.Ranges{{Start: 0, End: 10}, {Start: 10, End: 25}, {Start: 25, End: 60}, {Start: 60, End: 100}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ranges = %v, want %v", got, want)
	}
	// A gap that starts inside a range is cut at the next boundary, never across
	// it.
	if got := p.rangesOf(job.Range{Start: 15, End: 70}); fmt.Sprint(got) !=
		fmt.Sprint(job.Ranges{{Start: 15, End: 25}, {Start: 25, End: 60}, {Start: 60, End: 70}}) {
		t.Fatalf("a mid-range gap was not cut on the boundaries: %v", got)
	}
	for _, bad := range []string{"10,5", "0,10", "10,200", "10,,20", "ten"} {
		if _, ok := publishedPlan(Source{Attrs: map[string]string{BoundariesAttr: bad}}, 100); ok {
			t.Fatalf("accepted boundaries %q", bad)
		}
	}
}

// A source attribute this layer understands must not also be sent to the server
// as a header.
func TestBoundariesAreNotAHeader(t *testing.T) {
	h, err := headersFor(Source{Attrs: map[string]string{BoundariesAttr: "10,20"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, sent := h[BoundariesAttr]; sent {
		t.Fatalf("boundaries went out as a header: %v", h)
	}
}

// A predecessor in another language resumes from the prefix and truncates,
// which destroys exactly the bytes the non-prefix ranges named. A successor that
// believed the record over the file would leave holes in a delivered artifact.
func TestRangesPastTheEndOfTheFileAreNotBelieved(t *testing.T) {
	partial := filepath.Join(t.TempDir(), "part")
	if err := os.WriteFile(partial, make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	rec := &job.Record{ID: "j", Kind: Kind}
	if err := setCheckpoint(rec, Checkpoint{
		Verified: job.Ranges{{Start: 0, End: 50}, {Start: 80, End: 120}, {Start: 400, End: 500}},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := resumable(rec, partial)
	if err != nil {
		t.Fatal(err)
	}
	want := job.Ranges{{Start: 0, End: 50}, {Start: 80, End: 100}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("resumable = %v, want %v", got, want)
	}
	if _, err := resumable(rec, filepath.Join(t.TempDir(), "gone")); err != nil {
		t.Fatal(err)
	}
}

// parallelServer honours ranges and records every range it was asked for.
type parallelServer struct {
	*httptest.Server
	mu    sync.Mutex
	asked []string
}

func newParallelServer(t *testing.T, body []byte) *parallelServer {
	t.Helper()
	s := &parallelServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.asked = append(s.asked, r.Header.Get("Range"))
		s.mu.Unlock()
		http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), strings.NewReader(string(body)))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *parallelServer) ranges() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.asked...)
}

func parallelJob(t *testing.T, body []byte, digest string, loc string) (*Runner, job.Store, string) {
	t.Helper()
	r, store, root := newRunner(t)
	r.Connections = 4
	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources:  []Source{{Scheme: "https", Locator: loc}},
		Sink:     Sink{Final: filepath.Join(root, "final.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	return r, store, id
}

func TestParallelProvesTheWholeArtifact(t *testing.T) {
	body, digest := payload(t, minParallel+7)
	srv := newParallelServer(t, body)
	r, store, id := parallelJob(t, body, digest, srv.URL+"/blob.bin")

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("run: %v", err)
	}

	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(got)
	if "sha256:"+hex.EncodeToString(sum[:]) != digest {
		t.Fatal("the delivered file is not the artifact")
	}
	asked := srv.ranges()
	// One probe plus one request per range, and the artifact is bigger than one
	// range, so a single stream would not have produced this.
	if len(asked) < 3 || asked[0] != "bytes=0-0" {
		t.Fatalf("expected a probe and several ranges, got %v", asked)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != job.StateTransferred {
		t.Fatalf("state is %s", rec.State)
	}
}

// The point of a range set: a successor fetches the holes, not the tail.
func TestParallelResumesFromARangeSetNotAPrefix(t *testing.T) {
	body, digest := payload(t, 3*minRangeSize)
	srv := newParallelServer(t, body)
	r, store, id := parallelJob(t, body, digest, srv.URL+"/blob.bin")

	// A predecessor proved the first and third ranges and vanished. The middle
	// one is a hole in a file that is already full length.
	partial := partialOf(t, store, id)
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		t.Fatal(err)
	}
	staged := make([]byte, len(body))
	copy(staged, body[:minRangeSize])
	copy(staged[2*minRangeSize:], body[2*minRangeSize:])
	if err := os.WriteFile(partial, staged, 0o644); err != nil {
		t.Fatal(err)
	}
	held, err := store.Claim(id, "dead-owner", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	proven := job.Ranges{{Start: 0, End: minRangeSize}, {Start: 2 * minRangeSize, End: 3 * minRangeSize}}
	if _, err := store.Update(id, held.Lease.Epoch, func(rr *job.Record) error {
		rr.Progress.Done = covered(proven)
		return setCheckpoint(rr, Checkpoint{Verified: proven})
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := "bytes=" + strconv.Itoa(minRangeSize) + "-" + strconv.Itoa(2*minRangeSize-1)
	asked := srv.ranges()
	for _, got := range asked[1:] {
		if got != want {
			t.Fatalf("refetched %s; only the gap %s was missing (all: %v)", got, want, asked)
		}
	}
	if len(asked) != 2 {
		t.Fatalf("expected a probe and one range, got %v", asked)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(got)
	if "sha256:"+hex.EncodeToString(sum[:]) != digest {
		t.Fatal("the gap was filled with the wrong bytes")
	}
}

// A source that answers 200 to a Range cannot be partitioned, and the fallback
// has to be the one stream that still works — not a failure.
func TestFallsBackToOneStreamWhenRangesAreRefused(t *testing.T) {
	body, digest := payload(t, minParallel+1)
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	t.Cleanup(srv.Close)

	r, store, id := parallelJob(t, body, digest, srv.URL+"/blob.bin")
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected a refused probe and one whole-file stream, got %d requests", n)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(got)
	if "sha256:"+hex.EncodeToString(sum[:]) != digest {
		t.Fatal("the fallback stream did not deliver the artifact")
	}
}

// An artifact small enough that the second hashing pass costs more than the
// concurrency saves is never probed at all.
func TestSmallArtifactsAreNotPartitioned(t *testing.T) {
	body, digest := payload(t, 4096)
	srv := newParallelServer(t, body)
	r, _, id := parallelJob(t, body, digest, srv.URL+"/blob.bin")
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, got := range srv.ranges() {
		if got == "bytes=0-0" {
			t.Fatal("a small artifact was probed for range support")
		}
	}
}

// A range is written into the middle of the file, so a proxy answering with a
// different range than the one asked for corrupts bytes no later check could
// attribute to it.
func TestARangeRefusesAContentRangeItDidNotAskFor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 4096-8191/16384")
		w.WriteHeader(http.StatusPartialContent)
		w.Write(make([]byte, 4096))
	}))
	t.Cleanup(srv.Close)

	f, err := os.CreateTemp(t.TempDir(), "chunk")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	err = HTTP{}.FetchRange(context.Background(), RangeRequest{
		Source: Source{Scheme: "https", Locator: srv.URL},
		Range:  job.Range{Start: 0, End: 4096},
		Out:    f,
	})
	if err == nil || !strings.Contains(err.Error(), "Content-Range") {
		t.Fatalf("accepted a range the server did not send: %v", err)
	}
}
