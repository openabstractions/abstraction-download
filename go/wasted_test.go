package download

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// countingOrigin serves the artifact and counts every byte it hands out. It
// drops the connection the moment the owner is declared dead.
//
// The count belongs at the origin. What a downloader re-fetched is not
// something the downloader has to admit, and every tool measured against ours
// reported a clean resume while asking the server for the whole file again.
type countingOrigin struct {
	body    []byte
	dead    *atomic.Bool
	cutDone atomic.Bool
	mu      sync.Mutex
	served  int64
}

func (o *countingOrigin) serve(w http.ResponseWriter, r *http.Request) {
	from, to := int64(0), int64(len(o.body))
	ranged := strings.HasPrefix(r.Header.Get("Range"), "bytes=")
	if ranged {
		fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &from)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to-1, to))
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", fmt.Sprint(to-from))
	if ranged {
		w.WriteHeader(http.StatusPartialContent)
	}
	for off := from; off < to; {
		n := int64(64 << 10)
		if off+n > to {
			n = to - off
		}
		if _, err := w.Write(o.body[off : off+n]); err != nil {
			return
		}
		off += n
		o.mu.Lock()
		o.served += n
		o.mu.Unlock()
		// Once, for the owner that is being killed. The successor is a live
		// process and gets a working connection.
		if o.dead.Load() && !o.cutDone.Swap(true) {
			// What a lid closing looks like from the other end: no goodbye, and
			// no chance for either side to write anything down.
			panic(http.ErrAbortHandler)
		}
	}
}

func (o *countingOrigin) total() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.served
}

// diedAfter kills the owner once it has checkpointed n times, and accepts
// nothing from it afterwards.
//
// The kill is triggered by the owner's own progress rather than by the origin's
// byte count, because a server may run megabytes ahead of the client it is
// feeding and how far is a property of the socket buffer. Trigger on the far
// side and the moment of death moves with the operating system: the same cut
// left a checkpoint on Windows and none on Linux.
//
// Release is let through. A killed owner really would hold its lease until it
// lapsed; that is a different property, with its own tests, and it does not
// change how many bytes cross the wire.
type diedAfter struct {
	*job.FileStore
	n    int
	dead *atomic.Bool
	seen atomic.Int64
}

func (s *diedAfter) Update(id string, epoch int64, mutate func(*job.Record) error) (*job.Record, error) {
	if s.dead.Load() {
		return s.FileStore.Load(id)
	}
	r, err := s.FileStore.Update(id, epoch, mutate)
	if r != nil && r.Progress.Done > 0 && s.seen.Add(1) >= int64(s.n) {
		s.dead.Store(true)
	}
	return r, err
}

// Bytes not re-fetched after an interruption, which is the axis this layer is
// actually better on and the one nothing here measured.
//
// Every other number the project owns is bytes per second, and on that axis
// wget is faster. Against the field at 8 GiB, killed a third of the way in, dl
// re-fetched 11.6 MB where curl, wget and aria2c re-fetched all 3.22 GB that
// remained, and an aria2c tuned to save its control file every second still
// wasted 26 times what we did —
// research/downloader-bench/2026-09-07-against-the-field.txt. A number that
// good and never measured is a number that can rot unnoticed, and this one had
// already started to: it found a killed transfer resuming from zero on a fast
// link, because the progress callback the checkpoint hangs off was throttled by
// time alone. See reportEvery in fetchers.go.
//
// Waste is what the origin served beyond the size of the artifact. It is
// bounded by how far a transfer may run ahead of its last checkpoint, so it is
// a property of PersistEvery and of the socket buffer, not of the file: a
// bigger artifact does not make it bigger, which is why the field's numbers
// grow with the download and ours does not. The runner's shipped defaults are
// used deliberately — one tuned to checkpoint more often would measure the
// tuning.
func TestNothingIsFetchedTwiceAfterAKill(t *testing.T) {
	body, digest := payload(t, 64<<20)
	dead := &atomic.Bool{}
	origin := &countingOrigin{body: body, dead: dead}
	srv := httptest.NewServer(http.HandlerFunc(origin.serve))
	t.Cleanup(srv.Close)

	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	first := NewRunner(&diedAfter{FileStore: store, n: 3, dead: dead}, "first-runner")
	first.Connections = 1
	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources:  []Source{{Scheme: "https", Locator: srv.URL + "/blob.bin"}},
		Sink:     Sink{Final: filepath.Join(root, "out", "blob.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := first.Run(context.Background(), id); err == nil {
		t.Fatal("the owner was killed mid-transfer and the run reported success")
	}
	killedAt := origin.total()

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if cp.VerifiedPrefix == 0 {
		t.Fatalf("nothing survived the kill: the origin served %d bytes and the record proves none of them", killedAt)
	}

	// A different owner, as a different process would be. The job is adoptable
	// because the record, not the process, holds the work.
	second := NewRunner(store, "second-runner")
	second.Connections = 1
	if err := second.Run(context.Background(), id); err != nil {
		t.Fatalf("the job was not resumed: %v", err)
	}

	delivered, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(delivered) != string(body) {
		t.Fatal("the resumed file is not the artifact")
	}

	size := int64(len(body))
	wasted := origin.total() - size
	t.Logf("killed after %d proven of %d bytes; the origin had served %d, and %d in all",
		cp.VerifiedPrefix, size, killedAt, origin.total())
	t.Logf("wasted_bytes\t%d", wasted)

	// The exact claim, and the one no buffer can move: the second transfer
	// fetched the remainder and not one byte more.
	if got, want := origin.total()-killedAt, size-cp.VerifiedPrefix; got != want {
		t.Fatalf("the resume fetched %d bytes where %d remained unproven", got, want)
	}
	if wasted >= killedAt {
		t.Fatalf("%d bytes wasted against %d served before the kill: this is a restart, not a resume", wasted, killedAt)
	}
}
