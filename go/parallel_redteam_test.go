package download

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// A range is written at its own offset into a file that other ranges share. A
// server that answers 206 with the right Content-Range and a longer body than
// the range it named writes past the range's end, over bytes a neighbour may
// already have proven. The record still says they are proven.
func TestARangeOverflowMustNotTouchItsNeighbour(t *testing.T) {
	const size = 4096
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", size-1, 2*size))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(bytes.Repeat([]byte{'A'}, 2*size))
	}))
	t.Cleanup(srv.Close)

	f, err := os.CreateTemp(t.TempDir(), "chunk")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	neighbour := bytes.Repeat([]byte{'B'}, size)
	if _, err := f.WriteAt(neighbour, size); err != nil {
		t.Fatal(err)
	}

	err = HTTP{}.FetchRange(context.Background(), RangeRequest{
		Source: Source{Scheme: "https", Locator: srv.URL},
		Range:  job.Range{Start: 0, End: size},
		Out:    f,
	})
	if err == nil {
		t.Fatal("a body longer than the range was accepted")
	}
	got := make([]byte, size)
	if _, err := f.ReadAt(got, size); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, neighbour) {
		t.Fatalf("the overflow overwrote the neighbouring range: %q...", got[:8])
	}
}

// A published plan is taken on trust, and nothing bounds it. gridPlan caps
// itself at maxRanges; a source can publish a million cuts and each one is a
// request, a Sync and a record rewrite.
func TestPublishedPlanIsBounded(t *testing.T) {
	const size = 64 << 20
	var b strings.Builder
	for at := int64(1); at < size; at += 16 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(at, 10))
	}
	src := Source{Attrs: map[string]string{BoundariesAttr: b.String()}}
	p, ok := publishedPlan(src, size)
	if ok && len(p.cuts) > maxRanges {
		t.Fatalf("accepted a plan of %d ranges; gridPlan refuses to make more than %d", len(p.cuts)+1, maxRanges)
	}
}

// The partial is opened without truncation, so a tail past the artifact's end
// — a predecessor's overflow, a stale file — survives every range and is hashed
// with the rest. The whole download is then thrown away for bytes nothing asked
// for.
func TestAStaleTailPastTheArtifactIsNotFatal(t *testing.T) {
	body, digest := payload(t, minParallel+7)
	srv := newParallelServer(t, body)
	r, store, id := parallelJob(t, body, digest, srv.URL+"/blob.bin")

	partial := partialOf(t, store, id)
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := append(append([]byte(nil), body...), bytes.Repeat([]byte{'x'}, 1000)...)
	if err := os.WriteFile(partial, stale, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("a stale tail cost the whole download: %v", err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(body) {
		t.Fatalf("delivered %d bytes for a %d byte artifact", len(got), len(body))
	}
}

// One source failing one range ends the run. The single-stream path tries the
// next source; a parallel run fails, releases, and the next sweep claims it and
// picks the same source again, forever.
func TestAFailingRangeTriesTheNextSource(t *testing.T) {
	body, digest := payload(t, minParallel+7)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), bytes.NewReader(body))
			return
		}
		http.Error(w, "not now", http.StatusServiceUnavailable)
	}))
	t.Cleanup(bad.Close)
	good := newParallelServer(t, body)

	r, store, root := newRunner(t)
	r.Connections = 4
	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources: []Source{
			{Scheme: "https", Locator: bad.URL + "/blob.bin", Priority: 0},
			{Scheme: "https", Locator: good.URL + "/blob.bin", Priority: 1},
		},
		Sink: Sink{Final: filepath.Join(root, "final.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("a second source was available and never tried: %v", err)
	}
	if len(good.ranges()) == 0 {
		t.Fatal("the second source was never asked")
	}
}

// slowStore is a file store whose Update returns late, standing in for a Sync
// or a rename that takes a while on a share. It is called from inside
// progress.proved, under the progress mutex.
type slowStore struct {
	*job.FileStore
	after time.Duration
	once  sync.Once
}

func (s *slowStore) Update(id string, epoch int64, mutate func(*job.Record) error) (*job.Record, error) {
	rec, err := s.FileStore.Update(id, epoch, mutate)
	s.once.Do(func() {
		s.FileStore.SetIntent(id, job.WantPause, "ui")
		time.Sleep(s.after)
	})
	return rec, err
}

// The keeper reads intent through a callback that takes the progress mutex,
// and proved holds that mutex across a Sync and a record write. A pause pressed
// while one range is being recorded slowly parks the keeper in the callback: it
// renews nothing until proved lets go, and the lease lapses under an owner that
// is alive, working, and about to honour the pause.
func TestAPauseDuringASlowCheckpointMustNotCostTheLease(t *testing.T) {
	body, digest := payload(t, minParallel+7)
	srv := newParallelServer(t, body)

	root := t.TempDir()
	fs, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	store := &slowStore{FileStore: fs, after: 500 * time.Millisecond}
	r := NewRunner(store, "test-runner")
	r.Connections = 4
	r.LeaseTTL = 300 * time.Millisecond
	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources:  []Source{{Scheme: "https", Locator: srv.URL + "/blob.bin"}},
		Sink:     Sink{Final: filepath.Join(root, "final.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("a pause during one slow checkpoint, shorter than two TTLs, ended as: %v", err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != job.StatePending || rec.Lease.Owner != "" {
		t.Fatalf("after an honoured pause the record is %s, held by %q", rec.State, rec.Lease.Owner)
	}
}

// fencingStore refuses every renewal after the first, which is what a store
// says to an owner whose epoch has moved on.
type fencingStore struct {
	*job.FileStore
	renewals atomic.Int32
}

func (s *fencingStore) Renew(id string, epoch int64, ttl time.Duration) (*job.Record, error) {
	if s.renewals.Add(1) > 1 {
		return nil, fmt.Errorf("%w: fenced by the test", job.ErrStaleEpoch)
	}
	return s.FileStore.Renew(id, epoch, ttl)
}

// The fence: a refused renewal must stop every range, and nothing may be
// recorded after it. The file may still grow by the chunk each stream had
// already read — that is the bound, and it is checked.
func TestARefusedRenewalStopsEveryRange(t *testing.T) {
	body, digest := payload(t, minParallel+7)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), bytes.NewReader(body))
			return
		}
		http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), &drip{r: bytes.NewReader(body), every: 5 * time.Millisecond})
	}))
	t.Cleanup(srv.Close)

	root := t.TempDir()
	fs, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	store := &fencingStore{FileStore: fs}
	r := NewRunner(store, "test-runner")
	r.Connections = 2
	r.LeaseTTL = 300 * time.Millisecond
	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources:  []Source{{Scheme: "https", Locator: srv.URL + "/blob.bin"}},
		Sink:     Sink{Final: filepath.Join(root, "final.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	partial := partialOf(t, store, id)

	err = r.Run(context.Background(), id)
	if !errors.Is(err, job.ErrStaleEpoch) {
		t.Fatalf("run ended with %v, not the fence", err)
	}
	at := sizeOf(t, partial)
	time.Sleep(300 * time.Millisecond)
	if after := sizeOf(t, partial); after != at {
		t.Fatalf("the file grew by %d bytes after the run returned fenced", after-at)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rs, _ := rec.CheckpointRanges(); len(rs) != 0 {
		t.Fatalf("a fenced owner recorded %v", rs)
	}
	if at > int64(len(body)) {
		t.Fatalf("file is %d bytes for a %d byte artifact", at, len(body))
	}
}

type drip struct {
	r     *bytes.Reader
	every time.Duration
}

func (tr *drip) Read(p []byte) (int, error) {
	if len(p) > 64<<10 {
		p = p[:64<<10]
	}
	time.Sleep(tr.every)
	return tr.r.Read(p)
}

func (tr *drip) Seek(off int64, whence int) (int64, error) { return tr.r.Seek(off, whence) }

func sizeOf(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

// Renew is load, edit, write. So is SetIntent, and so is Update. None of them
// presents what it loaded, so any two that overlap lose one. The keeper renews
// three times a TTL for the whole transfer; this measures how often a pause
// pressed during a renewal is silently erased, and how often a checkpoint goes
// backwards.
func TestRenewalErasesConcurrentWrites(t *testing.T) {
	const rounds = 40
	lostPause, regressed, refused := 0, 0, 0
	for i := 0; i < rounds; i++ {
		root := t.TempDir()
		store, err := job.NewFileStore(root)
		if err != nil {
			t.Fatal(err)
		}
		id, err := Submit(store, Spec{
			Artifact: Artifact{Size: 1},
			Sources:  []Source{{Scheme: "https", Locator: "https://example.invalid/x"}},
			Sink:     Sink{Final: filepath.Join(root, "final.bin")},
		})
		if err != nil {
			t.Fatal(err)
		}
		held, err := store.Claim(id, "owner", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		epoch := held.Lease.Epoch

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
				store.Renew(id, epoch, time.Minute)
				time.Sleep(time.Millisecond)
			}
		}()

		const steps = 20
		for n := int64(1); n <= steps; n++ {
			if _, err := store.Update(id, epoch, func(rr *job.Record) error {
				rr.Progress.Done = n
				return nil
			}); err != nil {
				refused++
			}
		}
		if _, err := store.SetIntent(id, job.WantPause, "ui"); err != nil {
			refused++
		}
		close(stop)
		wg.Wait()

		rec, err := store.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Wants() != job.WantPause {
			lostPause++
		}
		if rec.Progress.Done != steps {
			regressed++
		}
	}
	t.Logf("%d rounds: pause erased %d times, checkpoint went backwards %d times, %d writes refused outright", rounds, lostPause, regressed, refused)
	if lostPause > 0 || regressed > 0 {
		t.Fatalf("a renewal overwrote another writer: %d pauses lost, %d checkpoints regressed in %d rounds", lostPause, regressed, rounds)
	}
}

// The file is hashed after the last range, by path, with nothing holding it. If
// the bytes change between the two — here: a stray writer, standing in for a
// predecessor still holding the file across a share — the digest catches it,
// and everything is thrown away. The parallel path is honest here; this pins
// that it stays so.
func TestHashIsOverTheFileNotTheStream(t *testing.T) {
	body, digest := payload(t, minParallel+7)
	srv := newParallelServer(t, body)
	r, store, id := parallelJob(t, body, digest, srv.URL+"/blob.bin")
	partial := partialOf(t, store, id)

	r.Connections = 2
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if st, err := os.Stat(partial); err == nil && st.Size() == int64(len(body)) {
				f, err := os.OpenFile(partial, os.O_WRONLY, 0)
				if err == nil {
					f.WriteAt([]byte{'!'}, 0)
					f.Close()
				}
				return
			}
			select {
			case <-time.After(time.Millisecond):
			}
		}
	}()
	err := r.Run(context.Background(), id)
	<-done
	if err == nil {
		got, _ := os.ReadFile(finalOf(t, store, id))
		sum := sha256.Sum256(got)
		if "sha256:"+hex.EncodeToString(sum[:]) != digest {
			t.Fatal("delivered a file whose bytes are not the artifact")
		}
		return
	}
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("run failed for another reason: %v", err)
	}
}
