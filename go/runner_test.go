package download

import (
	"context"
	"crypto/rand"
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
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

func payload(t *testing.T, n int) ([]byte, string) {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return b, "sha256:" + hex.EncodeToString(sum[:])
}

func newRunner(t *testing.T) (*Runner, job.Store, string) {
	t.Helper()
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "test-runner")
	r.PersistEvery = 4096 // exercise the persist path in small tests
	return r, store, root
}

func submit(t *testing.T, store job.Store, root, digest string, size int64, sources ...Source) string {
	t.Helper()
	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: size},
		Sources:  sources,
		Sink:     Sink{Final: filepath.Join(root, "final.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// partialOf reads the working file's path out of the job's own spec.
func partialOf(t *testing.T, store job.Store, id string) string {
	t.Helper()
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := SpecOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	partial, _ := LocalSink(store, spec.Sink)
	return partial
}

func finalOf(t *testing.T, store job.Store, id string) string {
	t.Helper()
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := SpecOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	_, final := LocalSink(store, spec.Sink)
	return final
}

// rangeServer serves content and honours Range, like any sane file host.
func rangeServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), strings.NewReader(string(body)))
	}))
	t.Cleanup(s.Close)
	return s
}

// stageDeadOwner leaves the job in the state a crashed owner leaves it in: a
// lease nobody will renew, and progress recorded up to verified.
//
// The lease TTL has to outlive the setup writes themselves. An earlier version
// used 1ms, the Update below failed with ErrLeaseExpiry, the test ignored the
// error, and the job was left with verified_prefix 0 — so two resume tests were
// silently not testing resume at all.
func stageDeadOwner(t *testing.T, store job.Store, id string, done, verified int64) {
	t.Helper()
	const ttl = 100 * time.Millisecond
	held, err := store.Claim(id, "dead-owner", ttl)
	if err != nil {
		t.Fatalf("stage claim: %v", err)
	}
	if _, err := store.Update(id, held.Lease.Epoch, func(rr *job.Record) error {
		rr.Progress.Done = done
		return rr.SetCheckpoint(Checkpoint{VerifiedPrefix: verified})
	}); err != nil {
		t.Fatalf("stage progress: %v", err)
	}
	time.Sleep(ttl + 50*time.Millisecond) // let the lease lapse, as a crash would
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if cp.VerifiedPrefix != verified {
		t.Fatalf("staging did not take: verified_prefix is %d, want %d", cp.VerifiedPrefix, verified)
	}
}

func TestFetchOverHTTP(t *testing.T) {
	body, digest := payload(t, 64<<10)
	srv := rangeServer(t, body)
	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rec, _ := store.Load(id)
	if rec.State != job.StateTransferred {
		t.Fatalf("state = %s, want transferred", rec.State)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatal("delivered bytes differ from the source")
	}
}

// TestFetchFromFileSource covers the NAS tier: a UNC path and a local path are
// the same thing to the operating system, which is why there is no SMB protocol
// implementation and should not be.
func TestFetchFromFileSource(t *testing.T) {
	body, digest := payload(t, 32<<10)
	r, store, root := newRunner(t)
	src := filepath.Join(root, "source.bin")
	if err := os.WriteFile(src, body, 0o644); err != nil {
		t.Fatal(err)
	}
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "file", Locator: src})

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, _ := os.ReadFile(finalOf(t, store, id))
	if string(got) != string(body) {
		t.Fatal("delivered bytes differ from the source")
	}
}

// TestResumeFromPartial is the point of the whole layer. A previous owner left
// a half-written file and a record saying how much of it was proven; the new
// runner must ask only for the rest, and must still end up with a file whose
// digest matches — which it can only do if it rebuilt the hash over the prefix
// it inherited rather than assuming it.
func TestResumeFromPartial(t *testing.T) {
	body, digest := payload(t, 100<<10)
	half := int64(40 << 10)

	var rangeAsked string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeAsked = r.Header.Get("Range")
		http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), strings.NewReader(string(body)))
	}))
	t.Cleanup(srv.Close)

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})

	// Stage what a dead owner would have left behind.
	if err := os.WriteFile(partialOf(t, store, id), body[:half], 0o644); err != nil {
		t.Fatal(err)
	}
	stageDeadOwner(t, store, id, half, half)

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rangeAsked != "bytes="+strconv.FormatInt(half, 10)+"-" {
		t.Fatalf("Range header was %q; the runner did not resume", rangeAsked)
	}
	got, _ := os.ReadFile(finalOf(t, store, id))
	if string(got) != string(body) {
		t.Fatal("resumed file does not match the source")
	}
}

// TestResumeDiscardsUnprovenBytes: the record says 40 KiB were proven but 60 KiB
// are on disk. The extra 20 KiB were written by an owner that died before
// proving them, and nothing vouches for them — here they are deliberately
// garbage. A runner that trusted the file length would produce a file of exactly
// the right size containing corruption, which is the failure this project
// exists to refuse.
func TestResumeDiscardsUnprovenBytes(t *testing.T) {
	body, digest := payload(t, 100<<10)
	proven := int64(40 << 10)

	srv := rangeServer(t, body)
	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})

	staged := append(append([]byte(nil), body[:proven]...), make([]byte, 20<<10)...) // 20 KiB of zeros
	if err := os.WriteFile(partialOf(t, store, id), staged, 0o644); err != nil {
		t.Fatal(err)
	}
	stageDeadOwner(t, store, id, int64(len(staged)), proven)

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, _ := os.ReadFile(finalOf(t, store, id))
	if string(got) != string(body) {
		t.Fatal("the unproven tail was kept; the delivered file is corrupt")
	}
}

// TestPartialShorterThanRecord: the record claims more was proven than the file
// actually holds. That is not a resume point at a lower offset — it is a file
// something other than this library has been writing to — so the attempt fails,
// the partial is thrown away, and the transfer starts again from zero.
func TestPartialShorterThanRecord(t *testing.T) {
	body, digest := payload(t, 64<<10)
	srv := rangeServer(t, body)
	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})

	if err := os.WriteFile(partialOf(t, store, id), body[:1000], 0o644); err != nil {
		t.Fatal(err)
	}
	stageDeadOwner(t, store, id, 50000, 50000) // a lie the file cannot back up

	err := r.Run(context.Background(), id)
	if !errors.Is(err, ErrFileTooShort) {
		t.Fatalf("Run = %v, want ErrFileTooShort — the record was believed down to a lower offset", err)
	}
	if _, err := os.Stat(partialOf(t, store, id)); err == nil {
		t.Fatal("the disagreeing partial was kept; the next runner would resume onto it")
	}
	rec, _ := store.Load(id)
	cp, _ := CheckpointOf(rec)
	if cp.VerifiedPrefix != 0 {
		t.Fatalf("checkpoint still claims %d bytes are proven", cp.VerifiedPrefix)
	}

	// And the restart it asked for actually works.
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	got, _ := os.ReadFile(finalOf(t, store, id))
	if string(got) != string(body) {
		t.Fatal("the restarted download does not match the source")
	}
}

// TestRefusesWrongDigest: the transport succeeded, the length is right, and the
// bytes are wrong. Every tool measured in this project's G-track serves that
// file happily. This one must not.
func TestRefusesWrongDigest(t *testing.T) {
	body, _ := payload(t, 16<<10)
	_, otherDigest := payload(t, 16<<10)
	srv := rangeServer(t, body)
	r, store, root := newRunner(t)
	id := submit(t, store, root, otherDigest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})

	err := r.Run(context.Background(), id)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Run = %v, want ErrDigestMismatch", err)
	}
	rec, _ := store.Load(id)
	if _, err := os.Stat(finalOf(t, store, id)); err == nil {
		t.Fatal("a file that failed verification was delivered anyway")
	}
	// The bad prefix must not survive to be resumed onto.
	if _, err := os.Stat(partialOf(t, store, id)); err == nil {
		t.Fatal("the failed partial was kept; the next runner would resume onto known-bad bytes")
	}
	if rec.Error == "" {
		t.Fatal("nothing recorded why the job failed")
	}
}

// TestRestartsWhenServerIgnoresRange: asked for bytes from N, the server sends
// the whole file from zero. Appending that to what is on disk yields a file of
// plausible length and impossible content. curl's -C - will do exactly this.
//
// The answer is not to fail — the response is a complete, valid artifact — but
// to throw the prefix away and take it from byte zero, which is what the
// delivered content proves happened.
func TestRestartsWhenServerIgnoresRange(t *testing.T) {
	body, digest := payload(t, 32<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // ignores Range entirely
		w.Write(body)
	}))
	t.Cleanup(srv.Close)

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})
	os.WriteFile(partialOf(t, store, id), body[:1000], 0o644)
	stageDeadOwner(t, store, id, 1000, 1000)

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatalf("delivered %d bytes that are not the artifact; the 200 was appended to the prefix", len(got))
	}
}

// TestFallsBackToNextSource: the first source is unreachable, the second is a
// local file. Multi-source is only possible because a job describes an artifact
// and a list of places to get it, rather than describing a URL.
func TestFallsBackToNextSource(t *testing.T) {
	body, digest := payload(t, 8<<10)
	r, store, root := newRunner(t)
	local := filepath.Join(root, "mirror.bin")
	os.WriteFile(local, body, 0o644)

	id := submit(t, store, root, digest, int64(len(body)),
		Source{Scheme: "https", Locator: "http://127.0.0.1:1/nothing-here", Priority: 0},
		Source{Scheme: "file", Locator: local, Priority: 1},
	)
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, _ := os.ReadFile(finalOf(t, store, id))
	if string(got) != string(body) {
		t.Fatal("fallback source did not deliver the artifact")
	}
}

// TestRequiredCapabilityIsHonoured: a job that demands a fetcher which survives
// its own process must not be quietly served by one that does not. Bindings
// differ enormously and pretending otherwise lies to the caller on the tier most
// people actually run.
func TestRequiredCapabilityIsHonoured(t *testing.T) {
	body, digest := payload(t, 4<<10)
	srv := rangeServer(t, body)
	r, store, root := newRunner(t)

	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources:  []Source{{Scheme: "https", Locator: srv.URL}},
		Sink:     Sink{Final: filepath.Join(root, "final.bin")},
	}, string(CapSurvivesProcessExit))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); !errors.Is(err, ErrNoFetcher) {
		t.Fatalf("Run = %v, want ErrNoFetcher — the in-process HTTP fetcher does not survive process exit", err)
	}
}

// TestAdoptRescuesOrphans is the reclamation path a service runs on start: find
// the transfers that were in flight when the machine slept or the app closed,
// and finish them.
func TestAdoptRescuesOrphans(t *testing.T) {
	r, store, root := newRunner(t)
	for i := 0; i < 3; i++ {
		body, digest := payload(t, 4<<10)
		src := filepath.Join(root, fmt.Sprintf("src%d.bin", i))
		os.WriteFile(src, body, 0o644)
		id, err := Submit(store, Spec{
			Artifact: Artifact{Digest: digest, Size: int64(len(body))},
			Sources:  []Source{{Scheme: "file", Locator: src}},
			Sink:     Sink{Final: filepath.Join(root, fmt.Sprintf("final%d.bin", i))},
		})
		if err != nil {
			t.Fatal(err)
		}
		// Claimed by someone who then died.
		if _, err := store.Claim(id, "dead-owner", 50*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	n, err := r.Adopt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("adopted %d orphans, want 3", n)
	}
}

// TestProgressIsVisibleToAnotherProcess: a reader holding no lease sees how far
// the transfer got. A callback could not do this — it is bound to the lifetime
// of the process that registered it, and that is the lifetime that fails.
func TestProgressIsVisibleToAnotherProcess(t *testing.T) {
	body, digest := payload(t, 256<<10)
	r, store, root := newRunner(t)
	src := filepath.Join(root, "big.bin")
	os.WriteFile(src, body, 0o644)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "file", Locator: src})

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	observer, err := job.NewFileStore(localRoot(store))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := observer.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if cp.VerifiedPrefix != int64(len(body)) {
		t.Fatalf("observer saw verified_prefix %d, want %d", cp.VerifiedPrefix, len(body))
	}
}
