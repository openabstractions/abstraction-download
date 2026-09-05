package download

import (
	"context"
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

// resumeService is serviceOn plus the runner, because these tests have to move
// real bytes to see a real Range header.
func resumeService(t *testing.T) (Service, *Runner, job.Store, string) {
	t.Helper()
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "test-owner")
	r.PersistEvery = 4096
	// A supervisor is pretended to be watching for the same reason service_test
	// does it: Submit and ResumeOrSubmit then hand the work over instead of
	// starting a goroutine no test can stop. Every run below is explicit.
	if err := Heartbeat(store, "test-supervisor@host:1", "here", time.Minute); err != nil {
		t.Fatal(err)
	}
	return NewService(r), r, store, root
}

// recordingServer serves body, honours Range, and remembers every Range header
// it was sent.
func recordingServer(t *testing.T, body []byte) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Range"))
		mu.Unlock()
		http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), strings.NewReader(string(body)))
	}))
	t.Cleanup(s.Close)
	return s, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

// writePartial puts n bytes of body where the job's own spec says its working
// file lives, which is what an interrupted transfer leaves behind.
func writePartial(t *testing.T, store job.Store, id string, body []byte, n int) {
	t.Helper()
	p := partialOf(t, store, id)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, body[:n], 0o644); err != nil {
		t.Fatal(err)
	}
}

// A one-shot command has no job id to remember. Asked a second time for the same
// destination, it must be handed the record it made the first time.
func TestASecondCallFindsTheFirstRecord(t *testing.T) {
	svc, _, store, root := resumeService(t)
	spec := Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/a.bin"}},
		Sink:    Sink{Final: filepath.Join(root, "out", "a.bin")},
	}

	first, c1, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c1.Disposition != Submitted {
		t.Fatalf("first call reported %q, want %q", c1.Disposition, Submitted)
	}
	second, c2, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c2.Disposition != Resumed {
		t.Fatalf("second call reported %q, want %q", c2.Disposition, Resumed)
	}
	if first.ID() != second.ID() {
		t.Fatalf("two ids for one destination: %s and %s", first.ID(), second.ID())
	}
	all, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("%d records for one destination, want 1", len(all))
	}
}

// The whole point: the second attempt asks the server for the rest of the file
// and not for all of it. Asserted on the wire, not inferred from how long it
// took or how many bytes arrived.
func TestTheSecondAttemptSendsARangeRequest(t *testing.T) {
	body, digest := payload(t, 64<<10)
	const have = 12 << 10
	srv, ranges := recordingServer(t, body)
	svc, runner, store, root := resumeService(t)

	spec := Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources:  []Source{{Scheme: "https", Locator: srv.URL}},
		Sink:     Sink{Final: filepath.Join(root, "out", "b.bin")},
	}
	first, _, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	// What a killed download leaves: bytes on disk and a record saying how many
	// of them were proven.
	writePartial(t, store, first.ID(), body, have)
	stageDeadOwner(t, store, first.ID(), have, have)

	second, c, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID() != first.ID() {
		t.Fatalf("second call started a new job %s instead of continuing %s", second.ID(), first.ID())
	}
	if c.Disposition != Resumed {
		t.Fatalf("disposition %q, want %q", c.Disposition, Resumed)
	}
	if c.ResumeFrom != have {
		t.Fatalf("ResumeFrom = %d, want %d", c.ResumeFrom, have)
	}

	if err := runner.Run(context.Background(), second.ID()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := ranges()
	if len(got) == 0 {
		t.Fatal("the server was never asked for anything")
	}
	last := got[len(got)-1]
	if want := "bytes=12288-"; last != want {
		t.Fatalf("Range header was %q, want %q — the second attempt did not resume", last, want)
	}
	delivered, err := os.ReadFile(finalOf(t, store, second.ID()))
	if err != nil {
		t.Fatal(err)
	}
	if string(delivered) != string(body) {
		t.Fatal("the resumed file does not match the source")
	}
}

// Two shells, one destination, at the same instant. The store's refusal to
// create one id twice is what makes this one record rather than two racing
// transfers to the same path.
func TestConcurrentCallersProduceOneRecord(t *testing.T) {
	svc, _, store, root := resumeService(t)
	spec := Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/c.bin"}},
		Sink:    Sink{Final: filepath.Join(root, "out", "c.bin")},
	}

	const callers = 8
	ids := make([]string, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			j, _, err := svc.ResumeOrSubmit(spec)
			if err != nil {
				errs[i] = err
				return
			}
			ids[i] = j.ID()
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	for i, id := range ids {
		if id != ids[0] {
			t.Fatalf("caller %d got %s, caller 0 got %s", i, id, ids[0])
		}
	}
	all, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("%d records for one destination, want 1", len(all))
	}
}

// A checkpoint is a claim about a file. When the file disagrees, the file wins:
// resuming from a byte that is not there produces the right length and the wrong
// contents, which no later check would catch without a digest.
func TestAVanishedPartialIsNotTrusted(t *testing.T) {
	body, digest := payload(t, 32<<10)
	srv, ranges := recordingServer(t, body)
	svc, runner, store, root := resumeService(t)

	spec := Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources:  []Source{{Scheme: "https", Locator: srv.URL}},
		Sink:     Sink{Final: filepath.Join(root, "out", "d.bin")},
	}
	first, _, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	// A record that believes 20 KiB are proven, and nothing on disk at all.
	stageDeadOwner(t, store, first.ID(), 20<<10, 20<<10)
	if _, err := os.Stat(partialOf(t, store, first.ID())); !os.IsNotExist(err) {
		t.Fatal("this test needs the partial file to be absent")
	}

	_, c, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c.ResumeFrom != 0 {
		t.Fatalf("ResumeFrom = %d, want 0 — the checkpoint was believed over the filesystem", c.ResumeFrom)
	}
	if err := runner.Run(context.Background(), first.ID()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range ranges() {
		if r != "" {
			t.Fatalf("a Range header of %q was sent for a file with no bytes on disk", r)
		}
	}
	delivered, err := os.ReadFile(finalOf(t, store, first.ID()))
	if err != nil {
		t.Fatal(err)
	}
	if string(delivered) != string(body) {
		t.Fatal("the delivered file does not match the source")
	}
}

// The same, one step less obvious: the file is there but shorter than the
// checkpoint says.
func TestAShortPartialResumesFromTheFile(t *testing.T) {
	body, _ := payload(t, 32<<10)
	svc, _, store, root := resumeService(t)
	spec := Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/e.bin"}},
		Sink:    Sink{Final: filepath.Join(root, "out", "e.bin")},
	}
	first, _, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	writePartial(t, store, first.ID(), body, 1<<10)
	stageDeadOwner(t, store, first.ID(), 20<<10, 20<<10)

	_, c, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c.ResumeFrom != 1<<10 {
		t.Fatalf("ResumeFrom = %d, want %d", c.ResumeFrom, 1<<10)
	}
}

// A URL is not the identity; the destination is. Continuing anyway is a choice,
// so the caller is told which source the job it was handed actually fetches.
func TestADifferentSourceIsContinuedAndReported(t *testing.T) {
	svc, _, _, root := resumeService(t)
	final := filepath.Join(root, "out", "f.bin")
	first, _, err := svc.ResumeOrSubmit(Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://origin.invalid/f.bin"}},
		Sink:    Sink{Final: final},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, c, err := svc.ResumeOrSubmit(Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://mirror.invalid/f.bin"}},
		Sink:    Sink{Final: final},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.ID() != first.ID() {
		t.Fatal("a mirror of the same destination became a second job")
	}
	if !c.SourceChanged {
		t.Fatal("the caller was not told that the job fetches from somewhere else")
	}
	if c.Source != "https://origin.invalid/f.bin" {
		t.Fatalf("Source = %q, want the record's own locator", c.Source)
	}
}

// Complete means the file is there. Checked, not believed.
func TestACompletedDownloadIsNotFetchedAgain(t *testing.T) {
	svc, _, store, root := resumeService(t)
	final := filepath.Join(root, "out", "g.bin")
	spec := Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/g.bin"}},
		Sink:    Sink{Final: final},
	}
	first, _, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("done"), 0o644); err != nil {
		t.Fatal(err)
	}
	complete(t, store, first.ID())

	second, c, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c.Disposition != Delivered {
		t.Fatalf("disposition %q, want %q", c.Disposition, Delivered)
	}
	if second.ID() != first.ID() {
		t.Fatal("a finished download was fetched again")
	}

	// And when the file is gone, the completed record is history rather than a
	// claim on the path.
	if err := os.Remove(final); err != nil {
		t.Fatal(err)
	}
	third, c, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if third.ID() == first.ID() {
		t.Fatal("a completed record was reused although its file had gone")
	}
	if c.Disposition != Submitted {
		t.Fatalf("disposition %q, want %q", c.Disposition, Submitted)
	}
}

// Somebody else is downloading to this path. Their lease is theirs.
func TestAnotherOwnersLeaseIsNotTaken(t *testing.T) {
	svc, _, store, root := resumeService(t)
	spec := Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/h.bin"}},
		Sink:    Sink{Final: filepath.Join(root, "out", "h.bin")},
	}
	first, _, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	held, err := store.Claim(first.ID(), "somebody-else@host:99", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	second, c, err := svc.ResumeOrSubmit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID() != first.ID() {
		t.Fatal("a job somebody else is doing was duplicated")
	}
	if c.Disposition != Busy {
		t.Fatalf("disposition %q, want %q", c.Disposition, Busy)
	}
	after, err := store.Load(first.ID())
	if err != nil {
		t.Fatal(err)
	}
	if after.Lease.Owner != "somebody-else@host:99" || after.Lease.Epoch != held.Lease.Epoch {
		t.Fatalf("the lease was taken: owner %q epoch %d", after.Lease.Owner, after.Lease.Epoch)
	}
}

// Two spellings of one path are one destination.
func TestTwoSpellingsOfOneDestinationAreOneRecord(t *testing.T) {
	svc, _, store, root := resumeService(t)
	src := []Source{{Scheme: "https", Locator: "https://example.invalid/i.bin"}}
	plain := filepath.Join(root, "out", "i.bin")
	roundabout := root + "/out/./sub/../i.bin"

	first, _, err := svc.ResumeOrSubmit(Spec{Sources: src, Sink: Sink{Final: plain}})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := svc.ResumeOrSubmit(Spec{Sources: src, Sink: Sink{Final: roundabout}})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID() != second.ID() {
		t.Fatalf("%q and %q became two jobs", plain, roundabout)
	}
	all, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("%d records for one destination, want 1", len(all))
	}
}

// ResumeOrGet is the shape a command-line tool has: a URL and a path.
func TestResumeOrGetIsKeyedOnTheSamePath(t *testing.T) {
	svc, _, store, root := resumeService(t)
	dest := filepath.Join(root, "out", "j.bin")
	first, _, err := svc.ResumeOrGet("https://example.invalid/j.bin", dest)
	if err != nil {
		t.Fatal(err)
	}
	second, c, err := svc.ResumeOrGet("https://example.invalid/j.bin", dest)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID() != second.ID() {
		t.Fatal("running the command twice made two jobs")
	}
	if c.Disposition != Resumed {
		t.Fatalf("disposition %q, want %q", c.Disposition, Resumed)
	}
	all, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("%d records, want 1", len(all))
	}
}

// The id is the cross-language part of this contract. If Go and Python derived
// it differently, a download begun by one and re-run through the other would be
// two records for one file — the very failure this operation removes. The Python
// test of the same name pins the same string.
func TestTheIDForADestinationIsPinned(t *testing.T) {
	const want = "dest-01ec6db371a234af"
	if got := destinationID("/models/x.gguf"); got != want {
		t.Fatalf("destinationID = %q, want %q — Go and Python no longer agree", got, want)
	}
}

// complete marks a job finished the way a delivered one is.
func complete(t *testing.T, store job.Store, id string) {
	t.Helper()
	rec, err := store.Claim(id, "test-finisher", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(id, rec.Lease.Epoch, func(r *job.Record) error {
		r.State = job.StateComplete
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
