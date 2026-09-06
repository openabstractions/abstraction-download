package download

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// recordRanges serves content, honours Range like any sane file host, and keeps
// the Range header of every request.
//
// The headers are the point of most of this file. Whether a resume SKIPS proven
// bytes or merely ends up with the right file afterwards is invisible in the
// delivered artifact — a downloader that re-fetches everything after the first
// hole produces exactly the same bytes, just slower, and slower by a whole
// artifact is the failure this change exists to remove. The only place the
// difference shows is in what went out on the wire.
func recordRanges(t *testing.T, body []byte) (*httptest.Server, func() []string) {
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

// stageSparse leaves the job in the state a parallel fetcher leaves it in: a
// partial holding the artifact's real bytes at scattered offsets, junk in the
// holes it has not filled yet, a checkpoint naming the proven ranges, and a
// lease nobody will renew.
//
// The file is exactly as long as the furthest proven byte, which is what a
// sparse file IS — the length is set by the highest offset written, and says
// nothing whatever about the hole in the middle.
func stageSparse(t *testing.T, store job.Store, id string, body []byte, proven ...Range) {
	t.Helper()
	rs, err := job.CanonicalRanges(proven)
	if err != nil {
		t.Fatal(err)
	}
	reach := rs[len(rs)-1].End
	// Junk everywhere, then the real bytes where they are claimed. Junk rather
	// than zeroes so that a run which fails to overwrite a hole cannot pass the
	// digest check by luck.
	file := make([]byte, reach)
	for i := range file {
		file[i] = 0xA5
	}
	for _, r := range rs {
		copy(file[r.Start:r.End], body[r.Start:r.End])
	}
	if err := os.WriteFile(partialOf(t, store, id), file, 0o644); err != nil {
		t.Fatal(err)
	}

	const ttl = 100 * time.Millisecond
	held, err := store.Claim(id, "dead-owner", ttl)
	if err != nil {
		t.Fatalf("stage claim: %v", err)
	}
	if _, err := store.Update(id, held.Lease.Epoch, func(rr *job.Record) error {
		rr.Progress.Done = rs.Total()
		return setCheckpoint(rr, Checkpoint{Verified: rs})
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
	if !cp.Verified.Equal(rs) {
		t.Fatalf("staging did not take: checkpoint holds %v, want %v", cp.Verified, rs)
	}
}

// TestResumeSkipsAProvenMiddleRange is the whole point of the change.
//
// Eight kilobytes at the front and sixteen in the middle are proven; the rest is
// not. The old rule could only say "the first 8 KiB are proven", so it resumed
// at byte 8192 and fetched everything after it — including the sixteen
// kilobytes already on disk. The new one asks for the two holes and nothing
// else, and the assertion is on the Range headers rather than on the delivered
// file, because the delivered file is identical either way.
func TestResumeSkipsAProvenMiddleRange(t *testing.T) {
	body, digest := payload(t, 64<<10)
	srv, ranges := recordRanges(t, body)
	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})

	stageSparse(t, store, id, body,
		Range{Start: 0, End: 8 << 10},
		Range{Start: 24 << 10, End: 40 << 10},
	)

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The last byte position in HTTP is inclusive, so the bounded hole
	// [8192, 24576) goes out as 8192-24575. The final hole runs to the end of
	// the artifact and is asked for open-ended, which is the identical request
	// a single-stream resume has always sent.
	want := []string{"bytes=8192-24575", "bytes=40960-"}
	got := ranges()
	if len(got) != len(want) {
		t.Fatalf("sent %d requests %q; want exactly the two holes %q", len(got), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("request %d asked %q, want %q (all requests: %q)", i, got[i], want[i], got)
		}
	}

	delivered, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(delivered) != string(body) {
		t.Fatalf("delivered %d bytes that are not the artifact", len(delivered))
	}

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != job.StateTransferred {
		t.Fatalf("state is %s, want %s", rec.State, job.StateTransferred)
	}
	// Whole again, so there is nothing a range set can say that the prefix
	// cannot, and the declaration goes with it.
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if cp.VerifiedPrefix != int64(len(body)) || len(cp.Verified) != 1 {
		t.Fatalf("finished checkpoint is %+v, want one range covering the artifact", cp)
	}
	if containsModel(rec.Content, job.FeatureRanges) {
		t.Fatalf("a finished download still declares %s; content is %v", job.FeatureRanges, rec.Content)
	}
}

// TestASparsePartialIsNotProgress is the trap the design named.
//
// A sparse file is exactly as long as its furthest written byte, so length says
// nothing about the holes. Three separate wrong answers are available here and
// each is checked: believing the length, believing the count of proven bytes,
// and comparing the two against each other.
func TestASparsePartialIsNotProgress(t *testing.T) {
	dir := t.TempDir()
	partial := dir + string(os.PathSeparator) + "sparse.bin"

	// 100 bytes long, 20 of them proven: [0,10) and [90,100).
	if err := os.WriteFile(partial, make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	cp := Checkpoint{Verified: Ranges{{Start: 0, End: 10}, {Start: 90, End: 100}}}

	plan, err := planResume(partial, cp, 100)
	if err != nil {
		t.Fatalf("planResume: %v", err)
	}
	if plan.Have.Total() != 20 {
		t.Fatalf("holds %d proven bytes, want 20 — the file's 100-byte length was mistaken for progress", plan.Have.Total())
	}
	if plan.From() != 10 {
		t.Fatalf("next request starts at %d, want 10", plan.From())
	}
	if plan.Discarded != 0 {
		t.Fatalf("discarded %d bytes; nothing above the highest proven byte exists to discard", plan.Discarded)
	}
	if plan.Trim != 100 {
		t.Fatalf("trimmed to %d; trimming below 100 would delete the proven range at 90", plan.Trim)
	}
	if len(plan.Gaps) != 1 || plan.Gaps[0] != (fetchRange{From: 10, To: 90}) {
		t.Fatalf("gaps are %+v, want the single hole [10,90) bounded so the range at 90 is not re-fetched", plan.Gaps)
	}

	// A file LONGER than the count of proven bytes and SHORTER than the highest
	// proven offset. Under the old rule — compare the length against the number
	// the checkpoint holds — 50 exceeds 20 and this is a healthy resume that is
	// merely ahead of its checkpoint. It is neither: the file is the floor, so
	// the range at 90 is struck out because the file does not reach it and the
	// range at 0 stands because it does.
	short := dir + string(os.PathSeparator) + "short.bin"
	if err := os.WriteFile(short, make([]byte, 50), 0o644); err != nil {
		t.Fatal(err)
	}
	clipped, err := planResume(short, cp, 100)
	if err != nil {
		t.Fatalf("short file: %v", err)
	}
	if clipped.Have.Total() != 10 || clipped.Trim != 10 {
		t.Fatalf("kept %d bytes trimmed to %d, want the 10 the file can back up", clipped.Have.Total(), clipped.Trim)
	}
	if len(clipped.Gaps) != 1 || clipped.Gaps[0] != (fetchRange{From: 10}) {
		t.Fatalf("gaps are %+v, want one open request from 10 — nothing above it is proven any more", clipped.Gaps)
	}

	// And a genuine unproven tail is still cut off, at the highest proven
	// offset rather than at the resume point — cutting at 10 would delete the
	// proven range at 90.
	long := dir + string(os.PathSeparator) + "long.bin"
	if err := os.WriteFile(long, make([]byte, 140), 0o644); err != nil {
		t.Fatal(err)
	}
	plan, err = planResume(long, cp, 100)
	if err != nil {
		t.Fatalf("planResume: %v", err)
	}
	if plan.Trim != 100 || plan.Discarded != 40 {
		t.Fatalf("trim %d discard %d; want a trim to the highest proven byte and 40 unproven bytes counted", plan.Trim, plan.Discarded)
	}
}

// TestPrefixOnlyRecordReadsAndContinues: a record written by something that has
// never heard of ranges.
//
// This is not a compatibility courtesy, it is the ordinary case for a long
// time: the Python and C++ bindings, the delegated path in this very package,
// and every record already on disk all write a bare prefix. Each has to read
// back as one range and resume exactly as it always did.
func TestPrefixOnlyRecordReadsAndContinues(t *testing.T) {
	body, digest := payload(t, 32<<10)
	srv, ranges := recordRanges(t, body)
	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})

	// Written as a stranger's implementation would write it: the raw JSON, no
	// `verified` key, no content feature.
	const proven = 8 << 10
	if err := os.WriteFile(partialOf(t, store, id), body[:proven], 0o644); err != nil {
		t.Fatal(err)
	}
	const ttl = 100 * time.Millisecond
	held, err := store.Claim(id, "prefix-only-writer", ttl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(id, held.Lease.Epoch, func(rr *job.Record) error {
		rr.Progress.Done = proven
		rr.Checkpoint = []byte(`{"verified_prefix":8192}`)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(ttl + 50*time.Millisecond)

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatalf("a prefix-only checkpoint would not read: %v", err)
	}
	if !cp.Verified.Equal(Ranges{{Start: 0, End: proven}}) {
		t.Fatalf("a prefix of %d read as %v, want the one range [0,%d)", proven, cp.Verified, proven)
	}

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := ranges(); len(got) != 1 || got[0] != "bytes=8192-" {
		t.Fatalf("continued a prefix-only record with %q, want the one open-ended request \"bytes=8192-\"", got)
	}
	delivered, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(delivered) != string(body) {
		t.Fatalf("delivered %d bytes that are not the artifact", len(delivered))
	}
}

// TestPrefixAndRangesAreUnioned: a prefix-only writer took over a job that had
// ranges, advanced the prefix, and left the two fields disagreeing.
//
// The record format says union them, and the reason is that each field is a
// claim that bytes ARE proven and neither is a claim that other bytes are not.
// Trusting `verified` alone throws away everything the newer prefix proved;
// refusing the record rejects what other implementations accept.
func TestPrefixAndRangesAreUnioned(t *testing.T) {
	rec := &job.Record{ID: "x", Kind: Kind}
	rec.Checkpoint = []byte(`{"verified_prefix":5000,"verified":[[0,1000],[9000,10000]]}`)

	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	want := Ranges{{Start: 0, End: 5000}, {Start: 9000, End: 10000}}
	if !cp.Verified.Equal(want) {
		t.Fatalf("read %v, want %v", cp.Verified, want)
	}
	if cp.VerifiedPrefix != 5000 {
		t.Fatalf("prefix reads %d, want the further of the two claims, 5000", cp.VerifiedPrefix)
	}
}

// TestRangesRoundTripThroughTheStore: what goes through a real store binding
// comes back canonical, and says so.
func TestRangesRoundTripThroughTheStore(t *testing.T) {
	_, store, root := newRunner(t)
	body, digest := payload(t, 1<<10)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})

	held, err := store.Claim(id, "writer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	epoch := held.Lease.Epoch

	// Deliberately not canonical going in: out of order, overlapping, adjacent,
	// duplicated and empty. All five have to come back as one spelling, because
	// two implementations that spell one state two ways fail the byte
	// comparison this project is built on.
	messy := Ranges{
		{Start: 600, End: 700},
		{Start: 0, End: 100},
		{Start: 100, End: 150}, // adjacent to the one before it
		{Start: 120, End: 200}, // overlapping
		{Start: 600, End: 700}, // duplicate
		{Start: 300, End: 300}, // empty
	}
	if _, err := store.Update(id, epoch, func(rr *job.Record) error {
		return setCheckpoint(rr, Checkpoint{Verified: messy})
	}); err != nil {
		t.Fatal(err)
	}

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	want := Ranges{{Start: 0, End: 200}, {Start: 600, End: 700}}
	if !cp.Verified.Equal(want) {
		t.Fatalf("round-tripped to %v, want %v", cp.Verified, want)
	}
	if cp.VerifiedPrefix != 200 {
		t.Fatalf("prefix is %d, want the end of the range starting at zero, 200", cp.VerifiedPrefix)
	}
	// Compacted before comparing, because the store binding re-indents the
	// record it writes — every element on its own line, which is what all three
	// encoders do by default and what the design says a comparison must expect.
	// The state is what is pinned here, not the whitespace.
	if got := compact(t, rec.Checkpoint); got != `{"verified_prefix":200,"verified":[[0,200],[600,700]],"validators":{}}` {
		t.Fatalf("stored checkpoint is %s, which is not the one spelling three languages agreed on", got)
	}
	if !containsModel(rec.Content, job.FeatureRanges) {
		t.Fatalf("a record carrying ranges does not declare %s; content is %v", job.FeatureRanges, rec.Content)
	}
	// Advisory, always. A reader that ignores ranges re-fetches some bytes and
	// is otherwise correct, so marking this critical would stop every existing
	// reader dead for no safety gain.
	if containsModel(rec.Critical, job.FeatureRanges) {
		t.Fatalf("%s is marked critical; critical is %v", job.FeatureRanges, rec.Critical)
	}

	// Validators are not this layer's keys and survive alongside the ranges,
	// in the one order every implementation writes.
	if _, err := store.Update(id, epoch, func(rr *job.Record) error {
		return setCheckpoint(rr, Checkpoint{Verified: want, Validators: Validators{ETag: `"v1"`}})
	}); err != nil {
		t.Fatal(err)
	}
	rec, _ = store.Load(id)
	if got := compact(t, rec.Checkpoint); got != `{"verified_prefix":200,"verified":[[0,200],[600,700]],"validators":{"etag":"\"v1\""}}` {
		t.Fatalf("checkpoint with validators is %s", got)
	}
	if cp, _ = CheckpointOf(rec); cp.Validators.ETag != `"v1"` {
		t.Fatalf("validators did not survive the round trip: %+v", cp.Validators)
	}
}

// TestPrefixShapedRecordsAreUnchanged pins the bytes a single-stream download
// writes.
//
// A record with nothing but a prefix to report must be spelled the way it was
// spelled before ranges existed — no `verified`, no declaration — because that
// is what the other two bindings still write and what the byte comparison
// between them compares. Recording ranges is for state a prefix cannot express;
// spending the key on state it can would move a record nobody asked to move.
func TestPrefixShapedRecordsAreUnchanged(t *testing.T) {
	rec := &job.Record{ID: "x", Kind: Kind}

	// `"validators":{}` and not an absent key: omitempty does nothing to a
	// struct in Go, so this is what a prefix checkpoint has spelled since the
	// field existed, and it is what the other bindings compare against.
	if err := setCheckpoint(rec, Checkpoint{VerifiedPrefix: 4096}); err != nil {
		t.Fatal(err)
	}
	if got := string(rec.Checkpoint); got != `{"verified_prefix":4096,"validators":{}}` {
		t.Fatalf("prefix-only checkpoint is %s", got)
	}
	if err := setCheckpoint(rec, Checkpoint{VerifiedPrefix: 4096, Validators: Validators{ETag: `"v1"`}}); err != nil {
		t.Fatal(err)
	}
	if got := string(rec.Checkpoint); got != `{"verified_prefix":4096,"validators":{"etag":"\"v1\""}}` {
		t.Fatalf("prefix-and-validators checkpoint is %s", got)
	}
	if containsModel(rec.Content, job.FeatureRanges) {
		t.Fatalf("a prefix-shaped record declares %s", job.FeatureRanges)
	}

	// Holes appear, so the feature is declared; the holes are then filled, so it
	// is withdrawn. Carried rather than derived means nothing else will do it.
	if err := setCheckpoint(rec, Checkpoint{Verified: Ranges{{Start: 0, End: 10}, {Start: 20, End: 30}}}); err != nil {
		t.Fatal(err)
	}
	if !containsModel(rec.Content, job.FeatureRanges) {
		t.Fatalf("holes did not declare %s; content is %v", job.FeatureRanges, rec.Content)
	}
	if err := setCheckpoint(rec, Checkpoint{Verified: Ranges{{Start: 0, End: 30}}}); err != nil {
		t.Fatal(err)
	}
	if containsModel(rec.Content, job.FeatureRanges) {
		t.Fatalf("filling the holes left %s declared; content is %v", job.FeatureRanges, rec.Content)
	}
	if got := string(rec.Checkpoint); got != `{"verified_prefix":30,"validators":{}}` {
		t.Fatalf("checkpoint after the holes closed is %s", got)
	}
}

// TestContinuationReportsProvenBytesAndResumePoint: the two numbers that used to
// be one.
//
// A resume that begins at byte zero because the first hole is at zero may still
// be holding almost the whole artifact. Reporting only the resume point tells
// the person watching that their download started over.
func TestContinuationReportsProvenBytesAndResumePoint(t *testing.T) {
	body, digest := payload(t, 64<<10)
	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: "https://example.invalid/x"})

	// Nothing at the front, everything after it: the shape a parallel fetcher
	// leaves when the part covering byte zero is the one that failed.
	stageSparse(t, store, id, body, Range{Start: 8 << 10, End: 64 << 10})

	svc := NewClient(r)
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	c := svc.(*client).outcome(rec, Spec{})
	if c.ResumeFrom != 0 {
		t.Fatalf("ResumeFrom is %d, want 0 — the first hole is at byte zero", c.ResumeFrom)
	}
	if c.Proven != 56<<10 {
		t.Fatalf("Proven is %d, want %d", c.Proven, 56<<10)
	}
	if !strings.Contains(c.Note, "already proven") {
		t.Fatalf("note %q does not say that %d bytes will be skipped", c.Note, c.Proven)
	}
}

// compact strips the indentation a store binding adds when it writes a record.
//
// The design warns about exactly this: inside a record every element is on its
// own line, because that is what all three encoders do by default, and an
// implementation faithful to the inline example in the prose would fail every
// comparison. What is being pinned above is the state and the key order, not
// the whitespace around them.
func compact(t *testing.T, raw []byte) string {
	t.Helper()
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		t.Fatalf("checkpoint is not JSON: %v", err)
	}
	return b.String()
}

// TestAWholeArtifactOnDiskNeedsNoRequest: the plan with no gaps at all.
//
// An owner that filled the last hole and then died leaves a partial that is
// already the whole artifact and a record that has not delivered it. Under a
// prefix this could not arise as anything but "resume from the end", which asks
// a server for byte N of an N-byte file and gets a 416. Ranges can say it
// exactly, and the right answer is to verify what is there and deliver it
// without going near the network.
func TestAWholeArtifactOnDiskNeedsNoRequest(t *testing.T) {
	body, digest := payload(t, 16<<10)
	srv, ranges := recordRanges(t, body)
	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})

	stageSparse(t, store, id, body, Range{Start: 0, End: int64(len(body))})

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := ranges(); len(got) != 0 {
		t.Fatalf("asked the server for %q; the artifact was already whole on disk", got)
	}
	delivered, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(delivered) != string(body) {
		t.Fatalf("delivered %d bytes that are not the artifact", len(delivered))
	}
	rec, _ := store.Load(id)
	if rec.State != job.StateTransferred {
		t.Fatalf("state is %s, want %s", rec.State, job.StateTransferred)
	}
	if rec.Progress.Done != int64(len(body)) {
		t.Fatalf("progress says %d of %d bytes done", rec.Progress.Done, len(body))
	}
}
