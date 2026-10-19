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

func TestFileAndSMBSourcesNameNoValidators(t *testing.T) {
	for _, scheme := range []string{"file", "smb"} {
		t.Run(scheme, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source.bin")
			if err := os.WriteFile(path, []byte("bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			src := Source{Scheme: scheme, Locator: path}
			var out bytes.Buffer
			observed := false
			if _, err := (File{}).Fetch(context.Background(), Request{Source: src, Out: &out, Observed: func(Validators) { observed = true }}); err != nil {
				t.Fatal(err)
			}
			if out.String() != "bytes" || observed {
				t.Fatal("file source named HTTP validators or changed bytes")
			}
			part, err := os.CreateTemp(t.TempDir(), "range-")
			if err != nil {
				t.Fatal(err)
			}
			defer part.Close()
			if err := (File{}).FetchRange(context.Background(), RangeRequest{Source: src, Range: job.Range{Start: 0, End: 5}, Out: part, Observed: func(Validators) error { observed = true; return nil }}); err != nil {
				t.Fatal(err)
			}
			if observed {
				t.Fatal("file range named HTTP validators")
			}
		})
	}
}

func TestSingleStreamScopesValidatorsToTheirSource(t *testing.T) {
	for _, mode := range []string{"same", "other", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			body := []byte(strings.Repeat("S", 4096))
			var mu sync.Mutex
			var firstRange, firstIfRange string
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				if calls == 0 {
					firstRange = r.Header.Get("Range")
					firstIfRange = r.Header.Get("If-Range")
				}
				calls++
				mu.Unlock()
				etag := `"a"`
				if mode == "other" {
					etag = `"b"`
				}
				w.Header().Set("ETag", etag)
				from := 0
				if rg := r.Header.Get("Range"); rg != "" && (r.Header.Get("If-Range") == "" || r.Header.Get("If-Range") == etag) {
					_, _ = fmt.Sscanf(rg, "bytes=%d-", &from)
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, len(body)-1, len(body)))
					w.WriteHeader(http.StatusPartialContent)
				}
				_, _ = w.Write(body[from:])
			}))
			t.Cleanup(srv.Close)
			r, store, root := newRunner(t)
			r.Connections = 1
			id := submit(t, store, root, "", int64(len(body)), Source{Scheme: "https", Locator: srv.URL})
			v := Validators{ETag: `"a"`, Source: srv.URL}
			want := v.ETag
			if mode == "other" {
				v.Source = "https://previous.invalid/artifact"
				want = ""
			}
			if mode == "legacy" {
				v.Source = ""
			}
			stageResume(t, store, id, body[:1024], v)
			if err := r.Run(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if firstRange != "bytes=1024-" || firstIfRange != want {
				t.Fatalf("first request Range=%q If-Range=%q; want bytes=1024- and %q", firstRange, firstIfRange, want)
			}
			got, err := os.ReadFile(finalOf(t, store, id))
			if err != nil || string(got) != string(body) {
				t.Fatalf("delivered bytes: %v", err)
			}
		})
	}
}

// stageResume leaves what a killed owner leaves: bytes on disk, a checkpoint
// saying how many of them were proven, and the validators that say which
// version of the artifact they came from.
//
// stageDeadOwner does the first two. This one exists because the validators are
// the whole subject here, and a staged resume without them tests a record no
// current owner would ever write.
func stageResume(t *testing.T, store job.Store, id string, prefix []byte, v Validators) {
	t.Helper()
	if err := os.WriteFile(partialOf(t, store, id), prefix, 0o644); err != nil {
		t.Fatal(err)
	}
	n := int64(len(prefix))
	stageAbandonedProgress(t, store, id, "dead-owner", func(rr *job.Record) error {
		rr.Progress.Done = n
		return setCheckpoint(rr, Checkpoint{VerifiedPrefix: n, Validators: v})
	})

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if cp.VerifiedPrefix != n || cp.Validators != v {
		t.Fatalf("staging did not take: checkpoint is %+v", cp)
	}
}

// versionedServer serves whatever body it currently holds under an ETag, and
// behaves the way RFC 7233 says a server should: a Range with an If-Range that
// does not match the current entity is answered with the whole current entity
// and a 200, not with a range of it.
type versionedServer struct {
	mu       sync.Mutex
	body     []byte
	etag     string
	requests []*http.Request
}

func (s *versionedServer) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		body, etag := s.body, s.etag
		s.requests = append(s.requests, r.Clone(context.Background()))
		s.mu.Unlock()

		w.Header().Set("ETag", etag)
		w.Header().Set("Accept-Ranges", "bytes")
		if ifRange := r.Header.Get("If-Range"); ifRange != "" && ifRange != etag {
			// The client is holding bytes from a version this is no longer
			// serving. Give it the current one, whole.
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			return
		}
		http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), strings.NewReader(string(body)))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (s *versionedServer) seen() []*http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*http.Request(nil), s.requests...)
}

func (s *versionedServer) set(body []byte, etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body, s.etag = body, etag
}

// TestAChangedFileIsNotSplicedOntoTheOldOne is the bug this file exists for.
//
// The artifact changed between attempts and the new version is the same length
// as the old, so nothing about the resulting file's SIZE can reveal what
// happened. The download carries no digest either, which is the ordinary case
// for `dl <url>`. The only thing that can catch it is the exchange itself: the
// resume says which version it is continuing, the server says that is not the
// version it has, and the client starts again.
//
// Asserted on content. A length assertion passes on the corrupt file.
func TestManagedDownloadReserveRefusalClearsChangedSourceCheckpoint(t *testing.T) {
	r, store, _ := newRunner(t)
	old, current := []byte(strings.Repeat("A", 4096)), []byte(strings.Repeat("B", 4096))
	server := &versionedServer{body: current, etag: `"v2"`}
	id := job.NewID()
	source := Source{Scheme: "https", Locator: server.serve(t).URL}
	_, err := submitAs(store, id, Spec{Artifact: Artifact{Size: int64(len(current))}, Sources: []Source{source}, Sink: Sink{Partial: "work/" + id, Final: "results/" + id}})
	if err != nil {
		t.Fatal(err)
	}
	stageResume(t, store, id, old[:1024], Validators{ETag: `"v1"`, Source: sourceKey(source)})
	var checked []int64
	r.CheckWriteSpace = func(_ string, additional int64) error {
		checked = append(checked, additional)
		if additional == int64(len(current)) {
			return Terminal(ErrRefused)
		}
		return nil
	}
	if err := r.Run(t.Context(), id); !errors.Is(err, ErrRefused) {
		t.Fatal(err)
	}
	if len(checked) != 2 || checked[0] != 3072 || checked[1] != 4096 {
		t.Fatal("unexpected space checks", checked)
	}
	record, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := CheckpointOf(record)
	if err != nil || record.Progress.Done != 0 || checkpoint.VerifiedPrefix != 0 || !checkpoint.Validators.Empty() {
		t.Fatal("space refusal retained obsolete progress", record.Progress, checkpoint, err)
	}
	if info, err := os.Stat(partialOf(t, store, id)); !errors.Is(err, os.ErrNotExist) && (err != nil || info.Size() != 0) {
		t.Fatal("replacement bytes reached disk", err)
	}
}

func TestAChangedFileIsNotSplicedOntoTheOldOne(t *testing.T) {
	v1 := []byte(strings.Repeat("A", 40<<10))
	v2 := []byte(strings.Repeat("B", 40<<10))
	srv := &versionedServer{}
	srv.set(v2, `"v2"`) // it has already changed by the time we resume
	url := srv.serve(t).URL

	r, store, root := newRunner(t)
	id := submit(t, store, root, "", 0, Source{Scheme: "https", Locator: url})
	stageResume(t, store, id, v1[:1000], Validators{ETag: `"v1"`})

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(v2) {
		t.Fatalf("delivered file is not the current artifact: %d bytes, first byte %q, byte 1000 %q — "+
			"the old prefix was spliced onto the new file", len(got), got[0], got[1000])
	}

	// And the record now describes what was actually delivered, so the NEXT
	// resume continues v2 rather than v1.
	rec, _ := store.Load(id)
	cp, _ := CheckpointOf(rec)
	if cp.VerifiedPrefix != int64(len(v2)) {
		t.Fatalf("checkpoint says %d bytes are proven, want %d", cp.VerifiedPrefix, len(v2))
	}
	if cp.Validators.ETag != `"v2"` {
		t.Fatalf("checkpoint validators are %+v, want the ETag of the version delivered", cp.Validators)
	}
}

// TestResumeSendsIfRange: the header that makes the case above possible is
// actually on the wire, carrying the stored ETag, and it does not stop the
// resume being a resume when the file has NOT changed.
func TestResumeSendsIfRange(t *testing.T) {
	body, digest := payload(t, 40<<10)
	srv := &versionedServer{}
	srv.set(body, `"v1"`)
	url := srv.serve(t).URL

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: url})
	stageResume(t, store, id, body[:12<<10], Validators{ETag: `"v1"`})

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reqs := srv.seen()
	if len(reqs) != 1 {
		t.Fatalf("%d requests, want 1: an unchanged file needs no restart", len(reqs))
	}
	last := reqs[0]
	if got, want := last.Header.Get("Range"), "bytes=12288-"; got != want {
		t.Fatalf("Range = %q, want %q", got, want)
	}
	if got, want := last.Header.Get("If-Range"), `"v1"`; got != want {
		t.Fatalf("If-Range = %q, want %q — a resume that does not say which version it is continuing "+
			"invites a range of a different file", got, want)
	}
	// Byte offsets are counted after decoding here and before it at the server,
	// so a ranged request must not be answered with a compressed body.
	if got := last.Header.Get("Accept-Encoding"); got != "identity" {
		t.Fatalf("Accept-Encoding = %q on a ranged request, want %q", got, "identity")
	}
	got, _ := os.ReadFile(finalOf(t, store, id))
	if string(got) != string(body) {
		t.Fatal("the resumed file does not match the source")
	}
}

// TestAWeakETagIsNeitherStoredNorUsed. `W/"..."` means the server is asserting
// that two responses are equivalent, not that they are byte-identical — which
// is exactly the distinction a byte-range resume depends on. Recording one
// would buy an If-Range that a changed file could still satisfy: false
// confidence, worse than none.
func TestAWeakETagIsNeitherStoredNorUsed(t *testing.T) {
	body, digest := payload(t, 8<<10)
	const lastModified = "Wed, 21 Oct 2015 07:28:00 GMT"

	var seen []*http.Request
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Clone(context.Background()))
		mu.Unlock()
		w.Header().Set("ETag", `W/"weak"`)
		w.Header().Set("Last-Modified", lastModified)
		http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), strings.NewReader(string(body)))
	}))
	t.Cleanup(srv.Close)

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rec, _ := store.Load(id)
	cp, _ := CheckpointOf(rec)
	if cp.Validators.ETag != "" {
		t.Fatalf("a weak ETag was recorded: %+v", cp.Validators)
	}
	if cp.Validators.LastModified != lastModified {
		t.Fatalf("Last-Modified was not recorded in place of the unusable ETag: %+v", cp.Validators)
	}

	// And on the wire: whatever a resume sends, it is never the weak tag.
	//
	// The first download's file would otherwise be copied for this second
	// request of the same bytes — that is the already-here dedup [DL-R28] doing
	// exactly its job — and no request would reach the server at all. Remove it,
	// so this half exercises the resume-over-network path it is about.
	os.Remove(filepath.Join(root, "final.bin"))
	second := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})
	stageResume(t, store, second, body[:2<<10], cp.Validators)
	if err := r.Run(context.Background(), second); err != nil {
		t.Fatalf("Run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, req := range seen {
		if v := req.Header.Get("If-Range"); strings.Contains(strings.ToLower(v), "w/") {
			t.Fatalf("If-Range carried a weak validator: %q", v)
		}
	}
	if got := seen[len(seen)-1].Header.Get("If-Range"); got != lastModified {
		t.Fatalf("If-Range = %q, want the Last-Modified %q", got, lastModified)
	}
}

// TestStrongValidators is the same rule at the unit it lives in, including the
// forms a real server writes.
func TestStrongValidators(t *testing.T) {
	const lm = "Wed, 21 Oct 2015 07:28:00 GMT"
	const later = "Thu, 22 Oct 2015 07:28:00 GMT"
	cases := []struct {
		name   string
		header http.Header
		want   Validators
	}{
		{"strong etag wins", http.Header{"Etag": {`"abc"`}, "Last-Modified": {lm}, "Date": {later}}, Validators{ETag: `"abc"`}},
		{"weak etag is dropped", http.Header{"Etag": {`W/"abc"`}, "Last-Modified": {lm}, "Date": {later}}, Validators{LastModified: lm}},
		// RFC 9110 8.8.2.2: two versions written within one second share a
		// Last-Modified, so only one at least a second before Date names one file.
		{"last-modified one second before date is strong", http.Header{"Last-Modified": {lm}, "Date": {"Wed, 21 Oct 2015 07:28:01 GMT"}}, Validators{LastModified: lm}},
		{"last-modified in the same second as date is weak", http.Header{"Last-Modified": {lm}, "Date": {lm}}, Validators{LastModified: lm, Weak: true}},
		{"last-modified after date is weak", http.Header{"Last-Modified": {later}, "Date": {lm}}, Validators{LastModified: later, Weak: true}},
		{"last-modified without date is weak", http.Header{"Last-Modified": {lm}}, Validators{LastModified: lm, Weak: true}},
		{"obsolete spellings compare too", http.Header{"Last-Modified": {"Sunday, 06-Nov-94 08:49:37 GMT"}, "Date": {"Sun Nov  6 08:49:38 1994"}}, Validators{LastModified: "Sunday, 06-Nov-94 08:49:37 GMT"}},
		{"weak in lower case too", http.Header{"Etag": {`w/"abc"`}}, Validators{}},
		{"unquoted is not an etag", http.Header{"Etag": {`abc`}}, Validators{}},
		{"a date that is not a date", http.Header{"Last-Modified": {"yesterday"}}, Validators{}},
		{"nothing at all", http.Header{}, Validators{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := StrongValidators(c.header); got != c.want {
				t.Fatalf("StrongValidators = %+v, want %+v", got, c.want)
			}
		})
	}
	if got := (Validators{ETag: `"a"`, LastModified: lm}).IfRange(); got != `"a"` {
		t.Fatalf("IfRange preferred %q over the ETag", got)
	}
	if got := (Validators{LastModified: lm}).IfRange(); got != lm {
		t.Fatalf("IfRange = %q, want the Last-Modified", got)
	}
	if !(Validators{}).Empty() {
		t.Fatal("an empty Validators does not report itself empty")
	}
}

// TestA206AtTheWrongOffsetRestarts: the status says partial content and the
// Content-Range says it begins somewhere other than where we asked. Those bytes
// are the artifact's, but they belong at an offset nobody asked about, so
// writing them where the request expected them puts real content in the wrong
// place — a corruption no length check and no transport error can see.
func TestA206AtTheWrongOffsetRestarts(t *testing.T) {
	body, digest := payload(t, 32<<10)
	var mu sync.Mutex
	var ranges []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()
		if r.Header.Get("Range") == "" {
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			return
		}
		// Asked for bytes from N; answers with the whole file and calls it a
		// range starting at zero. Servers behind rewriting proxies do this.
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(body)
	}))
	t.Cleanup(srv.Close)

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})
	stageResume(t, store, id, body[:4<<10], Validators{ETag: `"v1"`})

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatalf("delivered %d bytes that are not the artifact; a misplaced range was trusted", len(got))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ranges) != 2 || ranges[0] == "" || ranges[1] != "" {
		t.Fatalf("requests asked for %q; want a ranged one and then a whole-file restart", ranges)
	}
}

// TestA416Restarts: the stored offset is past the end of what the server now
// holds. Left alone the checkpoint produces the same 416 on every retry, so the
// job can never finish without somebody deleting the partial by hand.
func TestA416Restarts(t *testing.T) {
	body, digest := payload(t, 8<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(body)))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	t.Cleanup(srv.Close)

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})
	// A prefix longer than the artifact the server now serves.
	stageResume(t, store, id, make([]byte, 16<<10), Validators{ETag: `"v1"`})

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, _ := os.ReadFile(finalOf(t, store, id))
	if string(got) != string(body) {
		t.Fatalf("delivered %d bytes, want the %d-byte artifact", len(got), len(body))
	}
}

// TestContentRangeStart covers the forms the header arrives in, including the
// ones that must not be read as a starting byte.
func TestContentRangeStart(t *testing.T) {
	ok := map[string]int64{
		"bytes 0-99/100":       0,
		"bytes 1000-40959/*":   1000,
		"  bytes 12-99/100  ":  12,
		"BYTES 7-99/100":       7,
		"bytes 500-999/123456": 500,
	}
	for in, want := range ok {
		got, err := contentRangeStart(in)
		if err != nil || got != want {
			t.Fatalf("contentRangeStart(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "items 0-99/100", "bytes */100", "bytes 0-99", "bytes x-99/100", "bytes -99/100"} {
		if got, err := contentRangeStart(in); err == nil {
			t.Fatalf("contentRangeStart(%q) = %d with no error", in, got)
		}
	}
}

// TestResumeAt is what used to be the three-way test, kept exactly as it was
// asserted before ranges existed: a prefix checkpoint against a file ahead of
// it, level with it, behind it, and gone.
//
// The rule underneath it is not the same rule any more — see planResume — and
// that is why this test still has to pass unchanged. "Longer" and "equal" now
// reach the same branch rather than two, and the number that decides is the
// highest proven offset rather than the checkpoint's prefix. For a prefix those
// are the same number, so every answer below has to be identical, and if one of
// them moved then the replacement broke something the replacement was supposed
// to preserve.
func TestResumeAt(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, n int) string {
		p := dir + string(os.PathSeparator) + name
		if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	v := Validators{ETag: `"v1"`}

	rp, err := planResume(write("ahead.bin", 1500), Checkpoint{VerifiedPrefix: 1000, Validators: v}, 0)
	if err != nil || rp.From() != 1000 || rp.Discarded != 500 || rp.Validators != v {
		t.Fatalf("ahead: %+v, %v", rp, err)
	}
	if rp.Trim != 1000 {
		t.Fatalf("ahead: trimmed to %d, want the highest proven offset 1000", rp.Trim)
	}
	rp, err = planResume(write("equal.bin", 1000), Checkpoint{VerifiedPrefix: 1000}, 0)
	if err != nil || rp.From() != 1000 || rp.Discarded != 0 {
		t.Fatalf("equal: %+v, %v", rp, err)
	}
	rp, err = planResume(write("short.bin", 900), Checkpoint{VerifiedPrefix: 1000}, 0)
	if err != nil || rp.From() != 900 || rp.Discarded != 0 {
		t.Fatalf("short: %+v, %v — the file is the floor, so 900 of the claimed 1000 stand", rp, err)
	}
	// Missing is not short: there is no prefix to disbelieve, so this starts
	// over rather than failing.
	if rp, err = planResume(dir+string(os.PathSeparator)+"gone.bin", Checkpoint{VerifiedPrefix: 1000}, 0); err != nil || rp.From() != 0 {
		t.Fatalf("missing file: %+v, %v", rp, err)
	}
	// No checkpoint, no questions: a first attempt over whatever is lying there.
	if rp, err = planResume(dir+string(os.PathSeparator)+"gone.bin", Checkpoint{}, 0); err != nil || rp.From() != 0 {
		t.Fatalf("empty checkpoint: %+v, %v", rp, err)
	}
}
