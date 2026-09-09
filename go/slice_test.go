package download

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// served counts what the server actually put on the wire, which is the only
// number this feature is about. A test that asserted on Slice.Fetched alone
// would pass on an implementation that fetched everything and returned a slice.
type served struct {
	body []byte
	sent atomic.Int64
	reqs atomic.Int64
}

func (s *served) start(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.reqs.Add(1)
		rec := &countingWriter{ResponseWriter: w, n: &s.sent}
		http.ServeContent(rec, r, "artifact.bin", time.Unix(0, 0), bytes.NewReader(s.body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

type countingWriter struct {
	http.ResponseWriter
	n *atomic.Int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.n.Add(int64(n))
	return n, err
}

func sliceOf(t *testing.T, body []byte) (*Slice, *served) {
	t.Helper()
	s := &served{body: body}
	url := s.start(t)
	return &Slice{Fetcher: HTTP{}, Source: Source{Scheme: "https", Locator: url}}, s
}

func TestHeadFetchesTheHeadAndNotTheFile(t *testing.T) {
	body := bytes.Repeat([]byte("m"), 8<<20)
	copy(body, "GGUF")
	sl, srv := sliceOf(t, body)

	head, err := sl.Head(context.Background(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(head) != 4096 || string(head[:4]) != "GGUF" {
		t.Fatalf("head is %d bytes starting %q", len(head), head[:4])
	}
	if sent := srv.sent.Load(); sent > 8192 {
		t.Fatalf("server sent %d bytes for a 4 KiB head of an 8 MiB file", sent)
	}
}

func TestSliceRefusesASourceThatServesWholeFiles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "9")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("wholefile"))
	}))
	defer srv.Close()

	sl := &Slice{Fetcher: HTTP{}, Source: Source{Scheme: "https", Locator: srv.URL}}
	_, err := sl.Head(context.Background(), 4)
	if !errors.Is(err, ErrNotRanged) {
		t.Fatalf("want ErrNotRanged, got %v", err)
	}
	if !Permanent(err) {
		t.Fatal("a source that cannot serve a range will not start serving one")
	}
}

// zipWith builds an archive whose members are named and whose contents are the
// given sizes, so a test can ask for the small one out of the middle.
func zipWith(t *testing.T, members map[string][]byte, order []string) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	w := zip.NewWriter(buf)
	for _, name := range order {
		f, err := w.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(members[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestZipMemberFetchesOnlyThatMember(t *testing.T) {
	big := make([]byte, 4<<20)
	for i := range big {
		big[i] = byte(i * 7)
	}
	want := []byte("the one file anybody wanted")
	archive := zipWith(t, map[string][]byte{
		"noise/a.bin": big,
		"wanted.txt":  want,
		"noise/b.bin": big,
	}, []string{"noise/a.bin", "wanted.txt", "noise/b.bin"})

	sl, srv := sliceOf(t, archive)
	a, err := sl.Zip(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Entries()) != 3 {
		t.Fatalf("directory lists %d entries", len(a.Entries()))
	}
	out := &bytes.Buffer{}
	n, err := a.Member(context.Background(), "wanted.txt", out, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(want)) || !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("got %q", out.Bytes())
	}
	sent := srv.sent.Load()
	if sent > 1<<16 {
		t.Fatalf("server sent %d bytes of a %d byte archive for one 27 byte member", sent, len(archive))
	}
	if r := sl.Receipt(); r.Unverified == "" || r.Fetched != sent {
		t.Fatalf("receipt %s does not match the %d bytes the server sent", r, sent)
	}
}

func TestZipBombRefusedBeforeAnyBytesAreFetched(t *testing.T) {
	archive := zipWith(t, map[string][]byte{"bomb": make([]byte, 16<<20)}, []string{"bomb"})
	if len(archive) > 1<<16 {
		t.Fatalf("the bomb did not compress: %d bytes", len(archive))
	}
	sl, srv := sliceOf(t, archive)
	a, err := sl.Zip(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	before := srv.sent.Load()
	_, err = a.Member(context.Background(), "bomb", io.Discard, 1<<20)
	if !errors.Is(err, ErrExpansion) {
		t.Fatalf("want ErrExpansion, got %v", err)
	}
	if srv.sent.Load() != before {
		t.Fatal("the declared size was refused after fetching bytes for it")
	}
}

// TestZipBombRefusedMidStream is the case the declared size cannot catch: the
// central directory is patched to under-report, so the cheap check passes and
// only the arriving bytes give it away.
//
// It also pins somebody else's behaviour. archive/zip stops the member on the
// read that passes the declared size, which is why Member's limit can be
// enforced against a declared number at all. If that ever changes this test
// goes red before an adopter finds out.
func TestZipBombRefusedMidStream(t *testing.T) {
	archive := zipWith(t, map[string][]byte{"bomb": make([]byte, 16<<20)}, []string{"bomb"})
	cen := bytes.Index(archive, []byte("PK\x01\x02"))
	if cen < 0 {
		t.Fatal("no central directory")
	}
	binary.LittleEndian.PutUint32(archive[cen+24:], 1024)

	sl, _ := sliceOf(t, archive)
	a, err := sl.Zip(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := a.Entries()[0].UncompressedSize64; got != 1024 {
		t.Fatalf("the archive should be lying about 1024 bytes, it says %d", got)
	}
	out := &bytes.Buffer{}
	n, err := a.Member(context.Background(), "bomb", out, 1<<20)
	if !errors.Is(err, ErrExpansion) {
		t.Fatalf("want ErrExpansion, got %v", err)
	}
	if n > 1024 || out.Len() > 1024 {
		t.Fatalf("%d bytes of a 16 MiB member reached the caller; it declared 1024", out.Len())
	}
}

func TestMemberPathContainment(t *testing.T) {
	refused := []string{
		"../evil",
		"../../etc/cron.d/evil",
		`..\..\evil`,
		"a/../../b",
		"/etc/passwd",
		`\\nas\share\evil`,
		`C:\Windows\evil`,
		"C:relative",
		"",
		"/",
		"../",
		`..\..\evil\`,
		"work/someone-else/partial",
	}
	for _, name := range refused {
		if _, err := MemberPath("mine", name); err == nil {
			t.Fatalf("member %q was accepted", name)
		}
	}
	allowed := map[string]string{
		"a/b.txt":       "a/b.txt",
		"./a.txt":       "a.txt",
		"a/./b/../c.md": "a/c.md",
		`dir\file.txt`:  "dir/file.txt",
		"META-INF/":     "META-INF",
	}
	for name, want := range allowed {
		got, err := MemberPath("mine", name)
		if err != nil || got != want {
			t.Fatalf("member %q gave %q, %v", name, got, err)
		}
	}
}

// TestMemberPathRefusesWhatTheSinkRefuses ties the two together: a member name
// is a sink path arriving by another route, so anything the record's own sink
// check refuses must be refused here too.
func TestMemberPathRefusesWhatTheSinkRefuses(t *testing.T) {
	for _, name := range []string{"../out", "a/../../out", "work/other/x"} {
		_, sinkErr := MemberPath("mine", name)
		byRecord := EscapesRoot(name)
		if byRecord == nil {
			byRecord = ReservedSink("mine", name)
		}
		if (sinkErr == nil) != (byRecord == nil) {
			t.Fatalf("%q: member says %v, sink says %v", name, sinkErr, byRecord)
		}
	}
}

func TestIndexableSaysWhichFormatsCannot(t *testing.T) {
	for _, ok := range []string{"a.zip", "https://x/y/pkg-1.0-py3-none-any.whl", "slf4j.jar?x=1"} {
		if err := Indexable(ok); err != nil {
			t.Fatalf("%s: %v", ok, err)
		}
	}
	for _, no := range []string{"a.tar.gz", "a.tgz", "a.tar.zst", "linux.tar.xz", "m.gguf", "m.safetensors", "a.7z", "a.tar"} {
		err := Indexable(no)
		if !errors.Is(err, ErrUnindexable) {
			t.Fatalf("%s was called indexable: %v", no, err)
		}
		if !strings.Contains(err.Error(), ":") {
			t.Fatalf("%s refused without a reason: %v", no, err)
		}
	}
}

func TestReceiptNamesWhatWasNotChecked(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 1<<20)
	sl, _ := sliceOf(t, body)
	if _, err := sl.Head(context.Background(), 512); err != nil {
		t.Fatal(err)
	}
	if _, err := sl.Tail(context.Background(), 512); err != nil {
		t.Fatal(err)
	}
	r := sl.Receipt()
	if r.Unverified != UnverifiedText {
		t.Fatalf("receipt does not say the digest went unchecked: %s", r)
	}
	want := Ranges{job.Range{Start: 0, End: 512}, job.Range{Start: int64(len(body)) - 512, End: int64(len(body))}}
	if len(r.Read) != len(want) || r.Read[0] != want[0] || r.Read[1] != want[1] {
		t.Fatalf("receipt read %v, want %v", r.Read, want)
	}
	if r.Size != int64(len(body)) {
		t.Fatalf("receipt size %d", r.Size)
	}
}

// TestTailAsksExplicitlyRatherThanWithASuffixRange pins the measurement:
// two real hosts answer `bytes=-n` with 501 and the same bytes named explicitly
// with 206, so this must never send a suffix range.
func TestTailAsksExplicitlyRatherThanWithASuffixRange(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Range"))
		if strings.HasPrefix(r.Header.Get("Range"), "bytes=-") {
			w.WriteHeader(http.StatusNotImplemented)
			return
		}
		http.ServeContent(w, r, "a.bin", time.Unix(0, 0), bytes.NewReader(bytes.Repeat([]byte("z"), 4096)))
	}))
	defer srv.Close()

	sl := &Slice{Fetcher: HTTP{}, Source: Source{Scheme: "https", Locator: srv.URL}}
	tail, err := sl.Tail(context.Background(), 100)
	if err != nil || len(tail) != 100 {
		t.Fatalf("tail of %d bytes: %v", len(tail), err)
	}
	for _, r := range seen {
		if strings.HasPrefix(r, "bytes=-") {
			t.Fatalf("a suffix range went out: %q", r)
		}
	}
}
