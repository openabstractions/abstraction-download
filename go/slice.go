package download

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	job "github.com/openabstractions/abstraction-job/go"
)

// This file is how a caller reads part of an artifact without transferring the
// rest: one member of a remote ZIP, or the header of a twenty-gigabyte model.
//
// It is a decorator over RangeFetcher and not a Fetcher of its own. That is the
// whole design: the capability arrives without a transport arriving with it, so
// it composes with any range-capable fetcher including one that is not ours.
// Everything below is arithmetic on FetchRange.
//
// Measured before it was designed, over 20 real hosts: single ranges are nearly
// universal and multi-range replies are not, so
// nothing here asks for more than one range per request.

var (
	// ErrNotRanged means the source answers whole files. A slice of it is a
	// download of it, so this refuses rather than quietly transferring
	// everything the caller was trying not to transfer.
	ErrNotRanged = forever("download: the source will not serve a bounded range")
	// ErrUnindexable means the format cannot be read in place at all.
	ErrUnindexable = forever("download: this format has no index and cannot be read in place")
	// ErrExpansion means an archive member declared, or produced, more bytes
	// than the caller allowed.
	ErrExpansion = forever("download: archive member expands past the caller's limit")
)

// UnverifiedText is what a partial read has instead of a digest, and it is a
// sentence rather than an absent field on purpose.
//
// A record with no digest and a record whose digest was never checked look
// identical, and only one of them is honest. This layer refuses a pass by
// absence everywhere else; a slice says out loud which check it did not run.
const UnverifiedText = "artifact digest not checked: only part of the artifact was read"

// Slice reads bounded pieces of one source and counts what it cost.
type Slice struct {
	Fetcher    RangeFetcher
	Source     Source
	Headers    map[string]string
	Reach      Reach
	Validators Validators

	mu       sync.Mutex
	size     int64
	sized    bool
	fetched  int64
	requests int
	read     []Range
}

// Size is the artifact's length, and the proof that this source serves ranges
// at all. One byte on the wire.
func (s *Slice) Size(ctx context.Context) (int64, error) {
	s.mu.Lock()
	if s.sized {
		defer s.mu.Unlock()
		return s.size, nil
	}
	s.mu.Unlock()

	n, ranged, err := s.Fetcher.Ranged(ctx, s.Source, s.Headers)
	s.mu.Lock()
	s.requests++
	s.fetched++
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if !ranged {
		return 0, fmt.Errorf("%w: %s", ErrNotRanged, s.Source.Locator)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%w: %s served a range and named no length", ErrNotRanged, s.Source.Locator)
	}
	s.mu.Lock()
	s.size, s.sized = n, true
	s.mu.Unlock()
	return n, nil
}

// Read fetches exactly one range.
func (s *Slice) Read(ctx context.Context, r Range) ([]byte, error) {
	if r.Start < 0 || r.End <= r.Start {
		return nil, fmt.Errorf("download: slice asked for empty range %d-%d", r.Start, r.End)
	}
	buf := make([]byte, r.End-r.Start)
	err := s.Fetcher.FetchRange(ctx, RangeRequest{
		Source:     s.Source,
		Range:      r,
		Out:        &bufferAt{buf: buf, base: r.Start},
		Headers:    s.Headers,
		Reach:      s.Reach,
		Validators: s.Validators,
	})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.fetched += int64(len(buf))
	s.requests++
	s.read = append(s.read, r)
	s.mu.Unlock()
	return buf, nil
}

// Head is the first n bytes: a GGUF's tensor table, or the length-prefixed JSON
// a safetensors file opens with.
func (s *Slice) Head(ctx context.Context, n int64) ([]byte, error) {
	size, err := s.Size(ctx)
	if err != nil {
		return nil, err
	}
	if n > size {
		n = size
	}
	return s.Read(ctx, Range{Start: 0, End: n})
}

// Tail is the last n bytes, asked for as an explicit range and never as
// `bytes=-n`.
//
// Measured rather than assumed: GitHub's release CDN and cdn.kernel.org answer
// a suffix range with 501 Not Implemented and the same bytes named explicitly
// with 206. A ZIP is read from its end, so
// a suffix-only implementation would fail on two hosts that do support ranges.
func (s *Slice) Tail(ctx context.Context, n int64) ([]byte, error) {
	size, err := s.Size(ctx)
	if err != nil {
		return nil, err
	}
	if n > size {
		n = size
	}
	return s.Read(ctx, Range{Start: size - n, End: size})
}

// At binds a context so archive/zip can walk a remote directory. io.ReaderAt
// carries none and the transport needs one.
func (s *Slice) At(ctx context.Context) io.ReaderAt { return &sliceAt{s: s, ctx: ctx} }

// Prefix is a growing view of the front of an artifact, for a header whose
// length is only knowable once part of it has been parsed.
//
// A GGUF carries the tokenizer's entire vocabulary in its metadata block, so
// "the header" is a few hundred kilobytes for one model and several megabytes
// for the next, and the number is in the bytes. A caller that guessed low
// extends; only the bytes it does not already hold are fetched, so a parse
// retried four times still costs one copy of the header.
type Prefix struct {
	s   *Slice
	buf []byte
}

func (s *Slice) Prefix() *Prefix { return &Prefix{s: s} }

func (p *Prefix) Bytes() []byte { return p.buf }

// Grow returns the first n bytes, or everything there is when the artifact is
// shorter.
func (p *Prefix) Grow(ctx context.Context, n int64) ([]byte, error) {
	size, err := p.s.Size(ctx)
	if err != nil {
		return nil, err
	}
	if n > size {
		n = size
	}
	have := int64(len(p.buf))
	if n <= have {
		return p.buf[:n], nil
	}
	more, err := p.s.Read(ctx, Range{Start: have, End: n})
	if err != nil {
		return nil, err
	}
	p.buf = append(p.buf, more...)
	return p.buf, nil
}

// Receipt is what a slice read, and what it left unproven.
type Receipt struct {
	Locator    string     `json:"locator"`
	Size       int64      `json:"size"`
	Read       Ranges     `json:"read"`
	Fetched    int64      `json:"fetched"`
	Requests   int        `json:"requests"`
	Validators Validators `json:"validators"`
	Unverified string     `json:"unverified"`
}

func (s *Slice) Receipt() Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, _ := job.CanonicalRanges(s.read)
	return Receipt{
		Locator:    s.Source.Locator,
		Size:       s.size,
		Read:       rs,
		Fetched:    s.fetched,
		Requests:   s.requests,
		Validators: s.Validators,
		Unverified: UnverifiedText,
	}
}

func (r Receipt) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// A remote ReaderAt needs read-ahead or it is useless, and the number that
// proved it is 2622: one HTTP request per ReadAt, and compress/flate reads a
// member in four-kilobyte bites, so pulling one 43 MB entry out of a GitHub
// release asset took 2622 round trips and six minutes. Random reads — the
// end-of-directory probe, a local file header — must stay cheap, and sequential
// reads must not pay per chunk, so the window starts small and doubles only
// while reads continue where the last one stopped.
const (
	minWindow = 64 << 10
	maxWindow = 4 << 20
)

type sliceAt struct {
	s    *Slice
	ctx  context.Context
	buf  []byte
	base int64
	next int64
	win  int64
}

func (a *sliceAt) ReadAt(p []byte, off int64) (int, error) {
	size, err := a.s.Size(a.ctx)
	if err != nil {
		return 0, err
	}
	if off >= size || off < 0 {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	short := false
	if end > size {
		end, short = size, true
	}
	if err := a.fill(off, end, size); err != nil {
		return 0, err
	}
	n := copy(p, a.buf[off-a.base:end-a.base])
	if short {
		return n, io.EOF
	}
	return n, nil
}

func (a *sliceAt) fill(off, end, size int64) error {
	if a.buf != nil && a.base <= off && end <= a.base+int64(len(a.buf)) {
		return nil
	}
	if off == a.next && a.win > 0 {
		a.win = min(a.win*2, maxWindow)
	} else {
		a.win = minWindow
	}
	stop := min(off+max(end-off, a.win), size)
	buf, err := a.s.Read(a.ctx, Range{Start: off, End: stop})
	if err != nil {
		return err
	}
	a.buf, a.base, a.next = buf, off, stop
	return nil
}

type bufferAt struct {
	buf  []byte
	base int64
}

func (b *bufferAt) WriteAt(p []byte, off int64) (int, error) {
	at := off - b.base
	if at < 0 || at+int64(len(p)) > int64(len(b.buf)) {
		return 0, fmt.Errorf("%w: write at %d outside the requested range", ErrOverrun, off)
	}
	return copy(b.buf[at:], p), nil
}

// Archive is a remote ZIP whose central directory has been read and whose
// members have not.
type Archive struct {
	slice *Slice
	r     *zip.Reader
}

// Zip reads a remote ZIP's central directory and nothing else.
func (s *Slice) Zip(ctx context.Context) (*Archive, error) {
	size, err := s.Size(ctx)
	if err != nil {
		return nil, err
	}
	r, err := zip.NewReader(s.At(ctx), size)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrUnindexable, s.Source.Locator, err)
	}
	return &Archive{slice: s, r: r}, nil
}

func (a *Archive) Slice() *Slice { return a.slice }

// Entries are the members, in the order the directory lists them.
func (a *Archive) Entries() []*zip.File { return a.r.File }

// Member writes one entry to out and fetches only the bytes that entry occupies.
//
// A zip bomb is a member declaring far more than the archive could hold, and
// limit is what the caller will accept. Three guards, and each catches a
// different lie:
//
//   - The declared size is checked BEFORE anything is fetched, so a kilobyte
//     archive claiming a ten-gigabyte member costs no network at all.
//   - archive/zip refuses a member whose stream runs past the size its own
//     directory declared, on the read that passes it — so the bytes reaching
//     out are bounded by the number the first guard already approved. That is
//     load bearing and it is somebody else's code, which is why
//     TestZipBombRefusedMidStream exists: it fails if that ever stops being
//     true. Reported as ErrExpansion because in a member body a size
//     disagreement is the bomb and nothing else.
//   - The limit is enforced again on what actually arrives, so a future
//     archive/zip that stopped checking would be caught here rather than
//     believed.
func (a *Archive) Member(ctx context.Context, name string, out io.Writer, limit int64) (int64, error) {
	f, err := a.find(name)
	if err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, fmt.Errorf("%w: %s needs a byte limit", ErrExpansion, name)
	}
	if f.UncompressedSize64 > uint64(limit) {
		return 0, fmt.Errorf("%w: %s declares %d bytes, limit is %d", ErrExpansion, name, f.UncompressedSize64, limit)
	}
	rc, err := f.Open()
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	n, err := io.Copy(out, io.LimitReader(rc, limit+1))
	if n > limit {
		return n, fmt.Errorf("%w: %s ran past %d bytes", ErrExpansion, name, limit)
	}
	if errors.Is(err, zip.ErrFormat) {
		return n, fmt.Errorf("%w: %s ran past the %d bytes it declared", ErrExpansion, name, f.UncompressedSize64)
	}
	return n, err
}

func (a *Archive) find(name string) (*zip.File, error) {
	for _, f := range a.r.File {
		if f.Name == name {
			return f, nil
		}
	}
	return nil, fmt.Errorf("%w: %s has no member %q", ErrRefused, a.slice.Source.Locator, name)
}

// MemberPath is the relative path an archive member may be written to, or a
// refusal.
//
// An entry name is chosen by whoever built the archive, which makes it a sink
// path arriving by another route, and it answers to the containment the record's
// sink already answers to. Three ways out of a directory and all three refused:
// `..`, an absolute or drive-qualified name, and a name the store keeps for its
// own files. owner is the job the extraction belongs to; see ReservedSink.
//
// A trailing slash is a directory entry, and it is checked rather than skipped:
// `../evil/` is the same escape and it is the entry a caller creates
// directories from. The slash is not returned — a caller that wants to know
// which entries are directories asks the archive, not this.
func MemberPath(owner, name string) (string, error) {
	clean := strings.TrimSuffix(strings.ReplaceAll(name, `\`, "/"), "/")
	if clean == "" {
		return "", fmt.Errorf("%w: %q names nothing", ErrEscapesRoot, name)
	}
	if !relativeEverywhere(clean) {
		return "", fmt.Errorf("%w: %s", ErrEscapesRoot, name)
	}
	if err := EscapesRoot(clean); err != nil {
		return "", err
	}
	if err := ReservedSink(owner, clean); err != nil {
		return "", err
	}
	return path.Clean(clean), nil
}

// Indexable says whether a locator's format can be read in place, and refuses
// to guess.
//
// The NO half is the point. A .tar.gz has no index, so one member of one costs
// the whole file, and a layer that fetched it anyway and called the result a
// slice would be lying about what it saved. Decided on the extension because
// that is all a locator offers before a byte is fetched.
func Indexable(locator string) error {
	name := strings.ToLower(path.Base(locator))
	if i := strings.IndexAny(name, "?#"); i >= 0 {
		name = name[:i]
	}
	for _, ext := range zipShaped {
		if strings.HasSuffix(name, ext) {
			return nil
		}
	}
	for _, no := range notIndexable {
		if strings.HasSuffix(name, no.ext) {
			return fmt.Errorf("%w: %s: %s", ErrUnindexable, name, no.why)
		}
	}
	return fmt.Errorf("%w: %s: no index is known for this extension", ErrUnindexable, name)
}

// zipShaped are the formats that are a ZIP under another name, and a ZIP keeps
// its directory at the end where one range reaches it.
var zipShaped = []string{
	".zip", ".jar", ".war", ".ear", ".aar", ".whl", ".egg", ".apk", ".xpi",
	".nupkg", ".crx", ".epub", ".docx", ".xlsx", ".pptx", ".odt", ".ods", ".odp",
}

var notIndexable = []struct{ ext, why string }{
	{".tar.gz", "a gzip stream has no index; one member costs the whole file"},
	{".tgz", "a gzip stream has no index; one member costs the whole file"},
	{".tar.bz2", "a bzip2 stream has no index; one member costs the whole file"},
	{".tar.xz", "an xz stream is only seekable when written in blocks, and nothing records which"},
	{".tar.zst", "zstd is only seekable in the seekable-frame format, and nothing records which"},
	{".gz", "a gzip stream has no index"},
	{".zst", "zstd is only seekable in the seekable-frame format"},
	{".xz", "an xz stream is only seekable when written in blocks"},
	{".bz2", "a bzip2 stream has no index"},
	{".7z", "7z keeps its header at the end and no standard library reads it"},
	{".rar", "no standard library reads rar"},
	{".tar", "a tar has no directory; a member is found by reading every header before it, one round trip each"},
	{".gguf", "a model file is not an archive; read its header with Head"},
	{".safetensors", "a model file is not an archive; read its header with Head"},
}
