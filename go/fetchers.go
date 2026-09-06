package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// reportWriter counts bytes on their way through and tells the runner. Counting
// here rather than in each Fetcher means every implementation reports progress
// the same way, including ones that have no progress API of their own — which is
// most of them.
type reportWriter struct {
	w       io.Writer
	n       int64
	total   int64 // the artifact's full size, if this source disclosed one
	report  func(written, total int64)
	lastRep time.Time
}

func (rw *reportWriter) Write(p []byte) (int, error) {
	n, err := rw.w.Write(p)
	rw.n += int64(n)
	if rw.report != nil && time.Since(rw.lastRep) > 100*time.Millisecond {
		rw.lastRep = time.Now()
		rw.report(rw.n, rw.total)
	}
	return n, err
}

// HTTP fetches over http and https.
//
// This is the console tier from https://github.com/openabstractions/research/blob/main/transfer/SUMMARY.txt, and the survey's
// conclusion was to write it rather than adopt: aria2 is GPL-2.0, which this
// project will not take, and curl bought nothing from Go because net/http
// already does ranges, redirects, proxies and TLS with no CGO. What was worth
// stealing was the lesson, not the dependency — curl's CURLOPT_RESUME_FROM (the
// 32-bit one) silently breaks past 2 GB, so every offset here is int64.
type HTTP struct {
	Client *http.Client
}

func (h HTTP) Schemes() []string { return []string{"http", "https"} }

func (h HTTP) Capabilities() []Capability {
	// Deliberately not CapSurvivesProcessExit: this runs inside the caller's
	// process and dies with it. That is precisely the gap the Windows service
	// tier fills, and pretending otherwise here would hide it.
	return []Capability{CapResume}
}

func (h HTTP) client() *http.Client {
	if h.Client != nil {
		return h.Client
	}
	return http.DefaultClient
}

// get issues the GET, ranged when there is a prefix to continue.
func (h HTTP) get(ctx context.Context, req Request) (*http.Response, error) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.Source.Locator, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}
	if req.From > 0 || req.To > 0 {
		// Bounded when there are proven bytes on the far side of this gap,
		// open-ended when there are not. `bytes=N-` is the request a
		// single-stream resume has always sent and still sends, byte for byte;
		// `bytes=N-M` is the one that makes a resume skip a hole instead of
		// re-fetching everything after it. The last byte position in HTTP is
		// INCLUSIVE and To is exclusive, so it goes out as To-1 — an off-by-one
		// here would fetch one byte too few and leave a one-byte hole that
		// only a digest would ever catch.
		byteRange := strconv.FormatInt(req.From, 10) + "-"
		if req.To > req.From {
			byteRange += strconv.FormatInt(req.To-1, 10)
		}
		hreq.Header.Set("Range", "bytes="+byteRange)

		// Say which version the bytes on disk came from. Without this a server
		// whose file has changed answers the range honestly, with a valid range
		// of a DIFFERENT file, and nothing in the response says so. With it, the
		// server answers 206 only if the file is unchanged and 200 — the whole
		// new file — if it is not, which is the case Fetch handles by starting
		// again.
		if v := req.Validators.IfRange(); v != "" {
			hreq.Header.Set("If-Range", v)
		}
	}

	// On every request, not only the ranged ones. Offsets on this side are
	// counted after decoding and the server's before it, so a range over a
	// compressed body names a different byte at each end — but the digest is
	// over the artifact, and a coding applied to a whole body changes what "the
	// bytes" are just as much. Setting this header also turns OFF net/http's own
	// transparent gzip, which is what made this the one question where the
	// answer depended on which language's transport was underneath.
	hreq.Header.Set("Accept-Encoding", "identity")
	return h.do(hreq, req.Reach)
}

func (h HTTP) Fetch(ctx context.Context, req Request) (Result, error) {
	resp, err := h.get(ctx, req)
	if err != nil {
		return Result{}, err
	}
	// The variable, not the value: the restart paths below close the first
	// response themselves and put a second one here.
	defer func() { resp.Body.Close() }()

	// A request is ranged if it asked for a range, which is no longer the same
	// as starting past byte zero: the FIRST gap of a resumed sparse transfer
	// begins at zero and is still bounded, and a 206 answering it has to have
	// its Content-Range checked like any other.
	ranged := req.From > 0 || req.To > 0

	// restartWhole throws the prefix away and asks for the artifact from byte
	// zero. Used where the answer to the ranged request cannot be written at the
	// offset it was asked for AND is not itself a whole artifact, so a second
	// request is the only way to get one.
	restartWhole := func(why error) (*http.Response, error) {
		resp.Body.Close()
		if req.Restart == nil {
			return nil, fmt.Errorf("%w: %w", ErrCannotRestart, why)
		}
		if rerr := req.Restart(); rerr != nil {
			return nil, rerr
		}
		req.From = 0
		req.To = 0
		req.Validators = Validators{}
		again, gerr := h.get(ctx, req) // no Range this time: the whole file
		if gerr != nil {
			return nil, gerr
		}
		if again.StatusCode != http.StatusOK {
			again.Body.Close()
			return nil, fmt.Errorf("download: %s: %s", req.Source.Locator, again.Status)
		}
		return again, nil
	}

	if coding := unwantedCoding(resp.Header); coding != "" {
		return Result{}, fmt.Errorf("download: %s applied Content-Encoding %s to a request that asked for identity",
			req.Source.Locator, coding)
	}

	switch {
	case resp.StatusCode == http.StatusPartialContent:
		// Every 206, ranged or not. A first fetch sends no Range and a CDN may
		// still answer 206; that response is acceptable exactly when it names
		// the offset being written at, which for a first fetch is zero — the
		// same question, and one rule rather than two.
		if cerr := answersFrom(resp, req.From); cerr != nil {
			if !ranged {
				// Nothing to rewind to: this request already asked for the whole
				// artifact and got a piece of somewhere else.
				return Result{}, cerr
			}
			if resp, err = restartWhole(cerr); err != nil {
				return Result{}, err
			}
		}

	case ranged && resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		// The offset is past the end of what the server holds. With an If-Range
		// that matched, that means the artifact is the version these bytes came
		// from and is nonetheless shorter than the prefix on disk — so the
		// prefix cannot be a prefix of it. Nothing is recoverable from that, and
		// the checkpoint would produce the same 416 on every retry until
		// somebody deleted the partial by hand.
		if resp, err = restartWhole(fmt.Errorf("download: %s has fewer than %d bytes (416)",
			req.Source.Locator, req.From)); err != nil {
			return Result{}, err
		}

	case ranged && resp.StatusCode == http.StatusOK:
		// The server answered a ranged request with the whole file. With an
		// If-Range on the request that is the server saying, in the only way
		// HTTP has, "the file you have is not the file I have — here is mine".
		// Without one it is a server that does not do ranges. Either way the
		// body is a complete artifact starting at byte zero, and either way
		// appending it to the prefix on disk splices two files together at an
		// arbitrary offset.
		//
		// So rewind and take it. This is not an error and the transfer proceeds
		// normally; the earlier bytes are simply wasted. Chromium reaches the
		// same conclusion in components/download/internal/common/download_utils.cc
		// near line 349, resetting the offset and clearing the hash state rather
		// than failing the download.
		if req.Restart == nil {
			return Result{}, fmt.Errorf("%w: asked %s for bytes from %d and got the whole file (200)",
				ErrCannotRestart, req.Source.Locator, req.From)
		}
		if rerr := req.Restart(); rerr != nil {
			return Result{}, rerr
		}
		req.From = 0
		req.To = 0

	case resp.StatusCode != http.StatusOK:
		return Result{}, statusError(req.Source.Locator, resp)
	}

	// What this response says about the version being served, recorded now so
	// that the next attempt can ask for the same one. On a first download this
	// is the only chance to learn it; on a resume it confirms it.
	if req.Observed != nil {
		req.Observed(StrongValidators(resp.Header))
	}

	// Worked out BEFORE the copy, not after it. Content-Length on a 200 is the
	// whole artifact; on a 206 it is what remains, so From has to be added back.
	//
	// Except that "what remains" is only true of an OPEN range. A bounded one
	// gets back the length of the gap, and From plus that is the end of the
	// gap, not the size of the file — a number that would be written into
	// Progress.Total and shown to a person as the size of their download. The
	// 206 that answers a bounded request carries the real total after the slash
	// in Content-Range, so read it there and fall back only when it is absent.
	total := int64(0)
	if resp.ContentLength > 0 {
		total = req.From + resp.ContentLength
	}
	if resp.StatusCode == http.StatusPartialContent {
		if n := contentRangeTotal(resp.Header.Get("Content-Range")); n > 0 {
			total = n
		}
	}

	rw := &reportWriter{w: req.Out, total: total, report: req.Report}
	written, err := io.Copy(rw, resp.Body)
	if err != nil {
		return Result{Written: written, Total: total}, err
	}
	return Result{Written: written, Total: total}, nil
}

// Ranged asks the source two questions in one request: how long the artifact
// is, and whether it will serve a bounded piece of it.
//
// A one-byte range is the cheapest honest probe. A 206 answers both — the
// length is the total in Content-Range — and anything else means the source
// sends whole files, which is a plan of one stream rather than an error.
func (h HTTP) Ranged(ctx context.Context, src Source, headers map[string]string) (int64, bool, error) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, src.Locator, nil)
	if err != nil {
		return 0, false, err
	}
	for k, v := range headers {
		hreq.Header.Set(k, v)
	}
	hreq.Header.Set("Range", "bytes=0-0")
	hreq.Header.Set("Accept-Encoding", "identity")

	resp, err := h.do(hreq, nil)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusPartialContent {
		return sizeFromContentRange(resp.Header.Get("Content-Range")), true, nil
	}
	if refused(resp.StatusCode) {
		return 0, false, answered(src.Locator, resp)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, false, nil
	}
	size := int64(0)
	if resp.ContentLength > 0 {
		size = resp.ContentLength
	}
	return size, false, nil
}

// refused reports whether a status is the source saying no, as against the
// transport having a bad moment. See Permanent: the first ends the job, the
// second leaves it adoptable, and download/README.md § Two endings is the list
// every implementation answers to.
//
// Listed rather than ranged, because the two mistakes do not cost the same: an
// unrecognised 4xx is treated as "not now", since calling a retryable status a
// refusal abandons a download that would have worked and this layer exists to
// not lose downloads. 416 and 412 are the resume offset or the file version
// being wrong and restart cleanly, 408, 425 and 429 say try later, 409 and 423
// are somebody else's lock.
func refused(status int) bool {
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusPaymentRequired,
		http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed,
		http.StatusNotAcceptable, http.StatusGone, http.StatusRequestURITooLong,
		http.StatusUnavailableForLegalReasons:
		return true
	}
	return false
}

// answered names the source and what it said, as a refusal nothing will retry.
func answered(locator string, resp *http.Response) error {
	return fmt.Errorf("%w: %s: %s", ErrRefused, locator, resp.Status)
}

// statusError is answered for a status on the list above and an ordinary "not
// now" for every other one.
//
// Fetch used to call answered directly, so every non-200 was permanent and the
// list this file spends thirty lines justifying was consulted by nobody on the
// path that downloads. A 503 ended the job. Found by
// download/testdata/scenarios/wire-notnow-status.txt.
func statusError(locator string, resp *http.Response) error {
	if refused(resp.StatusCode) {
		return answered(locator, resp)
	}
	return fmt.Errorf("download: %s: %s", locator, resp.Status)
}

func (h HTTP) FetchRange(ctx context.Context, req RangeRequest) error {
	last := req.Range.End - 1
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.Source.Locator, nil)
	if err != nil {
		return err
	}
	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}
	hreq.Header.Set("Range", "bytes="+strconv.FormatInt(req.Range.Start, 10)+"-"+strconv.FormatInt(last, 10))
	if v := req.Validators.IfRange(); v != "" {
		hreq.Header.Set("If-Range", v)
	}

	resp, err := h.do(hreq, req.Reach)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		if refused(resp.StatusCode) {
			return answered(req.Source.Locator, resp)
		}
		return fmt.Errorf("download: asked for bytes %d-%d, server answered %s", req.Range.Start, last, resp.Status)
	}
	// A sequential fetch can trust its own offset because it only ever appends.
	// A range is written into the middle of a file, so a proxy answering with a
	// different range than the one asked for would corrupt bytes no later check
	// could attribute. The server names what it sent; hold it to that.
	if got := resp.Header.Get("Content-Range"); !namesRange(got, req.Range) {
		return fmt.Errorf("download: asked for bytes %d-%d, server sent Content-Range %q", req.Range.Start, last, got)
	}
	return copyRange(ctx, req, resp.Body)
}

// namesRange reports whether a Content-Range describes exactly the range asked
// for.
func namesRange(header string, want job.Range) bool {
	v := strings.TrimPrefix(strings.TrimSpace(header), "bytes ")
	slash := strings.IndexByte(v, '/')
	if slash < 0 {
		return false
	}
	dash := strings.IndexByte(v[:slash], '-')
	if dash < 0 {
		return false
	}
	start, err := strconv.ParseInt(v[:dash], 10, 64)
	if err != nil {
		return false
	}
	end, err := strconv.ParseInt(v[dash+1:slash], 10, 64)
	return err == nil && start == want.Start && end == want.End-1
}

// sizeFromContentRange reads the artifact length out of a Content-Range, or 0 when the
// server declined to say — "bytes 0-0/*" is legal and useless for planning.
func sizeFromContentRange(header string) int64 {
	_, total, ok := strings.Cut(strings.TrimSpace(header), "/")
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func copyRange(ctx context.Context, req RangeRequest, body io.Reader) error {
	want := req.Range.End - req.Range.Start
	w := &rangeWriter{out: req.Out, at: req.Range.Start, end: req.Range.End, beat: req.Beat}
	n, err := copyWithContext(ctx, w, io.LimitReader(body, want))
	if err != nil {
		return err
	}
	if n != want {
		return fmt.Errorf("%w: range %d-%d gave %d of %d bytes", ErrShortTransfer, req.Range.Start, req.Range.End-1, n, want)
	}
	var surplus [1]byte
	if extra, _ := body.Read(surplus[:]); extra > 0 {
		return fmt.Errorf("%w: range %d-%d", ErrOverrun, req.Range.Start, req.Range.End-1)
	}
	return nil
}

// rangeWriter writes at absolute artifact offsets and cannot leave its range. It
// also says the work is moving, without saying how far: distance is the proven
// set's business and it only moves when a whole range lands.
//
// Bounded by construction rather than by the server's honesty. A 206 carrying a
// correct Content-Range and a longer body than it named would otherwise be
// copied whole, over bytes a neighbouring stream has already synced and
// recorded as proven — and the second hashing pass would then throw the entire
// artifact away for it.
type rangeWriter struct {
	out  io.WriterAt
	at   int64
	end  int64
	beat func()
}

func (s *rangeWriter) Write(p []byte) (int, error) {
	if s.at+int64(len(p)) > s.end {
		return 0, fmt.Errorf("%w: %d bytes at %d reach past %d", ErrOverrun, len(p), s.at, s.end)
	}
	n, err := s.out.WriteAt(p, s.at)
	s.at += int64(n)
	if s.beat != nil {
		s.beat()
	}
	return n, err
}

// File fetches from anything the operating system will open as a path.
//
// That includes a UNC path to a NAS, which is why there is no SMB protocol
// implementation here and should not be. The survey was blunt about it: rsync is
// GPL-3.0, absent from Windows without vendoring Cygwin, has no job identity, and
// its delta algorithm is actively counterproductive on single opaque 40 GB blobs.
// SMB is the right substrate precisely because it is not a transfer protocol —
// open the path, seek, read, and let the OS redirector be the client. Offsets,
// hashing and progress then live in one place instead of once per protocol.
type File struct{}

func (File) Schemes() []string { return []string{"file", "smb"} }

func (File) Capabilities() []Capability { return []Capability{CapResume} }

func (File) Fetch(ctx context.Context, req Request) (Result, error) {
	f, err := os.Open(req.Source.Locator)
	if err != nil {
		return Result{}, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return Result{}, err
	}
	if req.From > 0 {
		if _, err := f.Seek(req.From, io.SeekStart); err != nil {
			return Result{}, err
		}
	}

	rw := &reportWriter{w: req.Out, total: st.Size(), report: req.Report}
	written, err := copyWithContext(ctx, rw, f)
	if err != nil {
		return Result{Written: written}, err
	}
	return Result{Written: written, Total: st.Size()}, nil
}

// copyWithContext is io.Copy that notices cancellation. os.File reads do not
// respect a context on their own, and a job that cannot be cancelled is a job
// that has to be killed — which loses the lease and the progress with it.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 1<<20)
	var total int64
	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}
		n, err := src.Read(buf)
		if n > 0 {
			w, werr := dst.Write(buf[:n])
			total += int64(w)
			if werr != nil {
				return total, werr
			}
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

func (File) Ranged(_ context.Context, src Source, _ map[string]string) (int64, bool, error) {
	st, err := os.Stat(src.Locator)
	if err != nil {
		return 0, false, err
	}
	return st.Size(), st.Mode().IsRegular(), nil
}

func (File) FetchRange(ctx context.Context, req RangeRequest) error {
	f, err := os.Open(req.Source.Locator)
	if err != nil {
		return err
	}
	defer f.Close()
	return copyRange(ctx, req, io.NewSectionReader(f, req.Range.Start, req.Range.End-req.Range.Start))
}

// DefaultFetchers is the set of fetchers that need nothing configured. The
// Windows service tier (BITS) and any NAS-side delegation are added by whoever
// has them, which is the whole point of the split.
func DefaultFetchers() *Fetchers {
	return NewFetchers(HTTP{}, File{})
}

// compile-time proof that both satisfy the interfaces
var (
	_ RangeFetcher = HTTP{}
	_ RangeFetcher = File{}
)
