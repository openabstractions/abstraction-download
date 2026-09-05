package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
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
// This is the console tier from research/transfer/SUMMARY.txt, and the survey's
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
		span := strconv.FormatInt(req.From, 10) + "-"
		if req.To > req.From {
			span += strconv.FormatInt(req.To-1, 10)
		}
		hreq.Header.Set("Range", "bytes="+span)

		// Say which version the bytes on disk came from. Without this a server
		// whose file has changed answers the range honestly, with a valid range
		// of a DIFFERENT file, and nothing in the response says so. With it, the
		// server answers 206 only if the file is unchanged and 200 — the whole
		// new file — if it is not, which is the case Fetch handles by starting
		// again.
		if v := req.Validators.IfRange(); v != "" {
			hreq.Header.Set("If-Range", v)
		}

		// Offsets on this side are counted after decoding and the server's are
		// counted before it, so a range over a compressed body asks for a byte
		// position that means something different at each end. Asking for the
		// identity encoding makes the two agree.
		hreq.Header.Set("Accept-Encoding", "identity")
	}
	return h.client().Do(hreq)
}

// Fetch gets the bytes, and decides what the server's answer to a ranged
// request actually means.
//
// Three answers are possible and only one of them is "here is the rest of your
// file". The other two — the whole file from zero, and a range starting
// somewhere other than where we asked — are what a server sends when the
// artifact it holds is no longer the artifact the bytes on disk came from.
// Neither is a transport error, and neither may be appended.
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

	switch {
	case ranged && resp.StatusCode == http.StatusPartialContent:
		start, cerr := contentRangeStart(resp.Header.Get("Content-Range"))
		if cerr != nil || start != req.From {
			// A range that does not begin where we asked is not a partial
			// answer to this request. Its bytes belong at an offset nobody
			// asked about, and appending them puts the artifact's own content
			// at the wrong place in the file — invisible to a length check and
			// invisible to a transport error. Do not trust it; start again.
			if cerr == nil {
				cerr = fmt.Errorf("download: asked for bytes from %d, got a range starting at %d", req.From, start)
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
		return Result{}, fmt.Errorf("download: %s: %s", req.Source.Locator, resp.Status)
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

	// A bounded gap reads exactly its own bytes and stops. Without the limit a
	// file source would read to EOF over the top of every proven range after
	// the hole — the same bytes, so nothing would break, but the whole saving
	// of knowing which ranges are proven would be spent copying them again.
	var src io.Reader = f
	if req.To > req.From {
		src = io.LimitReader(f, req.To-req.From)
	}

	rw := &reportWriter{w: req.Out, total: st.Size(), report: req.Report}
	written, err := copyWithContext(ctx, rw, src)
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

// DefaultRegistry is the set of fetchers that need nothing configured. The
// Windows service tier (BITS) and any NAS-side delegation are added by whoever
// has them, which is the whole point of the split.
func DefaultRegistry() *Registry {
	return NewRegistry(HTTP{}, File{})
}

// compile-time proof that both satisfy the interface
var (
	_ Fetcher = HTTP{}
	_ Fetcher = File{}
)
