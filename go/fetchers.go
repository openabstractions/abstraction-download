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

func (h HTTP) Fetch(ctx context.Context, req Request) (Result, error) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.Source.Locator, nil)
	if err != nil {
		return Result{}, err
	}
	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}
	if req.From > 0 {
		hreq.Header.Set("Range", "bytes="+strconv.FormatInt(req.From, 10)+"-")
	}

	resp, err := h.client().Do(hreq)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()

	switch {
	case req.From > 0 && resp.StatusCode == http.StatusPartialContent:
		// The server honoured the range. Good.
	case req.From > 0 && resp.StatusCode == http.StatusOK:
		// It ignored the range and is sending the whole file from zero. Appending
		// this to what is already on disk would silently produce a file that is
		// the right length in the wrong way, which is exactly the class of
		// corruption this project exists to refuse. Fail instead.
		return Result{}, fmt.Errorf("download: asked for bytes from %d, server sent the whole file (200); "+
			"resuming here would corrupt the artifact", req.From)
	case resp.StatusCode != http.StatusOK:
		return Result{}, fmt.Errorf("download: %s: %s", req.Source.Locator, resp.Status)
	}

	// Worked out BEFORE the copy, not after it. Content-Length on a 200 is the
	// whole artifact; on a 206 it is what remains, so From has to be added back.
	total := int64(0)
	if resp.ContentLength > 0 {
		total = req.From + resp.ContentLength
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
