package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// servedRange is one ranged GET and the bytes the origin wrote for it.
type servedRange struct {
	header  string
	start   int64
	sent    int64
	started time.Time
}

// countingOrigin serves one artifact by range and counts the bytes it sends
// for every request. The first request whose range contains holdAt sends the
// bytes before holdAt and then stalls until the client goes away or release
// is closed.
type countingOrigin struct {
	body    []byte
	holdAt  int64
	release chan struct{}

	mu       sync.Mutex
	requests []*servedRange
	held     *servedRange
}

func (o *countingOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	header := r.Header.Get("Range")
	spec, ok := strings.CutPrefix(header, "bytes=")
	first, last, dash := strings.Cut(spec, "-")
	start, err1 := strconv.ParseInt(first, 10, 64)
	end, err2 := strconv.ParseInt(last, 10, 64)
	if !ok || !dash || err1 != nil || err2 != nil || start > end || end >= int64(len(o.body)) {
		http.Error(w, "ranged requests only", http.StatusBadRequest)
		return
	}
	o.mu.Lock()
	served := &servedRange{header: header, start: start, started: time.Now()}
	hold := o.held == nil && start < o.holdAt && o.holdAt <= end
	if hold {
		o.held = served
	}
	// The one-byte probe is how a runner asks whether ranges are served; it
	// carries no artifact bytes the transfer keeps.
	if header != "bytes=0-0" {
		o.requests = append(o.requests, served)
	}
	o.mu.Unlock()

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(o.body)))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)
	stop := end + 1
	if hold {
		stop = o.holdAt
	}
	for at := start; at < stop; {
		n := min(stop-at, 64<<10)
		written, err := w.Write(o.body[at : at+n])
		o.mu.Lock()
		served.sent += int64(written)
		o.mu.Unlock()
		if err != nil {
			return
		}
		at += int64(written)
	}
	if hold {
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-o.release:
		}
	}
}

// holding reports whether the held request has sent every byte before holdAt.
func (o *countingOrigin) holding() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.held != nil && o.held.sent == o.holdAt-o.held.start
}

func (o *countingOrigin) served() []servedRange {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]servedRange, 0, len(o.requests))
	for _, r := range o.requests {
		out = append(out, *r)
	}
	return out
}

// seedEarlierFailure leaves the record as an earlier runtime's failed attempt
// leaves it: a retryable failure written under epoch 1, long enough ago that
// its backoff has passed.
func seedEarlierFailure(t *testing.T, root, operation string) {
	t.Helper()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := store.Claim(operation, "earlier-runtime", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(operation, rec.Lease.Epoch, func(rr *job.Record) error {
		rr.Error = "download: source http: no progress within the silence budget"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(operation, rec.Lease.Epoch); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "jobs", operation+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := job.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	seeded.UpdatedAt = job.At(time.Now().Add(-time.Hour))
	if raw, err = seeded.Encode(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if loaded, err := store.Load(operation); err != nil || download.LastFailure(loaded) == nil || time.Now().Before(download.RetryAfter(loaded)) {
		t.Fatalf("seeded record: %+v %v", loaded, err)
	}
}

// A runtime stopped part way through a ranged transfer resumes each range from
// its last checkpointed byte, and its successor starts promptly. The origin
// counts every byte it serves: across the stop and the restart each byte of
// the artifact is served exactly once. The record carries a retryable failure
// from an earlier attempt, as the 0.1.8 qualification's did. A graceful stop
// resumes within resumeBound of the successor starting, and a crash within the
// lease plus the same bound; backoff from the stale failure would take 30 s.
func TestHTTPExecutionResumesRangesFromTheCheckpointAfterAStop(t *testing.T) {
	if testing.Short() {
		t.Skip("transfers 32 MiB twice and spawns a service process")
	}
	const (
		rangeSize   = 16 << 20
		size        = 2 * rangeSize
		holdAt      = 4 << 20
		resumeBound = 5 * time.Second
	)
	lease := 2 * time.Second // fastRunner's LeaseTTL, in the child and here
	body := testBody(size)
	for _, mode := range []string{"graceful", "crash"} {
		t.Run(mode, func(t *testing.T) {
			origin := &countingOrigin{body: body, holdAt: holdAt, release: make(chan struct{})}
			server := httptest.NewServer(origin)
			// Deferred in reverse: a failing run stops its execution, then
			// releases the held request, then closes the origin.
			defer server.Close()
			defer close(origin.release)
			root := t.TempDir()
			configureRunner = fastRunner
			t.Cleanup(func() { configureRunner = func(*download.Runner) {} })
			p, err := acceptanceprovider.OpenWithExecutor(root, "crash-owner", HTTPExecution{})
			if err != nil {
				t.Fatal(err)
			}
			id, operation := submitDownload(t, p, "ranges", 0, body, true, server.URL+"/artifact")
			seedEarlierFailure(t, root, operation)

			// The second range lands whole and the first holds after 4 MiB. The
			// stop waits for the origin to hold and then, for at most 3 s, for
			// the checkpoint to cover every byte that landed; a runner that never
			// checkpoints a partial range is stopped anyway, and the byte count
			// below shows what it fetched again.
			settled := func() {
				waitFor(t, "the origin holding the first range", 30*time.Second, func() bool {
					return origin.holding() && observe(t, p, id).Progress.Done >= rangeSize
				})
				for end := time.Now().Add(3 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
					if observe(t, p, id).Progress.Done == rangeSize+holdAt {
						return
					}
				}
			}
			var stopped time.Time
			switch mode {
			case "graceful":
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- p.Execute(ctx) }()
				var once sync.Once
				var stopErr error
				stop := func() { once.Do(func() { cancel(); stopErr = <-done }) }
				defer stop()
				settled()
				stop()
				if err := stopErr; err != nil {
					t.Fatalf("execution stop: %v", err)
				}
				stopped = time.Now()
				rec, err := job.NewFileStore(root)
				if err != nil {
					t.Fatal(err)
				}
				loaded, err := rec.Load(operation)
				if err != nil {
					t.Fatal(err)
				}
				// Released, and no earlier failure left to charge backoff.
				if loaded.Lease.Owner != "" || download.LastFailure(loaded) != nil {
					t.Errorf("record after a graceful stop: owner %q, failure %v", loaded.Lease.Owner, download.LastFailure(loaded))
				}
			case "crash":
				exe, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				child := exec.Command(exe, "-test.run=^TestHTTPExecutionCrashChild$")
				child.Env = append(os.Environ(), "OA_SERVE_CRASH_ROOT="+root)
				if err := child.Start(); err != nil {
					t.Fatal(err)
				}
				defer func() {
					child.Process.Kill()
					child.Wait()
				}()
				settled()
				if err := child.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				child.Wait()
				stopped = time.Now()
			}
			before := len(origin.served())

			successor, err := acceptanceprovider.OpenWithExecutor(root, "crash-owner", HTTPExecution{})
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			execute(t, successor)
			waitFor(t, "completion after restart", 60*time.Second, func() bool {
				s := observe(t, successor, id)
				if s.State.String() == "failed" || s.State.String() == "cancelled" {
					t.Fatalf("operation ended %s: %+v", s.State, s.Failure)
				}
				return s.State.String() == "complete"
			})
			if got := readAll(t, successor, id); !bytes.Equal(got, body) || sha256.Sum256(got) != sha256.Sum256(body) {
				t.Fatalf("result differs: %d bytes", len(got))
			}

			served := origin.served()
			var total int64
			for _, r := range served {
				total += r.sent
			}
			if total != size {
				t.Fatalf("origin served %d bytes for a %d-byte artifact, so bytes that had landed were fetched again: %+v", total, size, served)
			}
			resumed := served[before:]
			want := fmt.Sprintf("bytes=%d-%d", holdAt, rangeSize-1)
			if len(resumed) != 1 || resumed[0].header != want {
				t.Fatalf("after the stop the origin was asked for %+v, want one request %s", resumed, want)
			}
			var bound time.Duration
			var from time.Time
			switch mode {
			case "graceful":
				bound, from = resumeBound, started
			case "crash":
				bound, from = lease+resumeBound, stopped
			}
			waited := resumed[0].started.Sub(from)
			if waited > bound {
				t.Fatalf("%s stop: the successor resumed after %s, bound %s", mode, waited, bound)
			}
			t.Logf("%s stop: resumed %s after the reference instant, bound %s", mode, waited, bound)
		})
	}
}
