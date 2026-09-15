package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// rangeSource serves one artifact with Range support and logs every GET's Range
// header. With hold > 0 the first GET sends hold bytes and then stalls.
type rangeSource struct {
	body    []byte
	hold    int64
	release chan struct{}
	mu      sync.Mutex
	ranges  []string
}

func (s *rangeSource) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.ranges = append(s.ranges, r.Header.Get("Range"))
	first := len(s.ranges) == 1
	s.mu.Unlock()
	if first && s.hold > 0 {
		w.Header().Set("Content-Length", fmt.Sprint(len(s.body)))
		w.WriteHeader(http.StatusOK)
		w.Write(s.body[:s.hold])
		w.(http.Flusher).Flush()
		select {
		case <-s.release:
			w.Write(s.body[s.hold:])
		case <-r.Context().Done():
		}
		return
	}
	http.ServeContent(w, r, "artifact", time.Time{}, bytes.NewReader(s.body))
}

func (s *rangeSource) log() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ranges...)
}

func fastRunner(r *download.Runner) {
	r.PersistInterval = 50 * time.Millisecond
	r.LeaseTTL = 2 * time.Second
}

// TestHTTPExecutionCrashChild is the service process a parent kills mid-transfer.
func TestHTTPExecutionCrashChild(t *testing.T) {
	root := os.Getenv("OA_SERVE_CRASH_ROOT")
	if root == "" {
		return
	}
	configureRunner = fastRunner
	p, err := acceptanceprovider.OpenWithExecutor(root, "crash-owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Execute(context.Background())
	t.Fatal("execution returned before the parent killed this process")
}

func testBody(n int) []byte {
	body := make([]byte, n)
	for i := range body {
		body[i] = byte(i*31 + i/7)
	}
	return body
}

func observe(t *testing.T, p *acceptanceprovider.Provider, id api.RequestIdentity) *api.OperationSnapshot {
	t.Helper()
	v, err := p.BindOperations("alice").ObserveWork(id)
	if err != nil || v.Outcome != "observed" || v.Snapshot == nil {
		t.Fatalf("observe: %+v %v", v, err)
	}
	return v.Snapshot
}

func waitFor(t *testing.T, what string, deadline time.Duration, check func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for !check() {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func readAll(t *testing.T, p *acceptanceprovider.Provider, id api.RequestIdentity) []byte {
	t.Helper()
	var out []byte
	for {
		v, err := p.BindOperations("alice").ReadResult(id, int64(len(out)), 65536)
		if err != nil || v.Outcome != "data" {
			t.Fatalf("read result at %d: %+v %v", len(out), v, err)
		}
		out = append(out, v.Chunk.Data...)
		if v.Chunk.Eof {
			return out
		}
	}
}

func submitDownload(t *testing.T, p *acceptanceprovider.Provider, key string, attempt int64, body []byte, declare bool, url string) (api.RequestIdentity, string) {
	t.Helper()
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	id := api.RequestIdentity{Key: key, HistoryEpoch: window.HistoryEpoch, Attempt: attempt}
	artifact := request.Artifact{Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(body))}
	if declare {
		artifact.Size = int64(len(body))
	}
	spec := request.Encode(&request.Request{Artifact: artifact, Sources: []request.Source{{Scheme: "http", Locator: url}}})
	v, err := p.Bind("alice").Submit(api.Submission{Identity: id, Kind: "download", Spec: spec})
	if err != nil || v.Outcome != "accepted" {
		t.Fatalf("submit: %+v %v", v, err)
	}
	return id, v.Receipt.OperationId
}

func execute(t *testing.T, p *acceptanceprovider.Provider) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Execute(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// After the service process is killed mid-transfer, the restarted service
// resumes from the durable checkpoint and delivers exact bytes. A crashed
// partial is truncated to its verified prefix, and corruption inside that
// prefix restarts the transfer from zero instead of being trusted.
func TestHTTPExecutionResumesAfterCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns and kills a service process")
	}
	const size, hold = 1 << 20, 256 << 10
	for _, c := range []struct {
		name   string
		damage func(t *testing.T, partial string)
		ranges func(t *testing.T, log []string)
	}{
		{name: "resumes-from-checkpoint",
			ranges: func(t *testing.T, log []string) {
				if len(log) != 2 || log[1] != fmt.Sprintf("bytes=%d-", hold) {
					t.Fatalf("source requests %q", log)
				}
			}},
		{name: "truncates-past-checkpoint",
			damage: func(t *testing.T, partial string) {
				f, err := os.OpenFile(partial, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				f.Write(bytes.Repeat([]byte("junk"), 16<<10))
				f.Close()
			},
			ranges: func(t *testing.T, log []string) {
				if len(log) != 2 || log[1] != fmt.Sprintf("bytes=%d-", hold) {
					t.Fatalf("source requests %q", log)
				}
			}},
		{name: "restarts-on-corrupt-prefix",
			damage: func(t *testing.T, partial string) {
				f, err := os.OpenFile(partial, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				f.WriteAt(bytes.Repeat([]byte{0xAA}, 512), 4096)
				f.Close()
			},
			ranges: func(t *testing.T, log []string) {
				if len(log) != 3 || log[1] != fmt.Sprintf("bytes=%d-", hold) || log[2] != "" {
					t.Fatalf("source requests %q", log)
				}
			}},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := testBody(size)
			source := &rangeSource{body: body, hold: hold, release: make(chan struct{})}
			server := httptest.NewServer(source)
			defer server.Close()
			defer close(source.release)
			root := t.TempDir()
			p, err := acceptanceprovider.OpenWithExecutor(root, "crash-owner", HTTPExecution{})
			if err != nil {
				t.Fatal(err)
			}
			id, operation := submitDownload(t, p, "crash", 0, body, true, server.URL+"/artifact")

			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			child := exec.Command(exe, "-test.run=^TestHTTPExecutionCrashChild$")
			child.Env = append(os.Environ(), "OA_SERVE_CRASH_ROOT="+root)
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "durable checkpoint in the child", 20*time.Second, func() bool {
				return observe(t, p, id).Progress.Done >= hold
			})
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			child.Wait()
			t.Logf("progress at kill: %+v", observe(t, p, id).Progress)
			partial := filepath.Join(root, "work", operation)
			if c.damage != nil {
				if _, err := os.Stat(partial); err != nil {
					var files []string
					filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
						if err == nil && !d.IsDir() {
							files = append(files, path)
						}
						return nil
					})
					t.Fatalf("partial %s: %v; files %q", partial, err, files)
				}
				c.damage(t, partial)
			}

			configureRunner = fastRunner
			t.Cleanup(func() { configureRunner = func(*download.Runner) {} })
			execute(t, p)
			waitFor(t, "completion after restart", 60*time.Second, func() bool {
				s := observe(t, p, id)
				if s.State == "failed" || s.State == "cancelled" {
					t.Fatalf("operation ended %s: %+v", s.State, s.Failure)
				}
				return s.State == "complete"
			})
			got := readAll(t, p, id)
			if !bytes.Equal(got, body) || sha256.Sum256(got) != sha256.Sum256(body) {
				t.Fatalf("result differs: %d bytes", len(got))
			}
			c.ranges(t, source.log())
		})
	}
}

// Observation carries the progress total from the declared size or from the
// source's Content-Length while the transfer runs and after it completes.
func TestHTTPExecutionReportsProgressTotal(t *testing.T) {
	for _, declare := range []bool{true, false} {
		t.Run(fmt.Sprintf("declared=%v", declare), func(t *testing.T) {
			body := testBody(300 << 10)
			source := &rangeSource{body: body, hold: 100 << 10, release: make(chan struct{})}
			server := httptest.NewServer(source)
			defer server.Close()
			p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "progress-owner", HTTPExecution{})
			if err != nil {
				t.Fatal(err)
			}
			configureRunner = fastRunner
			t.Cleanup(func() { configureRunner = func(*download.Runner) {} })
			id, _ := submitDownload(t, p, "progress", 0, body, declare, server.URL+"/artifact")
			execute(t, p)
			waitFor(t, "running progress", 20*time.Second, func() bool {
				s := observe(t, p, id)
				return s.Progress.Done > 0 && s.Progress.Total > 0
			})
			if s := observe(t, p, id); s.Progress.Total != int64(len(body)) || s.State != "running" {
				t.Fatalf("running snapshot: %+v", s)
			}
			close(source.release)
			waitFor(t, "completion", 30*time.Second, func() bool { return observe(t, p, id).State == "complete" })
			if s := observe(t, p, id); s.Progress.Total != int64(len(body)) || s.Progress.Done != int64(len(body)) {
				t.Fatalf("complete snapshot: %+v", s)
			}
		})
	}
}

// A complete operation whose result bytes disappear ends as a typed failure
// and permits the next attempt of the key [JOB-A10, JOB-A7].
func TestHTTPExecutionLostResultPermitsNextAttempt(t *testing.T) {
	body := testBody(70 << 10)
	source := &rangeSource{body: body}
	server := httptest.NewServer(source)
	defer server.Close()
	root := t.TempDir()
	p, err := acceptanceprovider.OpenWithExecutor(root, "lost-owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	id, operation := submitDownload(t, p, "lost", 0, body, true, server.URL+"/artifact")
	execute(t, p)
	waitFor(t, "completion", 30*time.Second, func() bool { return observe(t, p, id).State == "complete" })
	if !bytes.Equal(readAll(t, p, id), body) {
		t.Fatal("first result differs")
	}

	result := filepath.Join(root, "results", operation)
	if err := os.Remove(result); err != nil {
		t.Fatal(err)
	}
	if v, err := p.BindOperations("alice").ReadResult(id, 0, 16); err != nil || v.Outcome != "unavailable" {
		t.Fatalf("read of lost result: %+v %v", v, err)
	}
	s := observe(t, p, id)
	if s.State != "failed" || s.Failure == nil || s.Failure.Classification != "permanent" || s.Failure.Cause != "result_lost" || s.Receipt.OperationId != operation {
		t.Fatalf("lost result observation: %+v", s)
	}
	if err := os.WriteFile(result, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if v, err := p.BindOperations("alice").ReadResult(id, 0, 16); err != nil || v.Outcome != "unavailable" {
		t.Fatalf("reappearing bytes served under a lost identity: %+v %v", v, err)
	}
	page, err := p.BindInventory("alice").ListWork("", 8)
	if err != nil || page.Outcome != "page" || len(page.Snapshots) != 1 || page.Snapshots[0].Failure == nil || page.Snapshots[0].Failure.Cause != "result_lost" {
		t.Fatalf("inventory of lost result: %+v %v", page, err)
	}

	retry, second := submitDownload(t, p, "lost", 1, body, true, server.URL+"/artifact")
	if second == operation {
		t.Fatal("retry reused the lost operation")
	}
	waitFor(t, "retry completion", 30*time.Second, func() bool { return observe(t, p, retry).State == "complete" })
	if !bytes.Equal(readAll(t, p, retry), body) {
		t.Fatal("retry result differs")
	}
	if log := source.log(); len(log) != 2 || strings.Join(log, "") != "" {
		t.Fatalf("source requests %q", log)
	}
}
