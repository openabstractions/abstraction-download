package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	job "github.com/openabstractions/abstraction-job/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// trickleSource serves its first request's first hold bytes, waits for
// release, and then sends the rest chunk bytes at a time every interval, so a
// transfer is under way, checkpointing and renewing, for as long as a test
// needs. Later requests are served whole, ranges honoured.
type trickleSource struct {
	body    []byte
	hold    int64
	chunk   int
	every   time.Duration
	release chan struct{}
	mu      sync.Mutex
	ranges  []string
}

func (s *trickleSource) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.ranges = append(s.ranges, r.Header.Get("Range"))
	first := len(s.ranges) == 1
	s.mu.Unlock()
	if !first {
		http.ServeContent(w, r, "artifact", time.Time{}, bytes.NewReader(s.body))
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(s.body)))
	w.WriteHeader(http.StatusOK)
	w.Write(s.body[:s.hold])
	w.(http.Flusher).Flush()
	select {
	case <-s.release:
	case <-r.Context().Done():
		return
	}
	for rest := s.body[s.hold:]; len(rest) > 0; {
		n := min(s.chunk, len(rest))
		if _, err := w.Write(rest[:n]); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		rest = rest[n:]
		select {
		case <-time.After(s.every):
		case <-r.Context().Done():
			return
		}
	}
}

func (s *trickleSource) log() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ranges...)
}

// standingDownload is a download accepted under a recorded subject from a
// provider whose standing decision a test switches between permitted and
// unavailable (abstraction-job JOB-A18).
type standingDownload struct {
	p       *acceptanceprovider.Provider
	root    string
	binding acceptanceprovider.Binding
	outage  atomic.Bool
	// unansweredRenewals counts the lease renewals decided while the outage
	// was on.
	unansweredRenewals atomic.Int64
	source             *trickleSource
	id                 api.RequestIdentity
	op                 string
}

func newStandingDownload(t *testing.T, size, hold int, every time.Duration) *standingDownload {
	t.Helper()
	d := &standingDownload{root: t.TempDir()}
	body := testBody(size)
	d.source = &trickleSource{body: body, hold: int64(hold), chunk: 16 << 10, every: every, release: make(chan struct{})}
	server := httptest.NewServer(d.source)
	t.Cleanup(server.Close)
	p, err := acceptanceprovider.OpenWithExecutor(d.root, "standing-owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	d.p = p
	p.SetStanding(func(ctx context.Context, _ acceptanceprovider.AuthenticatedSubject, _ string) (string, error) {
		if d.outage.Load() {
			if acceptanceprovider.StandingCallOf(ctx) == acceptanceprovider.StandingRenew {
				d.unansweredRenewals.Add(1)
			}
			return "", errors.New("rights unavailable")
		}
		return "", nil
	})
	executable := filepath.Join(t.TempDir(), "fetch.exe")
	kind, account := "posix", "1000"
	if runtime.GOOS == "windows" {
		kind, account = "windows", "S-1-5-21-1-2-3-1001"
	}
	scope, err := acceptanceprovider.LocalSubjectScope(kind, account, executable)
	if err != nil {
		t.Fatal(err)
	}
	d.binding = acceptanceprovider.Binding{Scope: scope, Origin: "local",
		Subject: &acceptanceprovider.AuthenticatedSubject{AccountKind: kind, Account: account, Executable: executable, Program: executable}}
	fastService(t)
	window, err := p.Bind(scope).GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	d.id = api.RequestIdentity{Key: "standing", HistoryEpoch: window.HistoryEpoch}
	v, err := p.BindBinding(d.binding).Submit(api.Submission{Identity: d.id, Kind: "download", Spec: request.Encode(&request.Request{
		Artifact: request.Artifact{Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(body)), Size: int64(len(body))},
		Sources:  []request.Source{{Scheme: "http", Locator: server.URL + "/artifact"}}})})
	if err != nil || v.Outcome.String() != "accepted" {
		t.Fatalf("submit: %+v %v", v, err)
	}
	d.op = v.Receipt.OperationID
	execute(t, p)
	waitFor(t, "the held prefix checkpointed", 20*time.Second, func() bool { return d.snapshot(t).Progress.Done >= int64(hold) })
	return d
}

func (d *standingDownload) snapshot(t *testing.T) *api.OperationSnapshot {
	t.Helper()
	v, err := d.p.BindOperationsBinding(d.binding).ObserveWork(d.id)
	if err != nil || v.Snapshot == nil {
		t.Fatalf("observe: %+v %v", v, err)
	}
	return v.Snapshot
}

func (d *standingDownload) complete(t *testing.T) {
	t.Helper()
	select {
	case <-d.source.release:
	default:
		close(d.source.release)
	}
	waitFor(t, "completion", 30*time.Second, func() bool {
		s := d.snapshot(t)
		if s.State == api.WorkStateFailed || s.State == api.WorkStateCancelled {
			t.Fatalf("ended %s: %+v", s.State, s.Failure)
		}
		return s.State == api.WorkStateComplete
	})
	var got []byte
	for {
		v, err := d.p.BindOperationsBinding(d.binding).ReadResult(d.id, int64(len(got)), 65536)
		if err != nil || v.Outcome.String() != "data" {
			t.Fatalf("read result at %d: %+v %v", len(got), v, err)
		}
		got = append(got, v.Chunk.Data...)
		if v.Chunk.EOF {
			break
		}
	}
	if !bytes.Equal(got, d.source.body) {
		t.Fatalf("result differs: %d bytes", len(got))
	}
}

// JOB-A18 with the download runner: a standing decision that cannot be
// obtained for less than the lease's lifetime interrupts nothing. Bytes flow
// through the outage, so the runner checkpoints and renews during it; the
// transfer keeps its one source request and completes once the decision
// answers again.
func TestAShortStandingOutageInterruptsNoDownload(t *testing.T) {
	d := newStandingDownload(t, 1<<20, 64<<10, 25*time.Millisecond)
	d.outage.Store(true)
	close(d.source.release)
	// Longer than one renewal interval (a third of the 2 s lease), shorter
	// than the lease; the rest of the body takes about 1.5 s.
	time.Sleep(1200 * time.Millisecond)
	d.outage.Store(false)
	// The outage is only shown harmless if renewals met it.
	if n := d.unansweredRenewals.Load(); n == 0 {
		t.Fatal("no lease renewal was decided during the outage")
	}
	if s := d.snapshot(t); s.State == api.WorkStateFailed || s.State == api.WorkStateCancelled {
		t.Fatalf("the outage ended the download: %+v", s)
	}
	d.complete(t)
	if log := d.source.log(); len(log) != 1 {
		t.Fatalf("the outage interrupted the transfer: source requests %q", log)
	}
}

// JOB-A18 with the download runner: an outage that outlasts the lease ends
// the lease at its expiry and the runner lets the work go, ending nothing;
// no claim starts it again until a decision answers, and then it resumes
// from its checkpoint with one Range request.
func TestALongStandingOutageLapsesTheLeaseAndResumesAfter(t *testing.T) {
	hold := 64 << 10
	d := newStandingDownload(t, 256<<10, hold, 0)
	d.outage.Store(true)
	store, err := job.NewFileStore(d.root)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the lease to lapse", 20*time.Second, func() bool {
		r, err := store.Load(d.op)
		return err == nil && !r.Lease.Held(time.Now())
	})
	// No claim starts it while the decision cannot be obtained.
	time.Sleep(time.Second)
	r, err := store.Load(d.op)
	if err != nil || r.State.Terminal() || r.Lease.Held(time.Now()) {
		t.Fatalf("during the outage the work was ended or claimed again: %+v %v", r, err)
	}
	if log := d.source.log(); len(log) != 1 {
		t.Fatalf("a source was opened during the outage: %q", log)
	}
	d.outage.Store(false)
	d.complete(t)
	if log := d.source.log(); len(log) != 2 || log[1] != fmt.Sprintf("bytes=%d-", hold) {
		t.Fatalf("source requests %q", log)
	}
}
