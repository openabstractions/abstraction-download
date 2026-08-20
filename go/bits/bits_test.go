package bits

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	download "github.com/ReinisLusis/abstraction-download"
	job "github.com/ReinisLusis/abstraction-job"
)

// Every test that touches BITS goes through this. It must SKIP and never fail
// when BITS cannot be driven — not on Windows, no powershell.exe, no
// BitsTransfer module, BITS service disabled. This suite also runs on machines
// with none of those, and a binding that turns "not applicable here" into a red
// build teaches everyone to ignore the build.
func withBITS(t *testing.T) *Delegator {
	t.Helper()
	d := New()
	// Deterministic timing for a test: the cmdlet's own default is already
	// Foreground, but saying so means the test does not change meaning if that
	// default ever does.
	d.Priority = "Foreground"
	if err := d.Available(); err != nil {
		t.Skipf("skipping, BITS cannot be driven here: %v", err)
	}
	return d
}

func payload(t *testing.T, n int) ([]byte, string) {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return b, "sha256:" + hex.EncodeToString(sum[:])
}

// staticServer is what BITS actually requires: "the HTTP server's Head method
// must return the file size and its Get method must support the Content-Range
// and Content-Length headers". http.ServeContent does both, which is why the
// body is served through it rather than written straight to the ResponseWriter.
func staticServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	modtime := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing.bin" {
			http.NotFound(w, r)
			return
		}
		http.ServeContent(w, r, "payload.bin", modtime, bytes.NewReader(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// cancelAtEnd makes sure a test never leaves a job in the machine's BITS queue.
// A leaked job sits there for 90 days and counts against MaxJobsPerUser.
func cancelAtEnd(t *testing.T, d *Delegator, id string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = d.Abandon(ctx, id)
	})
}

// waitFor polls until the job reaches one of want, or gives up. It returns the
// last status either way so the caller can say something useful about a job
// that never got there.
func waitFor(t *testing.T, d *Delegator, id string, limit time.Duration, want ...download.DelegateState) download.Status {
	t.Helper()
	deadline := time.Now().Add(limit)
	var last download.Status
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		st, err := d.Poll(ctx, id)
		cancel()
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		last = st
		for _, w := range want {
			if st.State == w {
				return st
			}
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func specFor(t *testing.T, url, digest string, size int64) download.Spec {
	t.Helper()
	dir := t.TempDir()
	return download.Spec{
		Artifact: download.Artifact{Digest: digest, Size: size},
		Sources:  []download.Source{{Scheme: "http", Locator: url}},
		Sink: download.Sink{
			Partial: filepath.Join(dir, "work.part"),
			Final:   filepath.Join(dir, "model.bin"),
		},
	}
}

// ---------- pure mapping, no BITS required: these run everywhere ----------

// TestIdentity: the three constants a stored delegation is interpreted by. If
// System ever changes, every job record written by an older build becomes
// unreadable, so it is worth a test that says so out loud.
func TestIdentity(t *testing.T) {
	d := New()
	if d.System() != "bits" {
		t.Fatalf("System = %q, want \"bits\"", d.System())
	}
	want := map[string]bool{"http": true, "https": true, "smb": true}
	got := d.Schemes()
	if len(got) != len(want) {
		t.Fatalf("Schemes = %v", got)
	}
	for _, s := range got {
		if !want[s] {
			t.Fatalf("Schemes = %v, %q is not one BITS speaks", got, s)
		}
	}
	caps := map[download.Capability]bool{}
	for _, c := range d.Capabilities() {
		caps[c] = true
	}
	for _, c := range []download.Capability{download.CapResume, download.CapSurvivesProcessExit, download.CapDelegates} {
		if !caps[c] {
			t.Fatalf("Capabilities = %v, missing %q", d.Capabilities(), c)
		}
	}
	// The one it must NOT claim. BITS "guarantees that the version of the file
	// it transfers is consistent based on the file size and time stamp, not
	// content". Claiming verification here would tell the Runner it could skip
	// the hash, which is the one check standing between a BranchCache peer and
	// a corrupt 40 GB model.
	if caps[download.CapVerifies] {
		t.Fatal("claimed verifies_content; BITS checks size and timestamp only")
	}
}

// TestTransientErrorIsStillRunning is the mapping most likely to be got wrong
// and most expensive to get wrong. BITS moves a job to TRANSIENT_ERROR when the
// network drops and back to QUEUED when it returns, all by itself, over a retry
// window measured in hours. Calling that failure would make the Runner tear the
// delegation down and throw away every byte of a transfer that was about to
// continue.
func TestTransientErrorIsStillRunning(t *testing.T) {
	for _, name := range []string{"Queued", "Connecting", "Transferring", "Suspended", "TransientError"} {
		if got := stateOf(name); got != download.DelegateRunning {
			t.Fatalf("stateOf(%q) = %q, want running", name, got)
		}
	}
	if got := stateOf("Transferred"); got != download.DelegateTransferred {
		t.Fatalf("stateOf(Transferred) = %q; transferred is NOT complete, but it is not running either", got)
	}
	if got := stateOf("Error"); got != download.DelegateFailed {
		t.Fatalf("stateOf(Error) = %q, want failed", got)
	}
	if got := stateOf("Canceled"); got != download.DelegateFailed {
		t.Fatalf("stateOf(Canceled) = %q, want failed", got)
	}
	if got := stateOf("Acknowledged"); got != download.DelegateGone {
		t.Fatalf("stateOf(Acknowledged) = %q, want gone", got)
	}
	// A state this build has never heard of must not be read as failure either.
	if got := stateOf("SomethingWindows12Invented"); got != download.DelegateRunning {
		t.Fatalf("stateOf(unknown) = %q; guessing failure discards live work", got)
	}
}

// TestSizeUnknownBecomesZero: BytesTotal is BG_SIZE_UNKNOWN until BITS has
// asked the server how big the file is. That value is 0xFFFFFFFFFFFFFFFF, which
// is 18 exabytes as an unsigned number and -1 as a signed one. Status.Total
// already has a word for "not known yet" and it is 0.
func TestSizeUnknownBecomesZero(t *testing.T) {
	if got := bytesOf("18446744073709551615"); got != 0 {
		t.Fatalf("bytesOf(BG_SIZE_UNKNOWN) = %d, want 0", got)
	}
	if got := bytesOf(""); got != 0 {
		t.Fatalf("bytesOf(empty) = %d, want 0", got)
	}
	if got := bytesOf("9223372036854775808"); got != 0 {
		t.Fatalf("bytesOf(MaxInt64+1) = %d, want 0", got)
	}
	// And a real number still survives the round trip, including one past the
	// 2 GB mark that curl's CURLOPT_RESUME_FROM gets wrong.
	if got := bytesOf("42949672960"); got != 42949672960 {
		t.Fatalf("bytesOf(40GiB) = %d", got)
	}
}

// TestHandleFormsBothAccepted: a handle read out of a job record may have been
// written years earlier by another implementation. bitsadmin and the COM API
// print braces; the cmdlets do not.
func TestHandleFormsBothAccepted(t *testing.T) {
	const bare = "8076490f-919e-424c-b9c4-482f884fa286"
	for _, in := range []string{bare, "{" + bare + "}", "  " + bare + "  ", strings.ToUpper(bare)} {
		got, ok := normalizeGUID(in)
		if !ok || !strings.EqualFold(got, bare) {
			t.Fatalf("normalizeGUID(%q) = %q,%v", in, got, ok)
		}
	}
	for _, in := range []string{"", "not-a-guid", "8076490f919e424cb9c4482f884fa286", "'; Remove-Item C:\\ -Recurse; '"} {
		if _, ok := normalizeGUID(in); ok {
			t.Fatalf("normalizeGUID(%q) accepted a non-handle", in)
		}
	}
}

// TestMalformedHandleIsGoneNotError: the Runner answers Gone by taking the work
// back in-process. It answers an error by giving up and leaving the job
// delegated to something that will never report again. So even a handle that is
// obvious nonsense is Gone.
func TestMalformedHandleIsGoneNotError(t *testing.T) {
	d := New()
	st, err := d.Poll(context.Background(), "definitely-not-a-guid")
	if runtime.GOOS != "windows" {
		// Off Windows Poll refuses before it ever looks at the handle, and
		// that is correct too.
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Poll off Windows = %v, want ErrUnavailable", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("Poll of a malformed handle returned an error, which strands the job: %v", err)
	}
	if st.State != download.DelegateGone {
		t.Fatalf("state = %q, want gone", st.State)
	}
}

// ---------- against the real service ----------

// TestUnknownGUIDIsGone is the same rule against a well-formed handle BITS has
// simply never heard of, which is what a 90-day reap, a rebuilt machine or a
// discarded queue database all look like from here.
func TestUnknownGUIDIsGone(t *testing.T) {
	d := withBITS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	st, err := d.Poll(ctx, "11111111-2222-3333-4444-555555555555")
	if err != nil {
		t.Fatalf("Poll of an unknown GUID must not error — the Runner falls back on Gone and gives up on an error: %v", err)
	}
	if st.State != download.DelegateGone {
		t.Fatalf("state = %q, want gone", st.State)
	}
	// And cancelling something that is already gone is a success, because the
	// outcome Abandon exists to produce has already happened.
	if err := d.Abandon(ctx, "11111111-2222-3333-4444-555555555555"); err != nil {
		t.Fatalf("Abandon of an unknown GUID = %v, want nil", err)
	}
}

// TestStartPollFinalize is the whole two-phase shape in one test: BITS fetches
// the bytes, reports Transferred, and the file still does not exist at its
// final path — because "the transferred files are not available to the client
// until the application calls the IBackgroundCopyJob::Complete method".
func TestStartPollFinalize(t *testing.T) {
	d := withBITS(t)
	body, digest := payload(t, 512<<10)
	srv := staticServer(t, body)
	spec := specFor(t, srv.URL+"/payload.bin", digest, int64(len(body)))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	id, err := d.Start(ctx, spec, 0)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancelAtEnd(t, d, id)
	if _, ok := normalizeGUID(id); !ok {
		t.Fatalf("Start returned %q, which is not a job GUID and cannot be stored as a handle", id)
	}

	st := waitFor(t, d, id, 90*time.Second, download.DelegateTransferred, download.DelegateFailed, download.DelegateGone)
	if st.State != download.DelegateTransferred {
		t.Fatalf("state = %q (%s), want transferred", st.State, st.Err)
	}
	if st.Done != int64(len(body)) {
		t.Fatalf("Done = %d, want %d", st.Done, len(body))
	}
	if st.Total != int64(len(body)) {
		t.Fatalf("Total = %d, want %d — a BG_SIZE_UNKNOWN leaking through would show here", st.Total, len(body))
	}

	if _, err := os.Stat(spec.Sink.Final); err == nil {
		t.Fatal("the file existed at its final path before Finalize; BITS is supposed to withhold it until Complete()")
	}

	if err := d.Finalize(ctx, id); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got, err := os.ReadFile(spec.Sink.Final)
	if err != nil {
		t.Fatalf("after Finalize the file should be ours: %v", err)
	}
	sum := sha256.Sum256(got)
	if "sha256:"+hex.EncodeToString(sum[:]) != digest {
		t.Fatal("delivered bytes differ from the source")
	}

	// After Complete the job leaves the queue, so the handle stops resolving.
	// That is Gone, not an error, and it is why Runner records Delivered rather
	// than asking the delegate a second time.
	after, err := d.Poll(ctx, id)
	if err != nil {
		t.Fatalf("polling a completed job errored: %v", err)
	}
	if after.State != download.DelegateGone {
		t.Logf("state after Finalize = %q (BITS had not dropped the job yet)", after.State)
	}
}

// TestPollReportsProgressWhileRunning watches a transfer that is deliberately
// slowed down, so that at least one poll lands mid-flight. Progress observable
// from a process that did not start the transfer is the property a COM callback
// cannot have, and the reason Poll exists at all.
func TestPollReportsProgressWhileRunning(t *testing.T) {
	d := withBITS(t)
	body, digest := payload(t, 4<<20)

	// A server that trickles. BITS still gets its HEAD size and its ranges;
	// it just does not get them quickly.
	modtime := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "slow.bin", modtime, &slowReader{data: body, chunk: 128 << 10, delay: 120 * time.Millisecond})
	}))
	defer srv.Close()

	spec := specFor(t, srv.URL+"/slow.bin", digest, int64(len(body)))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	id, err := d.Start(ctx, spec, 0)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancelAtEnd(t, d, id)

	sawRunning := false
	deadline := time.Now().Add(120 * time.Second)
	var st download.Status
	for time.Now().Before(deadline) {
		st, err = d.Poll(ctx, id)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if st.State == download.DelegateRunning {
			sawRunning = true
			if st.Done < 0 || (st.Total > 0 && st.Done > st.Total) {
				t.Fatalf("nonsensical progress %d/%d", st.Done, st.Total)
			}
		}
		if st.State == download.DelegateTransferred || st.State == download.DelegateFailed {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if st.State != download.DelegateTransferred {
		t.Fatalf("final state = %q (%s), want transferred", st.State, st.Err)
	}
	if !sawRunning {
		// Not a failure: a fast enough machine can legitimately go from Start
		// to Transferred between two polls. Worth knowing when it happens.
		t.Log("never observed a running poll; the transfer finished between polls")
	}
	if err := d.Finalize(ctx, id); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if fi, err := os.Stat(spec.Sink.Final); err != nil || fi.Size() != int64(len(body)) {
		t.Fatalf("final file: %v", err)
	}
}

// TestAbandonCleansUp. BITS Cancel deletes the completed file as well as the
// partial, so this also demonstrates the thing the interface warns about:
// Abandon is not a way to keep what has arrived so far.
func TestAbandonCleansUp(t *testing.T) {
	d := withBITS(t)
	body, digest := payload(t, 256<<10)
	srv := staticServer(t, body)
	spec := specFor(t, srv.URL+"/payload.bin", digest, int64(len(body)))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	id, err := d.Start(ctx, spec, 0)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := d.Abandon(ctx, id); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	st, err := d.Poll(ctx, id)
	if err != nil {
		t.Fatalf("Poll after Abandon errored: %v", err)
	}
	if st.State != download.DelegateGone {
		t.Fatalf("state after Abandon = %q, want gone", st.State)
	}
	if _, err := os.Stat(spec.Sink.Final); err == nil {
		t.Fatal("Abandon left a file behind at the final path")
	}
	// Twice is a success. The Runner calls Abandon on a recovery path where the
	// goal is that nothing is left, and something already gone satisfies that.
	if err := d.Abandon(ctx, id); err != nil {
		t.Fatalf("second Abandon = %v, want nil", err)
	}
}

// TestFailedSourceIsFailedNotGone: a 404 is BITS's business to report and ours
// to pass on with its reason attached. Failed and Gone lead to the same
// recovery, but only Failed carries something a human can read out of the job
// record afterwards, without finding the log of a process that no longer exists.
func TestFailedSourceIsFailedNotGone(t *testing.T) {
	d := withBITS(t)
	body, digest := payload(t, 4<<10)
	srv := staticServer(t, body)
	spec := specFor(t, srv.URL+"/missing.bin", digest, int64(len(body)))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	id, err := d.Start(ctx, spec, 0)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancelAtEnd(t, d, id)

	st := waitFor(t, d, id, 60*time.Second, download.DelegateFailed, download.DelegateTransferred)
	if st.State == download.DelegateTransferred {
		t.Fatal("BITS reported a 404 as a successful transfer")
	}
	if st.State != download.DelegateFailed {
		// BITS's own retry policy is measured in hours, so a slow machine may
		// still have it parked in TRANSIENT_ERROR. That is the mapping working,
		// not failing.
		t.Skipf("job was still %q after 60s (BITS retry policy); nothing to assert", st.State)
	}
	if st.Err == "" {
		t.Fatal("a failed job reported no reason; the job record would say nothing useful")
	}
	t.Logf("BITS reported: %s", st.Err)
}

// TestRunnerDelegateAndReconcile is the path that actually matters: the real
// Runner, a real BITS job, and Reconcile driven by a process that could have
// been started after a reboot. Delegate must let go of the lease, Reconcile
// must track progress without taking one, and the finish must go through
// Finalize and then be verified here rather than trusted.
func TestRunnerDelegateAndReconcile(t *testing.T) {
	d := withBITS(t)
	body, digest := payload(t, 1<<20)
	srv := staticServer(t, body)

	root := t.TempDir()
	store, err := job.NewFileStore(filepath.Join(root, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "models", "model.bin")

	id, err := download.Submit(store, download.Spec{
		Artifact: download.Artifact{Digest: digest, Size: int64(len(body))},
		Sources:  []download.Source{{Scheme: "http", Locator: srv.URL + "/payload.bin"}},
		Sink:     download.Sink{Final: final},
	}, string(download.CapSurvivesProcessExit))
	if err != nil {
		t.Fatal(err)
	}

	runner := download.NewRunner(store, "bits-integration")
	runner.Delegators = download.NewDelegators(d)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := runner.Delegate(ctx, id); err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Delegated() || rec.Delegation.System != "bits" {
		t.Fatalf("delegation not recorded usably: %+v", rec.Delegation)
	}
	handle := rec.Delegation.ExternalID
	cancelAtEnd(t, d, handle)
	if !store.Claimable(rec) {
		t.Fatal("Delegate kept the lease; nobody else could poll or finalise, which makes the delegation pointless")
	}

	// A completely separate Runner, as a service on a timer after a reboot
	// would be. It has never seen this job before and knows it only by the
	// {system, external_id} pair in the record.
	watcher := download.NewRunner(store, "watcher-after-reboot")
	watcher.Delegators = download.NewDelegators(New())

	deadline := time.Now().Add(120 * time.Second)
	for {
		if err := watcher.Reconcile(ctx, id); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		rec, err = store.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		if rec.State == job.StateTransferred {
			break
		}
		if !rec.Delegated() {
			t.Fatalf("the delegation was dropped before delivery: state=%s error=%q", rec.State, rec.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job never reached transferred: state=%s progress=%d/%d",
				rec.State, rec.Progress.Done, rec.Progress.Total)
		}
		time.Sleep(300 * time.Millisecond)
	}

	if !rec.Delegation.Delivered {
		t.Fatal("delivery was not recorded; a later sweep would try to finalise the job twice")
	}
	if rec.Progress.Done != int64(len(body)) {
		t.Fatalf("progress = %d, want %d", rec.Progress.Done, len(body))
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("the artifact is not at its final path: %v", err)
	}
	sum := sha256.Sum256(got)
	if "sha256:"+hex.EncodeToString(sum[:]) != digest {
		t.Fatal("the Runner accepted bytes that do not match the digest")
	}
}

// TestSignedRedirectingCDN is the survey's single biggest practical risk, which
// it flagged UNTESTED: BITS requires static content whose GET honours
// Content-Range, and most model hosting is a 302 to a signed, expiring CDN URL.
//
// It is a network test, so it skips rather than fails when the network is not
// there. What it cannot cover is a signature expiring mid-transfer: the file is
// deliberately small, and the signature on that redirect was measured at about
// an hour of validity — which a real 40 GB model over BITS would outlive.
func TestSignedRedirectingCDN(t *testing.T) {
	if testing.Short() {
		t.Skip("network test")
	}
	d := withBITS(t)

	// A real LFS-backed HuggingFace object: huggingface.co answers 302 with a
	// Location on a CDN host carrying Expires, Policy, Signature and
	// Key-Pair-Id query parameters.
	const url = "https://huggingface.co/hf-internal-testing/tiny-random-gpt2/resolve/main/pytorch_model.bin"

	resp, err := http.Head(url)
	if err != nil {
		t.Skipf("skipping, no network: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("skipping, %s answered %s", url, resp.Status)
	}
	if resp.Header.Get("Accept-Ranges") != "bytes" {
		t.Skipf("skipping, the CDN stopped advertising ranges; BITS's precondition no longer holds")
	}

	dir := t.TempDir()
	spec := download.Spec{
		Sources: []download.Source{{Scheme: "https", Locator: url}},
		Sink:    download.Sink{Final: filepath.Join(dir, "cdn.bin")},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	id, err := d.Start(ctx, spec, 0)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancelAtEnd(t, d, id)

	st := waitFor(t, d, id, 120*time.Second, download.DelegateTransferred, download.DelegateFailed)
	if st.State != download.DelegateTransferred {
		// Report it rather than hide it. If BITS cannot do signed redirecting
		// CDN URLs, that is the finding, and the facade needs to resolve the
		// redirect chain before delegating.
		t.Fatalf("BITS could not fetch a signed, redirecting CDN URL: state=%q err=%q", st.State, st.Err)
	}
	if err := d.Finalize(ctx, id); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	fi, err := os.Stat(spec.Sink.Final)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != st.Total {
		t.Fatalf("delivered %d bytes, BITS reported %d", fi.Size(), st.Total)
	}
	t.Logf("BITS followed the 302 and fetched %d bytes from the signed CDN URL", fi.Size())
}

// slowReader is a ReadSeeker that trickles, so that a local transfer is slow
// enough for a poll to catch it in flight. http.ServeContent needs the Seeker
// to answer ranges and to discover the length.
type slowReader struct {
	data  []byte
	pos   int64
	chunk int
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	if s.pos >= int64(len(s.data)) {
		return 0, io.EOF
	}
	time.Sleep(s.delay)
	n := len(p)
	if n > s.chunk {
		n = s.chunk
	}
	if int64(n) > int64(len(s.data))-s.pos {
		n = int(int64(len(s.data)) - s.pos)
	}
	copy(p[:n], s.data[s.pos:s.pos+int64(n)])
	s.pos += int64(n)
	return n, nil
}

func (s *slowReader) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case 0:
		s.pos = off
	case 1:
		s.pos += off
	case 2:
		s.pos = int64(len(s.data)) + off
	}
	return s.pos, nil
}
