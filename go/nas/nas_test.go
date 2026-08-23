package nas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	download "github.com/ReinisLusis/abstraction-download"
	job "github.com/ReinisLusis/abstraction-job"
)

// twoMachines stands up both halves of the arrangement in one process: a local
// store, and a "remote" store standing in for the directory a NAS mounts. They
// are two different roots, which is the only thing that made the real thing
// hard — the record has to mean the same work in both.
type twoMachines struct {
	local, remote *job.FileStore
	localRoot     string
	remoteRoot    string
	del           *Delegator
	runner        *download.Runner
	body          []byte
	digest        string
	url           string
}

func setup(t *testing.T) *twoMachines {
	t.Helper()
	root := t.TempDir()
	localRoot := filepath.Join(root, "pc")
	remoteRoot := filepath.Join(root, "nas")

	local, err := job.NewFileStore(localRoot)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := job.NewFileStore(remoteRoot)
	if err != nil {
		t.Fatal(err)
	}

	body := make([]byte, 64<<10)
	rand.New(rand.NewSource(1)).Read(body)
	sum := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "model.gguf", time.Unix(0, 0), strings.NewReader(string(body)))
	}))
	t.Cleanup(srv.Close)

	d, err := New(remoteRoot)
	if err != nil {
		t.Fatal(err)
	}
	r := download.NewRunner(local, "pc")
	r.Delegators = download.NewDelegators(d)

	return &twoMachines{
		local: local, remote: remote,
		localRoot: localRoot, remoteRoot: remoteRoot,
		del: d, runner: r,
		body: body, digest: digest, url: srv.URL + "/model.gguf",
	}
}

// theOtherMachine is what jobd does over there: sweep the store, adopt anything
// stranded, do the work. It shares no memory with the local runner — only the
// directory.
func (m *twoMachines) theOtherMachine(t *testing.T) int {
	t.Helper()
	r := download.NewRunner(m.remote, "jobd@nas")
	n, err := r.Adopt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (m *twoMachines) submit(t *testing.T, final string) string {
	t.Helper()
	id, err := download.Submit(m.local, download.Spec{
		Artifact: download.Artifact{Digest: m.digest, Size: int64(len(m.body))},
		Sources:  []download.Source{{Scheme: "https", Locator: m.url}},
		Sink:     download.Sink{Final: final},
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// The whole arrangement, end to end: a job submitted here, done there, and
// delivered back here with its digest checked on arrival. No socket, no
// protocol, no agent — one directory both machines can see.
func TestWorkCrossesToTheOtherMachineAndBack(t *testing.T) {
	m := setup(t)
	ctx := context.Background()
	dest := filepath.Join(m.localRoot, "..", "downloads", "model.gguf")

	id := m.submit(t, dest)
	if err := m.runner.Delegate(ctx, id); err != nil {
		t.Fatalf("Delegate: %v", err)
	}

	// The local record now names the far side, and holds no lease — this
	// machine is free to exit.
	rec, err := m.local.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Delegated() || rec.Delegation.System != System {
		t.Fatalf("delegation not recorded: %+v", rec.Delegation)
	}
	if !m.local.Claimable(rec) {
		t.Fatal("the lease was held after delegating; nobody else could poll or finalise")
	}
	if rec.State != job.StateRunning {
		t.Fatalf("state = %s; the work is running on the other machine", rec.State)
	}

	// Nothing here may pick this up while the far side has it.
	if n, err := m.runner.Adopt(ctx); err != nil || n != 0 {
		t.Fatalf("Adopt took %d delegated job(s) (err %v); the bytes would be fetched twice", n, err)
	}

	// Over there, a supervisor that has never heard of this process finds the
	// job and does it.
	if n := m.theOtherMachine(t); n != 1 {
		t.Fatalf("the other machine adopted %d jobs, want 1", n)
	}

	// Back here: poll, take delivery, verify.
	if err := m.runner.Reconcile(ctx, id); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("the file never arrived: %v", err)
	}
	if string(got) != string(m.body) {
		t.Fatal("the delivered bytes are not the ones published")
	}
	final, err := m.local.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != job.StateTransferred {
		t.Fatalf("local state = %s, want transferred", final.State)
	}

	// And the far side has been told, so its own supervisor stops caring.
	remoteRec, err := m.remote.Load(rec.Delegation.ExternalID)
	if err != nil {
		t.Fatal(err)
	}
	if remoteRec.State != job.StateComplete {
		t.Fatalf("remote state = %s; the far side was never told delivery happened", remoteRec.State)
	}
}

// When the destination is already on the share, delivery is a no-op rather than
// a copy of the file onto itself. This is the case that matters for the machine
// whose model directory IS the NAS.
func TestDestinationOnTheShareIsNotCopied(t *testing.T) {
	m := setup(t)
	ctx := context.Background()

	// The local job's destination resolves to the very file the far side wrote.
	dest := filepath.Join(m.remoteRoot, "models", "model.gguf")
	id := m.submit(t, dest)
	if err := m.runner.Delegate(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n := m.theOtherMachine(t); n != 1 {
		t.Fatalf("the other machine adopted %d jobs, want 1", n)
	}

	before, err := os.Stat(dest)
	if err != nil {
		t.Fatalf("the far side did not write where expected: %v", err)
	}
	if err := m.runner.Reconcile(ctx, id); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	after, err := os.Stat(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("the file was replaced; delivery copied it over itself")
	}
	if _, err := os.Stat(dest + ".partial"); err == nil {
		t.Fatal("a copy was started even though source and destination are the same file")
	}
}

// A handle the far side has never heard of must not strand the job. Stores get
// cleaned out and NAS boxes get rebuilt; the job has to come back to this
// machine with its sources intact so an ordinary local run can finish it.
func TestUnknownHandleReturnsTheJob(t *testing.T) {
	m := setup(t)
	ctx := context.Background()
	dest := filepath.Join(m.localRoot, "..", "downloads", "model.gguf")

	id := m.submit(t, dest)
	if err := m.runner.Delegate(ctx, id); err != nil {
		t.Fatal(err)
	}
	rec, _ := m.local.Load(id)

	// The far side loses everything.
	if err := os.RemoveAll(filepath.Join(m.remoteRoot, "jobs")); err != nil {
		t.Fatal(err)
	}
	st, err := m.del.Poll(ctx, rec.Delegation.ExternalID)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != download.DelegateGone {
		t.Fatalf("Poll = %s, want gone", st.State)
	}

	if err := m.runner.Reconcile(ctx, id); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	back, err := m.local.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if back.Delegated() {
		t.Fatal("the job still points at a handle nobody can resolve")
	}
	if back.State != job.StatePending {
		t.Fatalf("state = %s; the job should be available to run here", back.State)
	}

	// And now this machine can simply do it.
	if n, err := m.runner.Adopt(ctx); err != nil || n != 1 {
		t.Fatalf("Adopt = %d (err %v); the job was stranded", n, err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != string(m.body) {
		t.Fatalf("local fallback did not deliver the file: %v", err)
	}
}

// The record written for the far side must not name a path only this machine
// can resolve — that is the mistake the whole relative-sink change exists to
// prevent, and it is worth asserting on the bytes rather than on the struct.
func TestRemoteRecordNamesNoLocalPath(t *testing.T) {
	m := setup(t)
	dest := filepath.Join(m.localRoot, "..", "downloads", "model.gguf")

	id := m.submit(t, dest)
	if err := m.runner.Delegate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	rec, _ := m.local.Load(id)

	raw, err := os.ReadFile(filepath.Join(m.remoteRoot, "jobs", rec.Delegation.ExternalID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), m.localRoot) || strings.Contains(string(raw), "downloads") {
		t.Fatalf("the remote record names this machine's filesystem:\n%s", raw)
	}
	if strings.Contains(string(raw), `\\`) {
		t.Fatalf("the remote record uses this machine's separator:\n%s", raw)
	}
}
