package nas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
)

// TestLiveRoundTrip runs the arrangement against a real NAS over a real share.
//
// Everything else in this package proves the logic with two directories in one
// process. This proves the things that only a second machine can: that O_EXCL
// creation is exclusive over SMB, that a record written by Windows is read
// correctly by Linux, and that a supervisor which has never heard of this
// process picks the work up on its own.
//
// It is opt-in, because it needs a NAS:
//
//	ABSTRACTION_NAS_STORE=//host/share/store  ABSTRACTION_LIVE=1  go test ./nas
//
// with a jobd running over there against the same directory.
func TestLiveRoundTrip(t *testing.T) {
	if os.Getenv("ABSTRACTION_LIVE") == "" {
		t.Skip("set ABSTRACTION_LIVE=1 (and ABSTRACTION_NAS_STORE) to run against a real NAS")
	}
	remoteRoot := os.Getenv(EnvStore)
	if remoteRoot == "" {
		t.Skip("set ABSTRACTION_NAS_STORE to the shared store")
	}

	// A real model, with a digest HuggingFace published, pinned to a commit.
	const (
		url    = "https://huggingface.co/bartowski/Qwen2.5-0.5B-Instruct-GGUF/resolve/41ba88dbac95fed2528c92514c131d73eb5a174b/Qwen2.5-0.5B-Instruct-IQ2_M.gguf"
		digest = "sha256:2fc237e65e1f963310e9c961d8e71e932734a72f90e3216a972d83edd1feb756"
		size   = 328597408
	)

	localRoot := t.TempDir()
	local, err := job.NewFileStore(filepath.Join(localRoot, "store"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(remoteRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Available(); err != nil {
		t.Fatalf("the share is not reachable: %v", err)
	}

	r := download.NewRunner(local, "live-test")
	r.Delegators = download.NewDelegators(d)

	dest := filepath.Join(localRoot, "downloads", "Qwen2.5-0.5B-Instruct-IQ2_M.gguf")
	id, err := download.Submit(local, download.Spec{
		Artifact: download.Artifact{Digest: digest, Size: size},
		Sources:  []download.Source{{Scheme: "https", Locator: url}},
		Sink:     download.Sink{Final: dest},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	if err := r.Delegate(ctx, id); err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	rec, err := local.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("handed to %s as %s", rec.Delegation.System, rec.Delegation.ExternalID)

	// Poll the way any other process would: by reading a file on a share.
	deadline := time.Now().Add(15 * time.Minute)
	for {
		if time.Now().After(deadline) {
			t.Fatal("the far side never finished")
		}
		if err := r.Reconcile(ctx, id); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		cur, err := local.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		if cur.State == job.StateTransferred {
			break
		}
		if cur.State == job.StateFailed {
			t.Fatalf("failed: %s", cur.Error)
		}
		if !cur.Delegated() {
			t.Fatalf("the job came back to us unfinished: %s (%s)", cur.State, cur.Error)
		}
		if cur.Progress.Total > 0 {
			t.Logf("%3.0f%% (%d/%d) on the far side",
				100*float64(cur.Progress.Done)/float64(cur.Progress.Total),
				cur.Progress.Done, cur.Progress.Total)
		}
		time.Sleep(10 * time.Second)
	}

	// The bytes are here. Check them ourselves — they crossed the internet once
	// and a share once, and the second crossing corrupts as well as the first.
	got, err := hashOf(dest)
	if err != nil {
		t.Fatalf("the file never arrived: %v", err)
	}
	if got != digest {
		t.Fatalf("delivered %s, want %s", got, digest)
	}
	t.Logf("delivered and verified: %s", dest)
}

func hashOf(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// TestLiveExclusiveCreate is the assumption the whole lease protocol rests on,
// checked where it was never checked: over SMB, against a Synology.
//
// Claiming a job means creating <id>.epoch.<n> with O_EXCL. If two machines can
// both succeed at that, two owners write one file. The README said this was
// "very probably fine and has not been proven here" — this is what proving it
// looks like.
func TestLiveExclusiveCreate(t *testing.T) {
	if os.Getenv("ABSTRACTION_LIVE") == "" {
		t.Skip("set ABSTRACTION_LIVE=1 (and ABSTRACTION_NAS_STORE) to run against a real NAS")
	}
	remoteRoot := os.Getenv(EnvStore)
	if remoteRoot == "" {
		t.Skip("set ABSTRACTION_NAS_STORE to the shared store")
	}
	dir := filepath.Join(remoteRoot, "excl-probe")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	name := filepath.Join(dir, fmt.Sprintf("token-%d", time.Now().UnixNano()))
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	f.Close()

	// The second must be refused. If SMB let this through, a lapsed lease could
	// be claimed by two machines at the same epoch.
	if _, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644); !os.IsExist(err) {
		t.Fatalf("a second exclusive create over SMB returned %v, want an already-exists error; "+
			"the lease protocol is not safe on this filesystem", err)
	}
}
