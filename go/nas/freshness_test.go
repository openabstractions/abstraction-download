package nas

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestLiveRecordFreshness measures how long a change made on the other machine
// takes to become visible here.
//
// The far side writes records through its LOCAL filesystem, so Samba is never
// told the file changed and a Windows client can serve a cached copy.
//
// The evidence here does not reconcile, and saying so is more useful than
// picking whichever half flatters the design:
//
//   - THE PRODUCT PATH WORKS. Against a live NAS download, successive `modelget
//     resume` invocations reported 37% -> 75% -> 100%, and delivery verified.
//     That is the path anybody actually uses.
//   - THIS PROBE DOES NOT. A single file on the same share, changed by the far
//     side, stayed stale here for 30s+.
//
// Three explanations were tested and each is dead. It is not per-process — a
// fresh `cmd /c type` read the stale copy too. It is not a foreign open
// refreshing the lease — this process stayed stale immediately after one. And it
// is not the write pattern: temp-and-rename, which is exactly what
// FileStore.write() does, was as stale as an in-place truncate.
//
// So the mechanism is uncharacterised. What differs between the two cases is
// most likely traffic — during a real download the far side is writing records,
// partials and epoch tokens continuously, and this probe touches one idle file —
// but that was not confirmed and should not be written down as though it were.
//
// The lease protocol is unaffected in either case, which is the part that
// matters. Claiming means CREATING a file with O_EXCL, and writes are not served
// from cache; TestLiveExclusiveCreate proves the exclusion holds against this
// Synology. A stale read can only make a job look claimable when it is not, and
// the create then fails. It degrades safely.
//
// This test is therefore a canary, not a specification. If it fails, progress
// reported across the share may lag; nothing will be corrupted.
func TestLiveRecordFreshness(t *testing.T) {
	if os.Getenv("ABSTRACTION_LIVE") == "" {
		t.Skip("set ABSTRACTION_LIVE=1 (and ABSTRACTION_NAS_STORE) to measure this against a real NAS")
	}
	root := os.Getenv(EnvStore)
	if root == "" {
		t.Skip("set ABSTRACTION_NAS_STORE to the shared store")
	}
	host := os.Getenv("ABSTRACTION_NAS_SSH")
	if host == "" {
		t.Skip("set ABSTRACTION_NAS_SSH=admin@host to let this test write from the other side")
	}

	probe := filepath.Join(root, "freshness-probe.txt")
	defer os.Remove(probe)

	// Where the store lives as the OTHER machine sees it. There is no way to
	// derive this from the share path, which is the whole point of the exercise.
	remoteStore := os.Getenv("ABSTRACTION_NAS_REMOTE_STORE")
	if remoteStore == "" {
		remoteStore = "~/abstraction-store"
	}

	write := func(v string) {
		t.Helper()
		args := []string{"-o", "BatchMode=yes"}
		if key := os.Getenv("ABSTRACTION_NAS_SSH_KEY"); key != "" {
			args = append(args, "-i", key)
		}
		args = append(args, host,
			fmt.Sprintf("printf '%%s' '%s' > %s/freshness-probe.txt", v, remoteStore))
		if out, err := exec.Command("ssh", args...).CombinedOutput(); err != nil {
			t.Fatalf("writing from the other side: %v %s", err, out)
		}
	}

	write("first")
	if b, err := os.ReadFile(probe); err != nil {
		t.Fatal(err)
	} else if string(b) != "first" {
		t.Fatalf("priming read got %q", b)
	}

	write("second")

	const budget = 30 * time.Second
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(probe)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) == "second" {
			t.Logf("a change made on the other machine became visible in %s", time.Until(deadline))
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("a change made on the other machine was still invisible after %s. "+
		"Progress reported across this share may lag; nothing is corrupted by it, because "+
		"claiming is a write and writes are not cached. Turning off oplocks for the share is "+
		"the usual remedy. See the comment above for what has and has not been ruled out.", budget)
}
