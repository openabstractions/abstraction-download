package nas

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestLiveRecordFreshness documents a limitation that is real, measured, and
// not fixed.
//
// The far side writes records through its LOCAL filesystem. Samba therefore
// never learns the file changed, never sends a lease break, and a Windows client
// holding an SMB2 read lease goes on serving its cached copy. In the live round
// trip this process read "0%" for ten minutes after the NAS had finished, then
// saw the finished state all at once.
//
// It is a latency defect, not a correctness one, and the difference is worth
// being precise about:
//
//   - Delivery was correct. The file arrived and its digest matched.
//   - The lease protocol is unaffected. Claiming means CREATING a file with
//     O_EXCL, and writes are not served from cache — TestLiveExclusiveCreate
//     proves the exclusion still holds. A stale read can only make a job look
//     claimable when it is not, and the create then fails, which is safe.
//   - What breaks is observation: "what is downloading?" answered from the other
//     machine can be minutes out of date.
//
// Reading differently does not help. os.ReadFile, opening O_RDWR, stat-then-read
// and readdir-then-read were all measured stale for 15s+, and records are
// already written atomically via temp-and-rename, so it is content leasing
// rather than the metadata cache. FileInfoCacheLifetime was at its default of 10
// seconds and is not the binding constraint.
//
// The fix is on the NAS: turn off oplocks for the share, so Samba stops handing
// out leases it cannot break. That is a change to the owner's NAS and is
// deliberately not made from here.
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

	write := func(v string) {
		t.Helper()
		cmd := exec.Command("ssh", "-o", "BatchMode=yes", host,
			fmt.Sprintf("printf '%%s' '%s' > ~/abstraction-store/freshness-probe.txt", v))
		if out, err := cmd.CombinedOutput(); err != nil {
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
		"This is the SMB read-lease staleness described above: correctness is unaffected, "+
		"but progress read across the share is not live. Disable oplocks on the share to fix it.", budget)
}
