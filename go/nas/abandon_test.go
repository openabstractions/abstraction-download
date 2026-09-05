package nas

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
)

// Abandoning one job must not delete a file another job is still counting on.
//
// Two runs fetching the same artifact name the same file — that is the identity
// rule this whole layer is built on — so a finished job and an abandoned one
// routinely point at one path. Deleting it unconditionally destroyed a
// COMPLETED download on a live NAS: an old cancelled job was abandoned, took
// the 3.1 GB the current job had just finished fetching, and did it again every
// sweep, so the transfer could never be finalised.
func TestAbandonDoesNotDeleteAFileAnotherJobStillWants(t *testing.T) {
	root := t.TempDir()
	d, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, DefaultDir), 0o755); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, DefaultDir, "shared.bin")
	if err := os.WriteFile(final, []byte("the bytes somebody is waiting for"), 0o644); err != nil {
		t.Fatal(err)
	}

	spec := download.Spec{
		Artifact: download.Artifact{Size: 33},
		Sources:  []download.Source{{Scheme: "https", Locator: "https://example.invalid/shared.bin"}},
		Sink:     download.Sink{Final: DefaultDir + "/shared.bin"},
	}

	// The one that finished and is waiting to be collected.
	keeper, err := download.Submit(d.store, spec)
	if err != nil {
		t.Fatal(err)
	}
	held, err := d.store.Claim(keeper, "far-side", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.store.Update(keeper, held.Lease.Epoch, func(r *job.Record) error {
		r.State = job.StateTransferred
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// The stale one, pointing at the same file, about to be abandoned.
	doomed, err := download.Submit(d.store, spec)
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Abandon(context.Background(), doomed); err != nil {
		t.Fatalf("abandon: %v", err)
	}

	if _, err := os.Stat(final); err != nil {
		t.Fatal("abandoning one job deleted the bytes another job had finished fetching; " +
			"on a real NAS this destroyed a completed 3.1 GB download and did it again every sweep")
	}

	// And with nobody else waiting, the bytes DO go — the contract says Abandon
	// cleans up after itself, and BITS deletes completed files too.
	if err := d.Abandon(context.Background(), keeper); err != nil {
		t.Fatalf("abandon keeper: %v", err)
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatal("the last job holding this file was abandoned and the file was left behind")
	}
}
