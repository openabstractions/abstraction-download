package download

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// The confused deputy, at the sink field, one spelling past the containment
// that closed the relative case.
//
// A record on a shared store names an ABSOLUTE sink — /etc/cron.d/evil on the
// NAS. Containment never saw it: an absolute path is never joined onto the store
// root, so it climbs out of nothing. ForeignPath refuses only the OTHER
// platform's spelling, so a POSIX path on the NAS passes. The bytes land where
// the record chose, written with the supervisor's authority.
//
// A runner over a shared store refuses it; a bare runner (dl writing to its own
// machine) still honours it.
func TestSharedStoreRefusesAnAbsoluteSink(t *testing.T) {
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}

	// This machine's own convention, so ForeignPath does not catch it — the
	// exact hole: a native absolute sink from a foreign submitter.
	native := "/etc/cron.d/evil"
	if runtime.GOOS == "windows" {
		native = `C:\Windows\System32\evil.exe`
	}

	src := []Source{{Scheme: "file", Locator: filepath.Join(root, "src.bin")}}
	if err := os.WriteFile(filepath.Join(root, "src.bin"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	id, err := Submit(store, Spec{Sources: src, Sink: Sink{Final: native}})
	if err != nil {
		t.Fatalf("a native absolute sink passes submission (it is valid on the machine that wrote it): %v", err)
	}

	shared := NewRunner(store, "nas")
	shared.SharedStore = true
	if err := shared.Run(context.Background(), id); !errors.Is(err, ErrUnportableSink) {
		t.Fatalf("a shared store wrote an absolute sink a record chose: %v", err)
	}
	if _, statErr := os.Stat(native); statErr == nil {
		t.Fatalf("bytes landed at the record's absolute path %q", native)
	}

	// The refusal is adoptable, not fatal: the same record is legitimate on the
	// machine that wrote it.
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State == job.StateFailed {
		t.Fatal("a machine's shared-store policy ended the job for every machine")
	}
}
