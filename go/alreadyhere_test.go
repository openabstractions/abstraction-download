package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// finished puts a complete record for body in the store, with its file where
// the record says it is.
func finished(t *testing.T, svc Client, store job.Store, final string, body []byte) (string, string) {
	t.Helper()
	sum := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	src := filepath.Join(t.TempDir(), "src.bin")
	os.WriteFile(src, body, 0o644)
	h, err := svc.Submit(Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources:  []Source{{Scheme: "file", Locator: src}},
		Sink:     Sink{Final: final},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, abs, err := LocalSink(store, h.ID(), Sink{Final: final})
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Dir(abs), 0o755)
	os.WriteFile(abs, body, 0o644)
	rec, err := store.Claim(h.ID(), "someone", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(h.ID(), rec.Lease.Epoch, func(r *job.Record) error {
		r.State = job.StateComplete
		r.Progress.Done = int64(len(body))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return h.ID(), digest
}

// The founding complaint, at the client: bytes already at the destination are
// not work, and bytes already anywhere in the store are a local copy.
func TestProvenBytesAreNotFetchedAgain(t *testing.T) {
	svc, store := clientOn(t)
	body := []byte("the same forty gigabytes, in miniature")
	first, digest := finished(t, svc, store, "out/x.bin", body)

	again, err := svc.Submit(Spec{
		Artifact: Artifact{Digest: digest},
		Sources:  []Source{{Scheme: "https", Locator: "https://example.invalid/x.bin"}},
		Sink:     Sink{Final: "out/x.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID() != first {
		t.Fatalf("delivered bytes were submitted as new work: %s, want %s", again.ID(), first)
	}

	elsewhere, err := svc.Submit(Spec{
		Artifact: Artifact{Digest: digest},
		Sources:  []Source{{Scheme: "https", Locator: "https://example.invalid/x.bin"}},
		Sink:     Sink{Final: "out/y.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := store.Load(elsewhere.ID())
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := SpecOf(rec)
	if len(spec.Sources) != 2 || spec.Sources[0].Scheme != "file" || spec.Sources[0].Attrs["job"] != first {
		t.Fatalf("the store's own copy is not the first source: %+v", spec.Sources)
	}

	_, abs, _ := LocalSink(store, first, Sink{Final: "out/x.bin"})
	os.WriteFile(abs, body[:3], 0o644)
	if got := proven(store, digest, ""); len(got) != 0 {
		t.Fatalf("a file that is not the size it proved was offered: %+v", got)
	}
}

// A supervisor holding the bytes keeps the job rather than handing it to a
// delegate that would fetch them again.
func TestProvenBytesAreNotDelegated(t *testing.T) {
	svc, store := clientOn(t)
	body := []byte("held here")
	_, digest := finished(t, svc, store, "out/a.bin", body)
	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest},
		Sources:  []Source{{Scheme: "https", Locator: "https://example.invalid/a.bin"}},
		Sink:     Sink{Final: "out/b.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "sweeper")
	r.Delegators = NewDelegators(newFakeDelegate(body))
	if err := r.Delegate(context.Background(), id); err != ErrNoDelegator {
		t.Fatalf("delegated bytes the store already holds: %v", err)
	}
	if rec, _ := store.Load(id); !store.Claimable(rec) {
		t.Fatal("the lease was kept on a job that was not delegated")
	}
}
