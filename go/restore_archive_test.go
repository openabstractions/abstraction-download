package download_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	archive "github.com/openabstractions/abstraction-download/go/archive"
	job "github.com/openabstractions/abstraction-job/go"
)

func TestPrivateArchiveRestoreHasNoNetworkFallback(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	folder := filepath.Join(root, "archive")
	archive.UseState(filepath.Join(root, "state"))
	t.Cleanup(func() { archive.UseState("") })
	if err := os.MkdirAll(filepath.Join(folder, "sha256"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := archive.SetFolder(ctx, folder); err != nil {
		t.Fatal(err)
	}
	body := []byte("bytes from the archive")
	hash := fmt.Sprintf("%x", sha256.Sum256(body))
	artifact := download.Artifact{Digest: "sha256:" + hash, Size: int64(len(body))}
	entry := filepath.Join(folder, "sha256", hash)
	if err := os.WriteFile(entry, body, 0600); err != nil {
		t.Fatal(err)
	}
	store, err := job.NewFileStore(filepath.Join(root, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	owner := archive.Service{}
	selected, ok := owner.SelectRestore(ctx, artifact.Digest, artifact.Size)
	if !ok {
		t.Fatal("matching managed entry unavailable")
	}
	id, err := download.SubmitArchiveRestore(store, job.NewID(), artifact, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := download.RunArchiveRestore(ctx, store, id, selected, func(string, int64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(got)
	if err != nil || string(raw) != string(body) {
		t.Fatalf("wrong delivered bytes: %q %v", raw, err)
	}
	records, err := store.List()
	if err != nil || len(records) != 1 || records[0].State != job.StateTransferred {
		t.Fatalf("managed delivery was not recorded: %v %v", records, err)
	}
	spec, err := download.SpecOf(records[0])
	if err != nil || !spec.ArchiveOnly || len(spec.Sources) != 0 || spec.Validate() == nil {
		t.Fatalf("private admission leaked into the ordinary profile: %+v %v", spec, err)
	}
	if err := os.WriteFile(entry, make([]byte, len(body)), 0600); err != nil {
		t.Fatal(err)
	}
	selection, ok := owner.SelectRestore(ctx, artifact.Digest, artifact.Size)
	if !ok {
		t.Fatal("the managed name should be observable without hashing its bytes")
	}
	id, err = download.SubmitArchiveRestore(store, job.NewID(), artifact, nil)
	if err != nil {
		t.Fatal(err)
	}
	if path, err := download.RunArchiveRestore(ctx, store, id, selection, nil); err == nil || path != "" {
		t.Fatalf("tampered archive delivered %q: %v", path, err)
	}
}
