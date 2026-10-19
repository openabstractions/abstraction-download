package archive

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/fileid"
)

func TestDownloadMemberUsesMountedArchive(t *testing.T) {
	base := os.Getenv("OA_TEST_MOUNTED_ARCHIVE")
	if base == "" {
		t.Skip("mounted archive fixture directory unavailable")
	}
	folder, err := os.MkdirTemp(base, "archive-mount-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(folder) })
	if !errors.Is(identity.LocalPath(folder), identity.ErrNotLocalPath) {
		t.Fatal("fixture must be a mount the local Identity opener refuses")
	}
	body := []byte("ordinary mounted archive read")
	if err := os.WriteFile(filepath.Join(folder, "entry"), body, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := fileid.Open(t.Context(), fileid.Self, folder, fileid.Options{}); !errors.Is(err, identity.ErrNotLocalPath) {
		t.Fatalf("restore-local owner admitted the mounted fixture: %v", err)
	}
	member, info, _, err := openArchiveMember(t.Context(), folder, "entry", true, probeTimeout)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(member.File)
	if err := errors.Join(err, member.Close()); err != nil || info.Size() != int64(len(body)) || string(got) != string(body) {
		t.Fatalf("ordinary mounted read was not observed: %q %v", got, err)
	}
	w := wantOf(body)
	path := putEntry(t, folder, w.SHA256, []byte("corrupt mounted archive member"))
	_, _, key, err := openArchiveMember(t.Context(), folder, "sha256/"+w.SHA256, false, probeTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if err := retireArchiveMember(t.Context(), folder, w.SHA256, key); err == nil {
		t.Fatal("mounted archive mutation lacked a local ownership proof")
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "corrupt mounted archive member" {
		t.Fatalf("unproven mounted archive member changed: %q %v", got, err)
	}
}
