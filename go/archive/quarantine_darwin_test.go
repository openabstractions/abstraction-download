//go:build cgo

package archive

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestQuarantinePreservesArchiveParentWithWritableExtendedACL(t *testing.T) {
	folder := fixture(t)
	body := []byte("expected archive member")
	wanted := wantOf(body)
	wrong := sameSizeWrong(body, 1)
	entry := putEntry(t, folder, wanted.SHA256, wrong)
	parent := filepath.Dir(entry)
	if out, err := exec.Command("/bin/chmod", "+a", "everyone allow add_file,delete_child", parent).CombinedOutput(); err != nil {
		t.Fatalf("grant fixture-only extended ACL: %v: %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := exec.Command("/bin/chmod", "-N", parent).CombinedOutput(); err != nil {
			t.Errorf("remove fixture-only extended ACL: %v: %s", err, out)
		}
	})
	if stat, err := os.Stat(parent); err != nil || stat.Mode().Perm()&0022 != 0 {
		t.Fatalf("extended ACL fixture must retain its non-writable BSD mode: %v %v", stat, err)
	}
	if _, ok := fill(t, wanted); ok {
		t.Fatal("archive supplied the corrupt entry")
	}
	if got, err := os.ReadFile(entry); err != nil || !bytes.Equal(got, wrong) {
		t.Fatalf("the ambiguous entry was not preserved: %q %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(parent, ".bad")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retirement touched the namespace before ACL admission: %v", err)
	}
}
