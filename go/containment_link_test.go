package download

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	identity "github.com/openabstractions/abstraction-identity"
)

// directoryLink makes link point at the folder target: a junction on
// Windows, a symbolic link elsewhere. It skips when the volume holds neither.
func directoryLink(t *testing.T, target, link string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		if out, err := exec.Command("cmd", "/c", "mklink", "/J", link, target).CombinedOutput(); err != nil {
			t.Skipf("this volume will not hold a junction: %v %s", err, out)
		}
		return
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("this volume will not hold a symbolic link: %v", err)
	}
}

// DL-S8: a folder inside an existing store root that is a junction or link
// to a folder outside it carries a relative destination out of the root,
// though the joined path is contained by every lexical measure: the
// filesystem decides (identity.Within). A real folder inside the root, and a
// root that does not exist, keep the lexical answer.
func TestSinkMayNotLeaveTheRootThroughALink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	directoryLink(t, outside, filepath.Join(root, "models"))
	if _, err := resolveUnder(root, "models/x.gguf"); !errors.Is(err, ErrEscapesRoot) {
		t.Fatalf("a destination through a link out of the root: %v", err)
	}
	if _, err := resolveUnder(root, "models/deeper/x.gguf"); !errors.Is(err, ErrEscapesRoot) {
		t.Fatalf("a destination below a link out of the root: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "real"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"real/x.gguf", "fresh/folder/x.gguf", "x.gguf", "."} {
		if _, err := resolveUnder(root, p); err != nil {
			t.Errorf("resolveUnder(%q) refused a contained path: %v", p, err)
		}
	}
	// A link inside the root to another folder inside it stays contained.
	directoryLink(t, filepath.Join(root, "real"), filepath.Join(root, "alias"))
	if _, err := resolveUnder(root, "alias/x.gguf"); err != nil {
		t.Errorf("a link to a folder inside the root: %v", err)
	}
	missing := filepath.Join(t.TempDir(), "not-yet")
	if _, err := resolveUnder(missing, "models/x.gguf"); err != nil {
		t.Errorf("a root that does not exist yet: %v", err)
	}
}

// DL-S8: only an absent root and a root identity refuses to open leave the
// lexical answer standing; any other error from the filesystem question
// refuses the destination.
func TestSinkIsRefusedWhenTheFilesystemCannotAnswer(t *testing.T) {
	saved := withinRoot
	t.Cleanup(func() { withinRoot = saved })
	root := t.TempDir()
	for _, tc := range []struct {
		err    error
		refuse bool
	}{
		{&os.PathError{Op: "open", Path: root, Err: os.ErrNotExist}, false},
		{&identity.NotLocalPathError{Path: root, Reason: "a network drive"}, false},
		{&os.PathError{Op: "open", Path: root, Err: os.ErrPermission}, true},
		{errors.New("identity: the final path could not be read"), true},
	} {
		withinRoot = func(string, string) (bool, error) { return false, tc.err }
		_, err := resolveUnder(root, "models/x.gguf")
		if refused := errors.Is(err, ErrEscapesRoot); refused != tc.refuse {
			t.Errorf("Within answering %v: refused %v, want %v", tc.err, refused, tc.refuse)
		}
	}
}
