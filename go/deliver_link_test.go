package download

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// When the rename across volumes is unavailable, delivery copies. The copy
// replaces whatever link sits at the destination and never writes through it
// into the file the link names.
func TestDeliveryAcrossVolumesDoesNotWriteThroughALink(t *testing.T) {
	saved := renameFile
	t.Cleanup(func() { renameFile = saved })
	renameFile = func(string, string) error { return errors.New("invalid cross-device link") }

	for _, tc := range []struct {
		name string
		link func(target, link string) error
	}{
		{name: "hard link", link: os.Link},
		{name: "symbolic link", link: os.Symlink},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			outside := filepath.Join(t.TempDir(), "someone-elses.bin")
			if err := os.WriteFile(outside, []byte("not yours"), 0o644); err != nil {
				t.Fatal(err)
			}
			final := filepath.Join(dir, "out", "model.bin")
			if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := tc.link(outside, final); err != nil {
				t.Skipf("this volume will not hold a %s: %v", tc.name, err)
			}
			partial := filepath.Join(dir, "model.bin.part")
			if err := os.WriteFile(partial, []byte("the artifact"), 0o644); err != nil {
				t.Fatal(err)
			}

			if err := deliver(partial, final); err != nil {
				t.Fatalf("deliver: %v", err)
			}
			if b, err := os.ReadFile(outside); err != nil || string(b) != "not yours" {
				t.Fatalf("delivery wrote through the link into %s: %q, %v", outside, b, err)
			}
			st, err := os.Lstat(final)
			if err != nil {
				t.Fatal(err)
			}
			if !st.Mode().IsRegular() {
				t.Fatalf("the destination is still a link: %v", st.Mode())
			}
			if b, err := os.ReadFile(final); err != nil || string(b) != "the artifact" {
				t.Fatalf("destination holds %q, %v", b, err)
			}
			if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the partial is still there: %v", err)
			}
			left, err := os.ReadDir(filepath.Dir(final))
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 1 {
				t.Fatalf("the destination folder holds %d entries, want only the delivered file", len(left))
			}
		})
	}
}
