package download

import (
	"os"
	"testing"
)

// A refusal list with no location of this account refuses every host and
// takes no edit: it never lives in the temporary directory, where on Linux
// another account can write first, and an empty path never reads as an empty
// list (storage STO-L3).
func TestARefusalListWithNoLocationRefusesInsteadOfAllowing(t *testing.T) {
	t.Setenv("APPDATA", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", "")
	if _, err := os.UserConfigDir(); err == nil {
		t.Skip("this platform answers a configuration folder with none of its variables set")
	}
	f := DefaultRefusals()
	if f.Path != "" {
		t.Fatalf("DefaultRefusals().Path = %q with no configuration folder, want none", f.Path)
	}
	if err := f.Check("example.com"); err == nil {
		t.Fatal("a list with no location allowed a host")
	}
	if err := f.Refuse("example.com", "blocked"); err == nil {
		t.Fatal("a list with no location took an edit")
	}
}
