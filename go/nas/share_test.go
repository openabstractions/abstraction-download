//go:build windows || darwin || linux

package nas

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	identity "github.com/openabstractions/abstraction-identity"
)

// A store on a network share is refused where it is configured. Both hosts
// write the job record and the cas lock holds within one host (cas CAS-L2),
// so New and Check answer identity.ErrNotLocalPath and create nothing on the
// share; a runtime configured with such a store fails at start instead of at
// its first delegated download.
func TestAStoreOnANetworkShareIsRefusedWhereItIsConfigured(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(shareSpelling(t, dir), "store")
	if d, err := New(root); !errors.Is(err, identity.ErrNotLocalPath) {
		t.Fatalf("New(%s) = %v, %v; want identity.ErrNotLocalPath", root, d, err)
	}
	if err := Check(context.Background(), root); !errors.Is(err, identity.ErrNotLocalPath) {
		t.Fatalf("Check(%s) = %v; want identity.ErrNotLocalPath", root, err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatalf("the refused store left %v in %s (%v)", entries, dir, err)
	}
}
