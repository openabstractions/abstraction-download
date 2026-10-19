//go:build darwin || linux

package nas

import (
	"testing"

	identity "github.com/openabstractions/abstraction-identity"
)

// shareSpelling marks dir as a network volume for identity.LocalPath, as an
// NFS or SMB mount on Linux or a volume without MNT_LOCAL on macOS reads.
func shareSpelling(t *testing.T, dir string) string {
	t.Helper()
	t.Cleanup(identity.TreatVolumeAsNetwork(dir))
	return dir
}
