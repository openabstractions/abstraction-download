package serve

import (
	"os"
	"testing"

	"github.com/openabstractions/abstraction-download/go/internal/personfolders"
)

// TestMain runs this package's tests with the person's per-user folders
// inside a temporary folder (personfolders.Main), with loopback fixtures
// reachable.
func TestMain(m *testing.M) {
	// Fixtures serve on loopback; egress tests turn this off [DL-G4].
	AllowLoopbackForTesting()
	os.Exit(personfolders.Main(m))
}
