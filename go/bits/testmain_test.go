package bits

import (
	"fmt"
	"os"
	"testing"

	"github.com/openabstractions/abstraction-download/go/internal/personfolders"
)

// TestMain runs this package's tests with the person's per-user folders
// inside a temporary folder (personfolders.Main). When OA_BITS_TEST_STDERR
// is set, the test binary stands in for PowerShell instead
// (TestRunRedactsShellStderr): it writes that text to stderr and exits 1.
func TestMain(m *testing.M) {
	if text, ok := os.LookupEnv("OA_BITS_TEST_STDERR"); ok {
		fmt.Fprint(os.Stderr, text)
		os.Exit(1)
	}
	os.Exit(personfolders.Main(m))
}
