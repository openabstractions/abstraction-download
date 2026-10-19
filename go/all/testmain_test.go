package all

import (
	"os"
	"testing"

	"github.com/openabstractions/abstraction-download/go/internal/personfolders"
)

// TestMain runs this package's tests with the person's per-user folders
// inside a temporary folder (personfolders.Main).
func TestMain(m *testing.M) { os.Exit(personfolders.Main(m)) }
