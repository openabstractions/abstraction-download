package bits

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/openabstractions/abstraction-facade/go-core/redact/redacttest"
)

// PowerShell's stderr, which can hold a signed URL, reaches a download error
// only through redact.Line.
func TestRunRedactsShellStderr(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	d := &Delegator{PowerShell: self}
	for _, c := range redacttest.Cases {
		t.Setenv("OA_BITS_TEST_STDERR", c.In)
		_, err := d.run(context.Background(), "", nil)
		if err == nil {
			t.Fatalf("stderr %q: no error", c.In)
		}
		for _, secret := range redacttest.Secrets {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("stderr %q: %q keeps %q", c.In, err, secret)
			}
		}
	}
}
