package nas

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestLiveFind asks the real network. Skipped unless ABSTRACTION_LIVE_NAS is set,
// because a gate that depends on a house being switched on is not a gate.
func TestLiveFind(t *testing.T) {
	if os.Getenv("ABSTRACTION_LIVE_NAS") == "" {
		t.Skip("set ABSTRACTION_LIVE_NAS to ask this network")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	found := Find(ctx)
	if len(found) == 0 {
		t.Fatal("nothing answered")
	}
	for _, f := range found {
		t.Logf("%-16s %-28q says=%q how=%v shares=%v", f.Address, f.Name, f.Says, f.How, f.Shares)
	}
}
