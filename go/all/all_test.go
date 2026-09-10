package all

import (
	"testing"

	config "github.com/openabstractions/abstraction-config/go"
	download "github.com/openabstractions/abstraction-download/go"
)

// Every tier this import links declares what it is a provider over. The whole
// value of the field is that a registration nobody filled in is visible here
// rather than counted as ours.
func TestEveryLinkedTierSaysWhatItIsAProviderOver(t *testing.T) {
	offers := download.Offers(config.Config{})
	if len(offers) == 0 {
		t.Fatal("this program links no tier, so this proves nothing")
	}
	for _, o := range offers {
		if o.Over == download.OverUndeclared {
			t.Errorf("%s does not say what it is a provider over", o.System)
		}
	}
}

// And the two that ship say different things, which is the distinction the
// adoption levels turn on: BITS is Windows' own transfer service, a NAS is us
// at the far end of a share.
func TestTheShippedTiersSayWhichKindTheyAre(t *testing.T) {
	want := map[string]download.Over{"bits": download.OverPlatform, "nas": download.OverOurs}
	for _, o := range download.Offers(config.Config{}) {
		if w, ok := want[o.System]; ok {
			if o.Over != w {
				t.Errorf("%s is a provider over %s, want %s", o.System, o.Over, w)
			}
			delete(want, o.System)
		}
	}
	for name := range want {
		t.Errorf("%s registered no tier at all", name)
	}
}
