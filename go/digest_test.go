package download

import "testing"

// A 1.5 GB download of correct bytes was rejected because one implementation
// wrote the digest bare and the other built "sha256:<hex>". The error said
// "got sha256:1fc70f… want 1fc70f…" — the same digest, twice.
func TestDigestsMatchAcrossImplementationsThatLabelThemDifferently(t *testing.T) {
	const hex = "1fc70f774d38eb169993ac391eea357ef47c88757ef72ee5943879b7e8e2bc69"
	same := []struct{ a, b string }{
		{"sha256:" + hex, hex},             // what Lemonade wrote
		{hex, "sha256:" + hex},             // and the other way round
		{"sha256:" + hex, "SHA256:" + hex}, // case in the label
		{"sha256:" + hex, "sha256-" + hex}, // how Ollama names its blobs
		{"sha256:" + hex, "sha256:" + hex}, // the ordinary case
	}
	for _, c := range same {
		if !sameDigest(c.a, c.b) {
			t.Errorf("rejected identical bytes: %q vs %q", c.a, c.b)
		}
	}

	other := "0000000000000000000000000000000000000000000000000000000000000000"
	notSame := []struct{ a, b string }{
		{"sha256:" + hex, "sha256:" + other}, // genuinely different
		{"sha256:" + hex, ""},                // nothing to compare against
		{"", ""},                             // must not say two blanks match
		{"sha256:" + hex, "sha256:abc"},      // truncated
	}
	for _, c := range notSame {
		if sameDigest(c.a, c.b) {
			t.Errorf("accepted bytes it should have refused: %q vs %q", c.a, c.b)
		}
	}
}
