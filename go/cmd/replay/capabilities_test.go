package main

import (
	"testing"

	dl "github.com/openabstractions/abstraction-download/go"
)

// The capability line follows the registered fetchers, never a declaration: a
// driver whose runner cannot fetch http must not claim the scenarios that do.
func TestCapabilitiesFollowTheRegisteredFetchers(t *testing.T) {
	const fixture = "http://127.0.0.1:1"
	for _, c := range []struct {
		name     string
		fixture  string
		fetchers *dl.Fetchers
		want     string
	}{
		{"default runner with a fixture", fixture, dl.NewRunner(nil, "").Fetchers, "store transfer wire wanted"},
		{"no fixture listening", "", dl.DefaultFetchers(), "store transfer"},
		{"only a file fetcher", fixture, dl.NewFetchers(dl.File{}), "store transfer"},
		{"no fetchers at all", fixture, dl.NewFetchers(), "store transfer"},
		{"http alone", fixture, dl.NewFetchers(dl.HTTP{}), "store transfer wire wanted"},
	} {
		if got := capabilitiesWith(c.fixture, c.fetchers); got != c.want {
			t.Errorf("%s: capabilities %q, want %q", c.name, got, c.want)
		}
	}
}
