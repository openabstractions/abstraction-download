package main

import (
	"errors"
	"testing"

	dl "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
)

type probeHolder struct{ releaseErr error }

func (h probeHolder) Hold(string, string) (job.Held, error) { return probeHeld{h.releaseErr}, nil }

type probeHeld struct{ releaseErr error }

func (h probeHeld) Done() <-chan struct{} { return nil }
func (h probeHeld) Release() error        { return h.releaseErr }

func TestInhibitorRequiresSuccessfulRelease(t *testing.T) {
	old := job.Platform
	t.Cleanup(func() { job.Platform = old })
	job.Platform = probeHolder{releaseErr: errors.New("release failed")}
	if inhibitor() {
		t.Fatal("claimed inhibitor after release failure")
	}
	job.Platform = probeHolder{}
	if !inhibitor() {
		t.Fatal("did not claim released inhibitor")
	}
}

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
