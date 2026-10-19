package main

import (
	"errors"
	"testing"

	dl "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
)

type probeHolder struct {
	holdErr error
	held    *probeHeld
	called  int
}

func (h *probeHolder) Hold(string, string) (job.Held, error) {
	h.called++
	return h.held, h.holdErr
}

type probeHeld struct {
	releaseErr error
	released   int
}

func (h *probeHeld) Done() <-chan struct{} { return nil }
func (h *probeHeld) Release() error {
	h.released++
	return h.releaseErr
}

func TestInhibitorRequiresSuccessfulRelease(t *testing.T) {
	for _, tc := range []struct {
		name                string
		holdErr, releaseErr error
		want                bool
		wantReleases        int
	}{
		{"acquisition refused", errors.New("hold refused"), nil, false, 0},
		{"release failed", nil, errors.New("release failed"), false, 1},
		{"released", nil, nil, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			held := &probeHeld{releaseErr: tc.releaseErr}
			holder := &probeHolder{holdErr: tc.holdErr, held: held}
			if got := inhibitorWith(holder); got != tc.want {
				t.Fatalf("inhibitor = %v, want %v", got, tc.want)
			}
			if holder.called != 1 || held.released != tc.wantReleases {
				t.Fatalf("hold calls = %d, releases = %d; want 1, %d", holder.called, held.released, tc.wantReleases)
			}
		})
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
