package serve

import (
	"net/url"
	"sync/atomic"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
)

// loopbackFixtures is set only by AllowLoopbackForTesting.
var loopbackFixtures atomic.Bool

// AllowLoopbackForTesting lets the download service in a test binary reach
// this computer's loopback addresses, and fetch http without a digest from a
// loopback host, for tests whose fixtures serve there. It returns the function
// that restores the previous setting. Outside a test binary it does nothing:
// testing.Testing is false in every program go build produces, so no
// production caller can open loopback with it. Every other egress rule still
// applies [DL-G4].
func AllowLoopbackForTesting() (restore func()) {
	if !testing.Testing() {
		return func() {}
	}
	previous := loopbackFixtures.Swap(true)
	return func() { loopbackFixtures.Store(previous) }
}

// serviceEgress is the egress policy of an application's downloads: the
// address and scheme rules, the person's refused hosts and the person's
// allowed local hosts, each list read at the moment it is asked [DL-G1 to
// DL-G8].
func serviceEgress() *download.Egress {
	return &download.Egress{
		Refused:      personReach(),
		AllowedLocal: download.DefaultLocalHosts().Allows,
		Loopback:     loopbackFixtures.Load() && testing.Testing(),
	}
}

// schemeRefusal refuses a request invalid when a source's scheme breaks the
// egress rule: https always, http only with a SHA-256 digest [DL-G1].
func schemeRefusal(kind string, raw []byte) (api.AcceptanceOutcome, string) {
	if kind != download.Kind {
		return 0, ""
	}
	input, err := request.Decode(raw)
	if err != nil {
		return 0, ""
	}
	egress := serviceEgress()
	bound := input.Artifact.Digest != ""
	for _, source := range input.Sources {
		u, err := url.Parse(source.Locator)
		if err != nil || u.Host == "" {
			continue // preparation refuses it
		}
		if err := egress.SchemeAllowed(u, bound); err != nil {
			return api.AcceptanceOutcomeInvalid, download.EgressReason(err)
		}
	}
	return 0, ""
}
