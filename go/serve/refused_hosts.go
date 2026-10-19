package serve

import (
	"fmt"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
)

// personReach is the hosts the person switched off, read from their list at
// the moment each host is asked about, so a switch takes effect on the next
// admission or connection and nothing restarts [DL-E8].
func personReach() download.Reach { return download.DefaultRefusals().Check }

// refusedHosts answers unavailable when the person's list refuses the host of
// every source a request names, with the reason the list gives, before
// anything is journaled [DL-E8]. A request with one reachable source is
// admitted, and the runner skips the refused ones. Input preparation would
// refuse is left to preparation.
func refusedHosts(kind string, raw []byte) (api.AcceptanceOutcome, string) {
	if kind != download.Kind {
		return 0, ""
	}
	input, err := request.Decode(raw)
	if err != nil || len(input.Sources) == 0 {
		return 0, ""
	}
	reach := serviceEgress().Name
	reason := ""
	for _, source := range input.Sources {
		host := download.HostOf(source.Locator)
		if host == "" {
			return 0, ""
		}
		refusal := reach(host)
		if refusal == nil {
			return 0, ""
		}
		if reason == "" {
			reason = download.EgressReason(fmt.Errorf("%w %s: %w", download.ErrUnreachable, host, refusal))
		}
	}
	return api.AcceptanceOutcomeUnavailable, reason
}
