package main

import (
	"strings"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
	wire "github.com/openabstractions/abstraction-job/go/rec"
)

// The four grouped refusals carry explicit overrides. Every other member uses
// its own snake_case word.
func TestVerdictTranscriptOverridesOnlyGroupedRefusals(t *testing.T) {
	if len(wire.VerdictValues()) == 0 {
		t.Fatal("the generated vocabulary is empty")
	}
	for _, name := range wire.VerdictValues() {
		word := wire.VerdictTranscript[string(name)]
		if name == wire.VerdictConflict || name == wire.VerdictUnknownOp || name == wire.VerdictNotSupported || name == wire.VerdictOther {
			if word != "refused" {
				t.Errorf("Verdict member %q overrides to %q, want refused", name, word)
			}
		} else if word != "" {
			t.Errorf("Verdict member %q has obsolete transcript override %q", name, word)
		}
	}
}

func TestOutcomeSpellsEachRefusalTheWayTheDefinitionDoes(t *testing.T) {
	for _, c := range []struct {
		err    error
		member wire.Verdict
		want   string
	}{
		{job.ErrNotFound, wire.VerdictNotFound, "not_found"},
		{job.ErrLeaseHeld, wire.VerdictLeaseHeld, "lease_held"},
		{job.ErrStaleEpoch, wire.VerdictStaleEpoch, "stale_epoch"},
		{job.ErrConflict, wire.VerdictConflict, "refused"},
		{job.ErrLeaseExpiry, wire.VerdictLeaseExpired, "lease_expired"},
		{job.ErrTerminal, wire.VerdictTerminal, "terminal"},
		{job.ErrInvalid, wire.VerdictInvalid, "invalid"},
		{job.ErrUnknownSchema, wire.VerdictUnknownSchema, "unknown_schema"},
		{job.ErrNotSupported, wire.VerdictNotSupported, "refused"},
	} {
		if got := outcome(c.err); got != c.want {
			t.Errorf("%v printed %q, want %s %q", c.err, got, c.member, c.want)
		}
	}
	if got := outcome(nil); got != "ok" {
		t.Errorf("no error printed %q, want ok", got)
	}
}

// The roster the harness diffs against the Python and C++ drivers, which spell
// these by hand. It is the whole of what outcome can print, so a token missing
// here would make that diff pass while a transcript disagreed.
func TestRefusalsIsEveryTokenOutcomeCanPrint(t *testing.T) {
	const want = "invalid\nlease_expired\nlease_held\nnot_found\nrefused\nstale_epoch\nterminal\nunknown_schema"
	if got := refusals(); got != want {
		t.Errorf("--refusals printed %q, want %q", got, want)
	}
	lines := strings.Split(refusals(), "\n")
	roster := map[string]bool{}
	for _, line := range lines {
		roster[line] = true
	}
	for _, name := range wire.VerdictValues() {
		if !roster[transcriptVerdict(name)] {
			t.Errorf("--refusals omits %q, which Verdict member %q spells",
				transcriptVerdict(name), name)
		}
	}
	if len(roster) != len(lines) {
		t.Errorf("--refusals printed %d lines for %d tokens", len(lines), len(roster))
	}
}
