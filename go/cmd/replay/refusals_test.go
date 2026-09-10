package main

import (
	"strings"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
	wire "github.com/openabstractions/abstraction-job/go/rec"
)

// A member without a transcript annotation makes outcome print the empty string
// for an error a caller has to branch on, and every transcript in docs/results/
// would change without a word of it appearing in a diff of this file.
func TestEveryVerdictHasATranscriptToken(t *testing.T) {
	if len(wire.VerdictNames) == 0 {
		t.Fatal("the generated vocabulary is empty")
	}
	for _, name := range wire.VerdictNames {
		if wire.VerdictTranscript[name] == "" {
			t.Errorf("Verdict member %q carries no transcript annotation in job.thrift", name)
		}
	}
}

func TestOutcomeSpellsEachRefusalTheWayTheDefinitionDoes(t *testing.T) {
	for _, c := range []struct {
		err    error
		member string
	}{
		{job.ErrNotFound, wire.VerdictNotFound},
		{job.ErrLeaseHeld, wire.VerdictLeaseHeld},
		{job.ErrStaleEpoch, wire.VerdictStaleEpoch},
		{job.ErrConflict, wire.VerdictConflict},
		{job.ErrLeaseExpiry, wire.VerdictLeaseExpired},
		{job.ErrTerminal, wire.VerdictTerminal},
		{job.ErrInvalid, wire.VerdictInvalid},
		{job.ErrUnknownSchema, wire.VerdictUnknownSchema},
		{job.ErrNotSupported, wire.VerdictNotSupported},
	} {
		if got, want := outcome(c.err), wire.VerdictTranscript[c.member]; got != want {
			t.Errorf("%v printed %q, and job.thrift spells %s %q", c.err, got, c.member, want)
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
	lines := strings.Split(refusals(), "\n")
	roster := map[string]bool{}
	for _, line := range lines {
		roster[line] = true
	}
	for _, name := range wire.VerdictNames {
		if !roster[wire.VerdictTranscript[name]] {
			t.Errorf("--refusals omits %q, which Verdict member %q spells",
				wire.VerdictTranscript[name], name)
		}
	}
	if len(roster) != len(lines) {
		t.Errorf("--refusals printed %d lines for %d tokens", len(lines), len(roster))
	}
}
