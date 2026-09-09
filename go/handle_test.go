package download

import (
	"path/filepath"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// Two honest callers of the facade both wrote the same anonymous struct to
// decode sink.final out of a record, because nothing else would tell them where
// their own file had gone.
func TestAHandleSaysWhereTheBytesGo(t *testing.T) {
	svc, _ := clientOn(t)

	h, err := svc.Submit(Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/x.bin"}},
		Sink:    Sink{Final: "out/x.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}

	where, err := h.Destination()
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(where) {
		t.Fatalf("%q is not something a caller can open", where)
	}
	if filepath.Base(where) != "x.bin" {
		t.Fatalf("%q is not where x.bin was asked to go", where)
	}
	if reopened := svc.Open(h.ID()); reopened.ID() != h.ID() {
		t.Fatalf("Open gave %s for %s", reopened.ID(), h.ID())
	}
}

// Wrapping a job handle to add Destination must not swallow the capabilities
// the wrapped handle advertises. Pause is discovered by type assertion, so a
// wrapper that does not declare it removes the pause button from every
// application built on this layer, silently and without a compiler error.
func TestPauseSurvivesTheWrapper(t *testing.T) {
	svc, _ := clientOn(t)

	h, err := svc.Submit(Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/y.bin"}},
		Sink:    Sink{Final: "out/y.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	p, ok := h.(job.Pausable)
	if !ok {
		t.Fatal("a download handle cannot be paused; the job handle underneath can")
	}
	if err := p.Pause(); err != nil {
		t.Fatal(err)
	}
	rec, err := h.Record()
	if err != nil {
		t.Fatal(err)
	}
	if rec.Intent == nil || rec.Intent.Want != job.WantPause {
		t.Fatalf("intent is %v after Pause", rec.Intent)
	}
}
