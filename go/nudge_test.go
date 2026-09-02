package download

import (
	"testing"
	"time"
)

// A nudge collapses the wait between submitting and being noticed.
func TestNudgeWakesTheSupervisor(t *testing.T) {
	_, store, _ := newRunner(t)

	n, err := ListenForNudges(store)
	if err != nil {
		t.Skipf("unix sockets unavailable here: %v", err)
	}
	defer n.Close()

	Nudge(store)
	select {
	case <-n.C():
	case <-time.After(3 * time.Second):
		t.Fatal("a nudge did not wake the listener")
	}
}

// Ten submissions in a second are one reason to sweep, not ten sweeps.
// Otherwise anybody can wedge the supervisor by connecting in a loop.
func TestNudgesCoalesce(t *testing.T) {
	_, store, _ := newRunner(t)
	n, err := ListenForNudges(store)
	if err != nil {
		t.Skipf("unix sockets unavailable here: %v", err)
	}
	defer n.Close()

	for i := 0; i < 20; i++ {
		Nudge(store)
	}
	select {
	case <-n.C():
	case <-time.After(3 * time.Second):
		t.Fatal("no wakeup at all")
	}
	// Drain whatever the burst produced, then require that it was bounded:
	// a coalescing channel holds at most one pending wakeup.
	extra := 0
	for {
		select {
		case <-n.C():
			extra++
			if extra > 1 {
				t.Fatal("nudges are not coalescing; a caller can force a sweep per message")
			}
		case <-time.After(200 * time.Millisecond):
			return
		}
	}
}

// The whole point: losing the nudge must cost latency and nothing else.
func TestNudgeWithNobodyListeningIsHarmless(t *testing.T) {
	_, store, _ := newRunner(t)
	done := make(chan struct{})
	go func() { Nudge(store); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Nudge blocked when no supervisor was listening; it must be best effort")
	}
}
