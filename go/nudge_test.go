package download

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
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

// nudgeAndWait is Nudge plus the acknowledgement Nudge deliberately does not wait
// for.
//
// The listener closes the connection and only then offers the wakeup on, so a
// read to EOF is proof that this nudge reached the accept loop. Nudge itself
// must never wait — it is best effort, and the supervisor it is talking to may
// not exist — which is exactly why a test that needs to know a burst landed has
// to ask for the acknowledgement here rather than change what Nudge does.
//
// The deadlines below are there so a broken listener fails the test instead of
// hanging it. Nothing is timed against them.
func nudgeAndWait(t *testing.T, store job.Store) {
	t.Helper()
	c, err := net.DialTimeout("unix", nudgePath(store), 5*time.Second)
	if err != nil {
		t.Fatalf("dialling the nudge socket: %v", err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	// The listener accepts and closes without reading, so the acknowledgement
	// arrives in whichever way this platform reports a peer that has gone: EOF
	// on the read, ECONNRESET on Windows, EPIPE on the write on Linux — where
	// this failed, because only the read was allowed to see it. All three say
	// the connection was accepted and finished with, and the accept loop offers
	// the wakeup after that close, so all three are proof the nudge landed.
	// Never hearing back at all is the only failure.
	_, err = c.Write([]byte("look\n"))
	if err == nil {
		_, err = io.ReadAll(c)
	}
	var ne net.Error
	if err != nil && errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("the listener never acknowledged the nudge: %v", err)
	}
}

// Ten submissions in a second are one reason to sweep, not ten sweeps.
// Otherwise anybody can wedge the supervisor by connecting in a loop.
//
// What coalescing promises — and the only thing it CAN promise — is that at
// most one wakeup is ever PENDING. It is not a bound on wakeups over time: a
// reader that empties the channel between two nudges is entitled to be woken
// twice, and a reader fast enough to empty it after every single one will be
// woken for every single one. That is not a supervisor anybody can wedge,
// because a sweep is what happens between two reads and the queue behind it
// never grows.
//
// This test used to send twenty nudges and count how many wakeups came out,
// which measures the scheduler rather than the code: whether the reader outran
// the accept loop is not something either side controls, and the totals
// observed here ranged from 1 to all 20. It failed about one run in five. In a
// project whose evidence is byte-identical agreement, a test that fails at
// random teaches everyone to run it again, which is how a real intermittent bug
// gets shipped.
//
// So the burst is delivered first and read afterwards. The accept loop handles
// one connection at a time, so by the time the twentieth acknowledgement comes
// back the first nineteen wakeups have already been offered to the channel —
// and if they were accumulating rather than coalescing, nineteen of them would
// be sitting there.
func TestNudgesCoalesce(t *testing.T) {
	_, store, _ := newRunner(t)
	n, err := ListenForNudges(store)
	if err != nil {
		t.Skipf("unix sockets unavailable here: %v", err)
	}
	defer n.Close()

	for i := 0; i < 20; i++ {
		nudgeAndWait(t, store)
	}
	select {
	case <-n.C():
	default:
		t.Fatal("no wakeup at all, after twenty acknowledged nudges")
	}
	// Nineteen sends have completed and one wakeup has just been taken, so
	// anything still waiting is a wakeup that was queued rather than collapsed.
	// The twentieth send may not have run yet, so one more is allowed and no
	// more.
	extra := 0
	for {
		select {
		case <-n.C():
			extra++
			if extra > 1 {
				t.Fatal("nudges are not coalescing; a caller can force a sweep per message")
			}
		default:
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
