package download

import (
	"net"
	"os"
	"path/filepath"
	"time"

	job "github.com/ReinisLusis/abstraction/job/go"
)

// Telling the supervisor to look now, instead of waiting for it to look anyway.
//
// # What was wrong with polling alone
//
// The store is the right place for the truth: work has to outlive the process
// that started it, the machine going to sleep, and a crash, and a socket cannot
// hold state. BITS keeps an on-disk queue for the same reason.
//
// But the store was also the only way anything learned that something had
// changed, and that part does not hold up. A job submitted a moment after a
// sweep waits a whole interval before anybody looks at it — thirty seconds by
// default, for work that could have started immediately. Over SMB the reads can
// be stale on top of that. "It is just files" is a fair description of where the
// truth lives and a poor description of how news travels.
//
// # The shape that does hold up
//
// Durable state in files, and notification as an ACCELERANT that is never the
// source of truth. A nudge is a hint, not a message: it carries no job id, no
// payload and no meaning beyond "there is something to look at". If it is lost,
// refused, or nobody is listening, the sweep finds the work anyway and the only
// cost is latency.
//
// That property is what makes it safe to add. A notification channel that
// carried state would be a second source of truth, and reconciling two of those
// is the failure this whole project exists to avoid.
const nudgeName = "supervisor.sock"

// nudgePath is "" when the store's binding is not a local filesystem. A nudge is
// a hint with no payload, so a binding that has its own channel will deliver it
// its own way, and one that has neither still loses nothing: the sweep comes.
func nudgePath(store job.Store) string {
	root := localRoot(store)
	if root == "" {
		return ""
	}
	return filepath.Join(root, nudgeName)
}

// Nudge asks the supervisor watching this store to sweep now.
//
// Best effort by construction. Every failure — no supervisor, a stale socket, a
// busy one, a platform without unix sockets — is silently fine, because the
// sweep is still coming.
func Nudge(store job.Store) {
	c, err := net.DialTimeout("unix", nudgePath(store), 250*time.Millisecond)
	if err != nil {
		return
	}
	c.SetDeadline(time.Now().Add(250 * time.Millisecond))
	c.Write([]byte("look\n"))
	c.Close()
}

// Nudges listens for them. The supervisor owns one.
type Nudges struct {
	ln net.Listener
	ch chan struct{}
}

// Listen opens the nudge socket beside the store.
//
// A stale socket file from a killed supervisor refuses bind, so it is removed
// first — refusing to start because of the corpse of a previous run is not
// useful behaviour, and the heartbeat is what actually establishes whether a
// supervisor is alive.
func ListenForNudges(store job.Store) (*Nudges, error) {
	path := nudgePath(store)
	os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	os.Chmod(path, 0o777)

	n := &Nudges{ln: ln, ch: make(chan struct{}, 1)}
	go n.accept()
	return n, nil
}

func (n *Nudges) accept() {
	for {
		c, err := n.ln.Accept()
		if err != nil {
			return
		}
		c.Close()
		// Coalesce. Ten submissions in a second are one reason to sweep, not
		// ten sweeps, and a supervisor that sweeps per message is a supervisor
		// anyone can wedge by connecting in a loop.
		select {
		case n.ch <- struct{}{}:
		default:
		}
	}
}

// C fires when somebody has asked for a sweep. It is deliberately not "a job
// arrived": the supervisor's answer to a nudge is the same sweep it was going to
// do anyway, just sooner.
func (n *Nudges) C() <-chan struct{} { return n.ch }

func (n *Nudges) Close() error {
	err := n.ln.Close()
	os.Remove(n.ln.Addr().String())
	return err
}
