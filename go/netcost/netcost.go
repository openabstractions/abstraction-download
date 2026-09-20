// Package netcost reports what the network path this machine sends traffic on
// costs, and says when that changes.
//
// It is the cost source behind abstraction.download/network-cost@1: a download
// requesting `network: unmetered` waits while the source reports Metered and
// resumes on the notice that the cost changed. Every platform furnishes a
// change notification, so a listener waits on a notice and never on a tick:
//
//   - Windows: Network List Manager, INetworkCostManager::GetCost for the
//     machine-wide cost and INetworkCostManagerEvents::CostChanged;
//   - Linux: NetworkManager's `Metered` property on the system bus and its
//     PropertiesChanged signal;
//   - macOS: Network.framework's nw_path_monitor, `isExpensive` and
//     `isConstrained`, which needs cgo.
//
// A host without one of those (WSL, a headless Linux without NetworkManager, a
// cgo-less macOS build, any other platform) reports ErrUnavailable from Open,
// and a provider on it does not advertise the guarantee.
package netcost

import (
	"context"
	"errors"
	"strconv"
	"sync"

	watch "github.com/openabstractions/abstraction-watch/go"
)

// Cost is one word for what traffic on the current path costs.
type Cost string

const (
	// Unknown means the platform reports no cost for the current path, for
	// example while nothing is connected. It is no evidence of metering.
	Unknown Cost = "unknown"
	// Unmetered is a path the platform reports as unrestricted.
	Unmetered Cost = "unmetered"
	// Metered is a path the platform reports as costed: a metered or
	// data-limited connection, roaming, over its data limit, or constrained.
	Metered Cost = "metered"
)

// ErrUnavailable means this host has no network cost source.
var ErrUnavailable = errors.New("netcost: no network cost source on this host")

// ErrClosed is what a Watcher's Next returns once the watcher or its source
// is closed.
var ErrClosed = watch.ErrClosed

// Source is a network cost source.
type Source interface {
	// Cost is the cost as of the last notice.
	Cost() Cost
	// Watch subscribes to changes. Its first Next returns the present cost;
	// every later Next waits for a different one.
	Watch() *Watcher
	// Close stops the platform notification and ends every watcher.
	Close() error
}

// Open starts this platform's cost source, or reports ErrUnavailable.
func Open() (Source, error) { return open() }

// Watcher is one listener's subscription to a Source.
type Watcher struct {
	sub *watch.Subscription[Cost]
	hub *hub
}

// Next blocks until the cost differs from what this watcher last returned. The
// first call returns the present cost at once.
func (w *Watcher) Next(ctx context.Context) (Cost, error) {
	n, err := w.sub.Next(ctx)
	return n.Now, err
}

// Close ends this subscription. A waiting Next returns ErrClosed.
func (w *Watcher) Close() error {
	w.hub.forget(w.sub)
	return w.sub.Close()
}

// hub fans one platform notification out to every watcher.
type hub struct {
	mu     sync.Mutex
	cost   Cost
	stamp  uint64
	subs   map[*watch.Subscription[Cost]]struct{}
	closed bool
}

func newHub(initial Cost) *hub {
	return &hub{cost: initial, subs: map[*watch.Subscription[Cost]]struct{}{}}
}

func (h *hub) Cost() Cost {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cost
}

// set records a platform notice. The same cost twice wakes nobody.
func (h *hub) set(c Cost) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || c == h.cost {
		return
	}
	h.cost = c
	h.stamp++
	stamp := strconv.FormatUint(h.stamp, 10)
	for s := range h.subs {
		s.Post(c, stamp)
	}
}

func (h *hub) Watch() *Watcher {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := watch.Push(h.cost, strconv.FormatUint(h.stamp, 10), 0)
	if h.closed {
		s.Close()
	} else {
		h.subs[s] = struct{}{}
	}
	return &Watcher{sub: s, hub: h}
}

func (h *hub) forget(s *watch.Subscription[Cost]) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs, s)
}

func (h *hub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for s := range h.subs {
		s.Close()
	}
	h.subs = nil
}

// Fake is a Source a test drives. It is safe for concurrent use.
type Fake struct{ *hub }

// NewFake returns a source reporting initial until Set says otherwise.
func NewFake(initial Cost) *Fake { return &Fake{newHub(initial)} }

// Set reports a new cost, as a platform notification would.
func (f *Fake) Set(c Cost) { f.set(c) }

func (f *Fake) Close() error {
	f.close()
	return nil
}
