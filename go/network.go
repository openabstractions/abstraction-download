package download

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-download/go/netcost"
	job "github.com/openabstractions/abstraction-job/go"
)

// CapNetworkCost is the guarantee a record requiring network: unmetered carries
// in requires [DL-N2]. The runner keeps it through its Network source, never a
// fetcher, so it is removed from what a fetcher is asked to promise.
var CapNetworkCost = Capability(request.NetworkCostGuarantees[0])

// WaitingExtension is the record extension key a waiting attempt carries
// [DL-N5]. Its value is a JSON string of one word.
var WaitingExtension = request.WaitingExtensions[0]

// WaitingNetworkMetered is the waiting word for a transfer held by a metered path.
const WaitingNetworkMetered = "network:metered"

// WaitingNetworkUnavailable is the waiting word for a transfer held because the
// runner's cost source has not answered yet, such as a NetworkManager that has
// not started when the service restarts [DL-N8].
const WaitingNetworkUnavailable = "network:unavailable"

// CostUnavailable is the cost a runner's source reports while the platform
// source it stands for cannot be opened yet. A job requiring an unmetered path
// waits on it as on Metered, and moves on the notice that the source answers.
const CostUnavailable netcost.Cost = "unavailable"

var (
	// ErrWaiting ends a run that stopped for a constraint rather than a
	// failure. The record says why under WaitingExtension, the lease is let go
	// and nothing counts it as an attempt that failed [DL-N4].
	ErrWaiting = errors.New("download: waiting for an unmetered network")
	// ErrNetworkUnavailable is ErrWaiting for a cost source that has not
	// answered yet; the record's word is network:unavailable [DL-N8].
	ErrNetworkUnavailable = fmt.Errorf("%w: the network cost source has not answered", ErrWaiting)
	// ErrNoNetworkCost is a record requiring network-cost@1 met by a runner
	// with no cost source. It is not now: the same record runs where one exists.
	ErrNoNetworkCost = errors.New("download: this runner has no network cost source for a network-constrained job")
)

// Waiting returns a record's waiting word, or "" when it waits for nothing.
func Waiting(r *job.Record) string {
	if r == nil {
		return ""
	}
	raw, ok := r.Extensions[WaitingExtension]
	if !ok {
		return ""
	}
	var word string
	if json.Unmarshal(raw, &word) != nil {
		return ""
	}
	return word
}

func setWaiting(rr *job.Record, word string) {
	if rr.Extensions == nil {
		rr.Extensions = map[string]json.RawMessage{}
	}
	raw, _ := json.Marshal(word)
	rr.Extensions[WaitingExtension] = raw
}

func clearWaiting(rr *job.Record) { delete(rr.Extensions, WaitingExtension) }

// fetcherRequires is what a fetcher must promise for this record. The network
// cost guarantee is the runner's own to keep.
func (r *Runner) fetcherRequires(requires []string) []string {
	if r.Network == nil || !slices.Contains(requires, string(CapNetworkCost)) {
		return requires
	}
	return slices.DeleteFunc(slices.Clone(requires), func(s string) bool { return s == string(CapNetworkCost) })
}

// networkHolds reports whether a record already says why it waits now: it asks
// for an unmetered path, and its word matches this runner's source, metered or
// not answering yet. A word the source no longer matches is run again, so the
// record either moves or says the new reason.
func (r *Runner) networkHolds(rec *job.Record) bool {
	if r.Network == nil || rec.Wants() != job.WantRun {
		return false
	}
	spec, err := SpecOf(rec)
	if err != nil || !spec.WantsUnmetered() {
		return false
	}
	switch r.Network.Cost() {
	case netcost.Metered:
		return Waiting(rec) == WaitingNetworkMetered
	case CostUnavailable:
		return Waiting(rec) == WaitingNetworkUnavailable
	}
	return false
}

// waitingWord is the word a run that ended waiting records.
func waitingWord(err error) string {
	if errors.Is(err, ErrNetworkUnavailable) {
		return WaitingNetworkUnavailable
	}
	return WaitingNetworkMetered
}

// networkGate evaluates a run's network constraint. It is carried in the run's
// context so every source open and range request asks it first [DL-N3], and
// its watcher cancels that context on the notice that the path turned metered.
type networkGate struct {
	source  netcost.Source
	watcher *netcost.Watcher
	cancel  context.CancelFunc
	tripped atomic.Bool
	done    chan struct{}
	once    sync.Once
}

type networkGateKey struct{}

// gateNetwork starts a run's constraint. It returns ErrWaiting when the path is
// metered now and ErrNoNetworkCost when the runner cannot tell. A spec with no
// constraint returns ctx unchanged and a nil gate.
func (r *Runner) gateNetwork(ctx context.Context, rec *job.Record, spec Spec) (context.Context, *networkGate, error) {
	if !spec.WantsUnmetered() && !slices.Contains(rec.Requires, string(CapNetworkCost)) {
		return ctx, nil, nil
	}
	if r.Network == nil {
		return ctx, nil, ErrNoNetworkCost
	}
	if !spec.WantsUnmetered() {
		return ctx, nil, nil
	}
	w := r.Network.Watch()
	first, err := w.Next(ctx)
	if err != nil {
		w.Close()
		if ctx.Err() != nil {
			return ctx, nil, ctx.Err()
		}
		return ctx, nil, ErrNoNetworkCost
	}
	switch first {
	case netcost.Metered:
		w.Close()
		return ctx, nil, ErrWaiting
	case CostUnavailable:
		w.Close()
		return ctx, nil, ErrNetworkUnavailable
	}
	gctx, cancel := context.WithCancel(ctx)
	g := &networkGate{source: r.Network, watcher: w, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(g.done)
		for {
			c, err := w.Next(gctx)
			if err != nil {
				return
			}
			if c == netcost.Metered {
				g.trip()
				return
			}
		}
	}()
	return context.WithValue(gctx, networkGateKey{}, g), g, nil
}

func (g *networkGate) trip() {
	g.tripped.Store(true)
	g.cancel()
}

// held reports whether the run stopped for a metered path.
func (g *networkGate) held() bool { return g != nil && g.tripped.Load() }

func (g *networkGate) stop() {
	if g == nil {
		return
	}
	g.once.Do(func() {
		g.cancel()
		g.watcher.Close()
		<-g.done
	})
}

// networkAllows is asked before every source open and range request of a run.
// It reads the source's present cost rather than waiting for the watcher to
// have seen it.
func networkAllows(ctx context.Context) error {
	g, ok := ctx.Value(networkGateKey{}).(*networkGate)
	if !ok {
		return nil
	}
	if g.tripped.Load() || g.source.Cost() == netcost.Metered {
		g.trip()
		return ErrWaiting
	}
	return nil
}
