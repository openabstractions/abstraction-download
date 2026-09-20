package serve

import (
	"context"
	"sync"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	"github.com/openabstractions/abstraction-download/go/netcost"
)

// networkRetry is the first delay before a service asks again for a cost
// source that could not be opened; each failure doubles it up to
// networkRetryCeiling. Tests shorten it.
var (
	networkRetry        = time.Second
	networkRetryCeiling = 30 * time.Second
)

// platformRetryAfter bounds how often the shared platform source is opened
// again after a failure, so admission checks do not dial the platform on every
// submission while no source answers.
const platformRetryAfter = 2 * time.Second

var platformNetwork struct {
	sync.Mutex
	source netcost.Source
	err    error
	failed time.Time
}

// sharedPlatformNetworkCost opens this platform's cost source once per process
// and keeps it. A failure is remembered for platformRetryAfter and then tried
// again: a Linux service can start before NetworkManager answers [DL-N8].
func sharedPlatformNetworkCost() (netcost.Source, error) {
	platformNetwork.Lock()
	defer platformNetwork.Unlock()
	if platformNetwork.source != nil {
		return platformNetwork.source, nil
	}
	if platformNetwork.err != nil && time.Since(platformNetwork.failed) < platformRetryAfter {
		return nil, platformNetwork.err
	}
	source, err := netcost.Open()
	if err != nil {
		platformNetwork.err, platformNetwork.failed = err, time.Now()
		return nil, err
	}
	platformNetwork.source, platformNetwork.err = source, nil
	return source, nil
}

// lateNetwork stands for a cost source that could not be opened when the
// service started. It reports download.CostUnavailable until open succeeds,
// then the opened source's cost, and tells its watchers when either changes,
// so work waiting with network:unavailable moves on that notice [DL-N8]. It
// never closes the source it opened, which the opener owns.
type lateNetwork struct {
	hub    *netcost.Fake
	mu     sync.Mutex
	opened netcost.Source
	cancel context.CancelFunc
	done   chan struct{}
}

// servedNetwork returns the cost source a serving execution uses and a stop
// function. An opener that answers at once is used directly; one that fails
// is asked again with backoff until it answers or ctx ends.
func servedNetwork(ctx context.Context, open func() (netcost.Source, error)) (netcost.Source, func()) {
	if source, err := open(); err == nil && source != nil {
		return source, func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	late := &lateNetwork{hub: netcost.NewFake(download.CostUnavailable), cancel: cancel, done: make(chan struct{})}
	go late.run(ctx, open)
	return late, func() { late.Close() }
}

func (l *lateNetwork) run(ctx context.Context, open func() (netcost.Source, error)) {
	defer close(l.done)
	delay := networkRetry
	var source netcost.Source
	for source == nil {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if opened, err := open(); err == nil && opened != nil {
			source = opened
		}
		delay = min(2*delay, networkRetryCeiling)
	}
	watcher := source.Watch()
	defer watcher.Close()
	l.mu.Lock()
	l.opened = source
	l.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { watcher.Close() })
	defer stop()
	for {
		cost, err := watcher.Next(ctx)
		if err != nil {
			return
		}
		l.hub.Set(cost)
	}
}

// Cost is the opened source's present cost, or CostUnavailable before it opened.
func (l *lateNetwork) Cost() netcost.Cost {
	l.mu.Lock()
	opened := l.opened
	l.mu.Unlock()
	if opened != nil {
		return opened.Cost()
	}
	return download.CostUnavailable
}

func (l *lateNetwork) Watch() *netcost.Watcher { return l.hub.Watch() }

func (l *lateNetwork) Close() error {
	l.cancel()
	<-l.done
	return l.hub.Close()
}
