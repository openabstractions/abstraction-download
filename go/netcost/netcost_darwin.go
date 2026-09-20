//go:build darwin && cgo

package netcost

/*
#cgo CFLAGS: -fblocks
#cgo LDFLAGS: -framework Network
#include <stdint.h>
#include <Network/Network.h>
#include <dispatch/dispatch.h>

extern void netcostPathUpdate(uintptr_t handle, int satisfied, int expensive, int constrained);

// netcost_monitor starts an nw_path_monitor on its own serial queue. The
// update handler runs once with the current path and again on every change.
static nw_path_monitor_t netcost_monitor(uintptr_t handle) {
	nw_path_monitor_t monitor = nw_path_monitor_create();
	dispatch_queue_t queue = dispatch_queue_create("org.openabstractions.netcost", DISPATCH_QUEUE_SERIAL);
	nw_path_monitor_set_queue(monitor, queue);
	dispatch_release(queue);
	nw_path_monitor_set_update_handler(monitor, ^(nw_path_t path) {
		netcostPathUpdate(handle, nw_path_get_status(path) == nw_path_status_satisfied,
			nw_path_is_expensive(path), nw_path_is_constrained(path));
	});
	nw_path_monitor_start(monitor);
	return monitor;
}

static void netcost_cancel(nw_path_monitor_t monitor) {
	nw_path_monitor_cancel(monitor);
	nw_release(monitor);
}
*/
import "C"

import (
	"runtime/cgo"
	"sync"
	"time"
)

// pathMonitor is the macOS source: a satisfied path that is expensive or
// constrained is metered, a satisfied one that is neither is unmetered, and
// an unsatisfied path is unknown.
type pathMonitor struct {
	*hub
	monitor C.nw_path_monitor_t
	handle  cgo.Handle
	first   chan struct{}
	once    sync.Once
	closed  sync.Once
}

func classifyPath(satisfied, expensive, constrained bool) Cost {
	switch {
	case !satisfied:
		return Unknown
	case expensive || constrained:
		return Metered
	}
	return Unmetered
}

func (p *pathMonitor) update(c Cost) {
	p.set(c)
	p.once.Do(func() { close(p.first) })
}

func open() (Source, error) {
	p := &pathMonitor{hub: newHub(Unknown), first: make(chan struct{})}
	p.handle = cgo.NewHandle(p)
	p.monitor = C.netcost_monitor(C.uintptr_t(p.handle))
	select {
	case <-p.first:
	case <-time.After(nmOpenLimit * time.Second):
		p.Close()
		return nil, ErrUnavailable
	}
	return p, nil
}

func (p *pathMonitor) Close() error {
	p.closed.Do(func() {
		C.netcost_cancel(p.monitor)
		p.hub.close()
		// The handler may still be queued; the handle stays valid for it and
		// only the hub it reaches is closed.
	})
	return nil
}
