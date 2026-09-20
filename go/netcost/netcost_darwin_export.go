//go:build darwin && cgo

package netcost

// This file holds the exported callback. A file with //export may declare, and
// may not define, C in its preamble.

/*
#include <stdint.h>
*/
import "C"

import "runtime/cgo"

//export netcostPathUpdate
func netcostPathUpdate(handle C.uintptr_t, satisfied, expensive, constrained C.int) {
	if p, ok := cgo.Handle(handle).Value().(*pathMonitor); ok {
		p.update(classifyPath(satisfied != 0, expensive != 0, constrained != 0))
	}
}
