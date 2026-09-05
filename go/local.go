package download

import (
	"errors"

	job "github.com/openabstractions/abstraction-job/go"
)

// This file is the whole of what download knows about filesystems, and it exists
// so that the answer to "does this store have a directory?" is asked in one
// place instead of assumed in eight.
//
// Before it, callers reached through a concrete *job.FileStore for Root() to
// resolve a sink, to place a nudge socket and to write a supervisor heartbeat.
// Each of those is a mechanism of the FILE binding that had quietly become part
// of the generic layer, which is how a binding gets promoted to a design without
// anyone deciding to promote it.
//
// When a service binding arrives, nothing here needs new cases: it will not
// advertise job.Scratch, localRoot returns "", and every mechanism below either
// degrades honestly or says so.

// ErrNoLocalArea is returned by a mechanism that only means something when the
// store is a local filesystem.
//
// Returned rather than silently skipped on purpose. A supervisor that thinks it
// announced itself and did not is exactly the failure mode this project keeps
// finding: the caller must handle the case, not be spared knowing about it.
var ErrNoLocalArea = errors.New("download: this store has no local area — its binding announces and notifies its own way")

// localRoot is the store's own directory, or "" if its binding is not a
// filesystem at all.
func localRoot(store job.Store) string {
	if sc, ok := store.(job.Scratch); ok {
		return sc.Root()
	}
	return ""
}

// LocalSink resolves a sink's relative paths into paths on THIS machine.
//
// A relative path in a record means "under the store's own area", so that a
// record written by a PC and adopted by a NAS names one directory rather than
// one machine's view of it. Absolute paths are left exactly as the caller wrote
// them.
func LocalSink(store job.Store, s Sink) (partial, final string) {
	return s.Resolve(localRoot(store))
}
