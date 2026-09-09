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
//
// It returns an error — rather than a best guess — for a path this machine must
// not write. The record was written by another machine and the writing is done
// with this one's authority, so the refusal happens here, at the boundary, and
// is not carried any further: a path that climbs out of the root
// (ErrEscapesRoot), one that aims at the store's own files (ErrReservedPath),
// and one that is absolute in a convention this platform does not use
// (ErrForeignPath).
//
// owner is the id of the job whose sink this is. work/<owner> is the store's
// scratch for that job and the one reserved path it may write, so a caller that
// passes the wrong id gets its own partial refused rather than someone else's
// accepted.
func LocalSink(store job.Store, owner string, s Sink) (partial, final string, err error) {
	return s.Resolve(localRoot(store), owner)
}
