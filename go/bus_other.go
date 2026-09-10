//go:build !windows

package download

import (
	"runtime"

	"github.com/openabstractions/abstraction-identity/listen"
)

// XDG_RUNTIME_DIR is already one directory per account, so the user-scope name
// carries no account of its own and listen.Endpoint spells the whole of it.
//
// The machine-scope socket is spelled here instead, because listen.Endpoint has
// no scope to be told and its fallback is a world-writable temporary directory —
// not a door a machine service may put its endpoint behind. macOS has no /run
// and a daemon creating one would be making a directory at the root of the
// filesystem; /var/run is the same place on both, and is where a launchd
// Sockets key points.
func endpointOf(s Scope) string {
	if s == MachineScope {
		if runtime.GOOS == "darwin" {
			return "/var/run/openabstractions/jobs.sock"
		}
		return "/run/openabstractions/jobs.sock"
	}
	return listen.Endpoint("jobs")
}
