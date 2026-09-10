//go:build windows

package download

import "github.com/openabstractions/abstraction-identity/listen"

// A pipe name is machine-wide, so the account is in the user-scope name. Without
// it the second account to start a supervisor is told the name is taken — the
// pipe's door is already this account's alone — and one machine would carry one
// user supervisor between all its people.
func endpointOf(s Scope) string {
	if s == MachineScope {
		return listen.Endpoint("jobs")
	}
	if who := Account(); who != "" {
		return listen.Endpoint("jobs-" + who)
	}
	return ""
}
