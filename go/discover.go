package download

import (
	config "github.com/ReinisLusis/abstraction-config"
	job "github.com/ReinisLusis/abstraction-job"
)

// Discover is the whole integration, for an application that knows nothing.
//
//	r, err := download.Discover("lemonade")
//
// It reads what this machine has — from the location a setup step wrote once,
// not from anything the caller supplies — registers every tier that is present
// AND reachable, and returns a Runner. A machine with a NAS set up delegates
// there. A Windows machine without one delegates to BITS. A machine with neither
// downloads in-process. The application branches on none of it and contains no
// path, no hostname and no flag.
//
// Presence is the configuration. Lemonade chooses the NAS about as much as a
// Java library chooses Logback.
func Discover(program string) (*Runner, error) {
	store, err := storeFor()
	if err != nil {
		return nil, err
	}
	return DiscoverIn(program, store), nil
}

// DiscoverIn is Discover against a store the caller already has open.
func DiscoverIn(program string, store *job.FileStore) *Runner {
	cfg := config.Load()
	r := NewRunner(store, Owner(program))
	r.Delegators = NewDelegators(available(cfg)...)
	return r
}

// Tier names what would answer, for a status line. An application can tell its
// user "this is going to your NAS" without knowing what a NAS is.
func (r *Runner) Tier() string {
	if r.Delegators == nil || len(r.Delegators.all) == 0 {
		return "here"
	}
	return r.Delegators.all[0].System()
}
