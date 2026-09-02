package download

import (
	config "github.com/ReinisLusis/abstraction-config"
	job "github.com/ReinisLusis/abstraction-job"
)

// Discover is the whole integration, for an application that knows nothing.
//
//	r, err := download.Discover()
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
func Discover() (*Runner, error) {
	store, err := storeFor()
	if err != nil {
		return nil, err
	}
	return DiscoverIn(store), nil
}

// DiscoverIn is Discover against a store the caller already has open.
func DiscoverIn(store *job.FileStore) *Runner {
	cfg := config.Load()
	r := NewRunner(store, Owner())
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

// Handoff is what an application does with a job it has just submitted, and the
// answer is one of two things.
type Handoff int

const (
	// RunHere: nothing else on this machine will do it, so this process must.
	RunHere Handoff = iota
	// LeftToSupervisor: a system downloader is watching this store and will pick
	// the job up. This process may exit.
	LeftToSupervisor
)

func (h Handoff) String() string {
	if h == LeftToSupervisor {
		return "the system downloader"
	}
	return "here"
}

// Handoff decides what an application should do with a submitted job, and it is
// deliberately the ONLY question an application gets to ask.
//
// It does not name BITS. It does not name a NAS. It answers "is there a system
// downloader on this machine", and if there is, the job is already in the store
// that service is watching, so there is nothing more to do — the supervisor
// adopts it, and the supervisor decides whether that means a NAS, the OS
// transfer service, or its own two hands.
//
// That is the delegation chain the project started with, and having applications
// link every tier was a departure from it. A library that logs does not know
// which sink is configured; an application that downloads should not know which
// machine ends up doing it.
func (r *Runner) Handoff() Handoff {
	if _, live := SupervisorOf(r.Store); live {
		return LeftToSupervisor
	}
	return RunHere
}
