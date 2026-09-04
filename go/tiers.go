package download

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	config "github.com/ReinisLusis/abstraction-config"
	job "github.com/ReinisLusis/abstraction-job"
)

// Tier registration: how a binding makes itself available without this package
// knowing it exists.
//
// This package cannot import bits or nas — they import it — which is the same
// constraint SLF4J had and for the same reason: a facade that depends on its
// bindings is not a facade. SLF4J solved it with the classpath. Go solves it the
// way database/sql and image do, with an init that registers:
//
//	func init() { download.RegisterTier(download.Tier{ ... }) }
//
// and an application linking a binding in with a blank import:
//
//	import _ "github.com/ReinisLusis/abstraction-download/all"
//
// That import is the honest cost. In a compiled language "present" has to mean
// "linked in", and pretending otherwise would require plugins that do not work
// portably. One line in a program, versus a path, a hostname and a branch in
// every program — which is the trade SLF4J made and the reason anyone adopted it.
type Tier struct {
	// Name is what appears in job.Delegation.System.
	Name string

	// Priority orders the chain, lowest first. A NAS outranks the local OS
	// service because it is always on and this machine is not; the OS service
	// outranks in-process because it survives this process exiting.
	Priority int

	// New builds the Delegator, or returns an error if this machine cannot use
	// it. Returning an error is the normal case, not a failure: most machines
	// have no NAS, and Discover simply has one fewer tier to offer work to.
	//
	// It must PROBE, not just read configuration. A NAS that is switched off
	// must not be registered, because a job written into a directory nobody is
	// watching looks exactly like a download that started.
	New func(config.Config) (Delegator, error)
}

var (
	tiersMu sync.Mutex
	tiers   []Tier
)

// RegisterTier makes a binding available to Discover. Call it from an init.
func RegisterTier(t Tier) {
	tiersMu.Lock()
	defer tiersMu.Unlock()
	tiers = append(tiers, t)
	sort.SliceStable(tiers, func(i, j int) bool { return tiers[i].Priority < tiers[j].Priority })
}

// RegisteredTiers names what is linked into this binary, in priority order.
// Useful in a status command: "linked, but not available here" and "not linked
// at all" are very different problems and a user needs to tell them apart.
func RegisteredTiers() []string {
	tiersMu.Lock()
	defer tiersMu.Unlock()
	out := make([]string, 0, len(tiers))
	for _, t := range tiers {
		out = append(out, t.Name)
	}
	return out
}

// available builds every registered tier that this machine can actually use.
func available(cfg config.Config) []Delegator {
	tiersMu.Lock()
	snapshot := append([]Tier(nil), tiers...)
	tiersMu.Unlock()

	var out []Delegator
	for _, t := range snapshot {
		d, err := t.New(cfg)
		if err != nil || d == nil {
			continue
		}
		out = append(out, d)
	}
	return out
}

// Owner identifies this process in a lease: program, host and pid.
//
// All three are needed. A lease held by a process on another machine cannot be
// broken by checking whether that pid is alive here, so the host has to be on
// the record; the program name is what makes a stalled job legible to a human
// reading the store later.
//
// The program name is NOT a parameter, and that is deliberate. Asking an
// application to state its own name is asking for a claim, and this project has
// spent a lot of effort establishing that claims are the weak kind of identity.
// The operating system already knows which executable is running. Reading it
// from there is both less work for the caller and harder to get wrong — nobody
// can typo it, and nobody can copy an integration snippet from another project
// and end up with every lease in the store claiming to be "lemonade".
func Owner() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s@%s:%d", Program(), host, os.Getpid())
}

// Program is the name of the running executable, as the OS reports it.
//
// Falls back to os.Args[0] and then to "unknown". An empty or invented name
// would be worse than an honest "unknown": a store full of leases owned by
// nobody in particular is a store nobody can debug.
func Program() string {
	if exe, err := os.Executable(); err == nil && exe != "" {
		return strings.TrimSuffix(filepath.Base(exe), ".exe")
	}
	if len(os.Args) > 0 && os.Args[0] != "" {
		return strings.TrimSuffix(filepath.Base(os.Args[0]), ".exe")
	}
	return "unknown"
}

// storeFor opens the store, for Discover.
//
// The one place in this package that turns a configured location into a store.
// Where jobs live is config's answer, not download's — this layer used to
// export it, which invited every other program to ask the download package a
// question about jobs.
func storeFor() (job.Store, error) {
	root, err := config.JobStore()
	if err != nil {
		return nil, err
	}
	return job.NewFileStore(root)
}
