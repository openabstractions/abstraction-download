package download

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	config "github.com/openabstractions/abstraction-config/go"
	identity "github.com/openabstractions/abstraction-identity"
	job "github.com/openabstractions/abstraction-job/go"
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
//	import _ "github.com/openabstractions/abstraction-download/go/all"
//
// That import is the honest cost for a Fetcher, which streams bytes through
// this address space and therefore has to be in it. One line in a program,
// versus a path, a hostname and a branch in every program — which is the trade
// SLF4J made and the reason anyone adopted it.
//
// It is NOT the cost for a Delegator. A Delegator hands the whole job away and
// never carries a byte through us, so it need not be linked in, or written in
// Go, or written by us: a program launched over a pipe is one, portably on all
// three platforms, at one round trip per job rather than per byte. This comment
// said the opposite for months — that portable plugins were impossible — which
// was true of Fetcher and generalised to Tier without anybody checking. It
// argued well enough to end scrutiny, which is the expensive kind of wrong.
type Tier struct {
	// Name is what appears in job.Delegation.System.
	Name string

	// Publisher is who wrote this tier, and how firmly that is known.
	//
	// Zero means us: a tier registered from an init in this program was
	// published by whoever published the program, and offers() fills that in.
	Publisher Publisher

	// Over is what this tier is a provider over — the thing underneath it that
	// existed before it did — and Facility names that thing where it has a
	// name: "BITS", "curl", the engine an adopter already runs.
	//
	// Publisher says who wrote a tier; this says what they wrote it on top of,
	// and the two are independent: a tier somebody else published over a
	// platform facility is normal. Nothing here is filled in from a zero value,
	// because the one thing worth knowing is exactly what a default would hide.
	Over     Over
	Facility string

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

// Over is what a tier is a provider over.
//
// Three values, because they are the three that change what somebody should do
// about a tier: our own code, a facility the machine already has, or an engine
// somebody else runs. A tier registered from outside this repository sets it
// like any other field, which makes it a claim — read it with Publisher.Proof
// beside it, the way every other claim in this layer is read.
type Over string

const (
	OverOurs     Over = "ours"
	OverPlatform Over = "platform"
	OverForeign  Over = "foreign"
	// OverUndeclared is the zero value, and it is what a tier that did not say
	// reports. It is not a synonym for ours: guessing is what stops the answer
	// being an answer.
	OverUndeclared Over = ""
)

func (o Over) String() string {
	if o == OverUndeclared {
		return "undeclared"
	}
	return string(o)
}

var (
	tiersMu sync.Mutex
	tiers   []Tier
)

// RegisterTier makes a binding available to Discover, replacing whatever was
// registered under the same name. Call it from an init, or from a loader.
//
// Replacing rather than appending, because the name is what Delegation.System
// records and two tiers answering to one name make a stored handle ambiguous
// about who may interpret it. An init could not produce a duplicate; a
// directory of manifests a person edits while a supervisor runs produces one on
// the second read.
func RegisterTier(t Tier) {
	tiersMu.Lock()
	defer tiersMu.Unlock()
	for i := range tiers {
		if tiers[i].Name == t.Name {
			tiers[i] = t
			order()
			return
		}
	}
	tiers = append(tiers, t)
	order()
}

// UnregisterTier removes a tier by name, and reports whether it was there.
//
// Registration was append-only, which is right for an init and wrong for a
// manifest a person deletes: the tier stayed registered until the process
// restarted, so "delete the file" was not the whole of uninstalling and a bad
// tier could not be got rid of without stopping the service. Removing one has
// to be something a person can actually do — that is the Layered Service
// Provider lesson, where what made removal dangerous was that it broke the
// machine.
func UnregisterTier(name string) bool {
	tiersMu.Lock()
	defer tiersMu.Unlock()
	delete(disbelieved, name)
	for i := range tiers {
		if tiers[i].Name == name {
			tiers = append(tiers[:i], tiers[i+1:]...)
			return true
		}
	}
	return false
}

// disbelieved names, per tier, the claims that tier's own behaviour refuted.
var disbelieved = map[string]map[Capability]bool{}

// Disbelieve records that a tier claimed c and did the opposite, so nothing
// requiring c is offered to it again while this process runs.
//
// It survives re-probing on purpose, and that is the whole design decision.
// Tier.New runs again on every Offers and every Rebind — for a plugin, once a
// second behind an open control surface — and a re-probe is the same program
// repeating the same claim, which is not new evidence. Clearing on it would
// launder a refutation on a timer.
//
// So it is cleared by exactly one thing: the tier being unregistered, which is
// what deleting or replacing a plugin does (PL-14). A person who fixes a plugin
// gets it believed again; a person who does nothing does not. It is also not
// persisted, so a restart believes the claim once more and pays one poll to
// refute it again — the alternative is a durable accusation against a named
// program with no way to appeal it, which needs an expiry and a revocation
// story before it is worth having.
func Disbelieve(system string, c Capability) {
	tiersMu.Lock()
	defer tiersMu.Unlock()
	if disbelieved[system] == nil {
		disbelieved[system] = map[Capability]bool{}
	}
	disbelieved[system][c] = true
}

func disbelieves(system string, c Capability) bool {
	tiersMu.Lock()
	defer tiersMu.Unlock()
	return disbelieved[system][c]
}

// Disbelieved names the refuted claims of one tier, for a report or a status
// line. Empty for every tier that has not lied.
func Disbelieved(system string) []Capability {
	tiersMu.Lock()
	defer tiersMu.Unlock()
	out := make([]Capability, 0, len(disbelieved[system]))
	for c := range disbelieved[system] {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// publisherOf is who published a registered tier, for a message that has to
// name the program somebody else wrote. Unknown names are not ours and do not
// get to look like us.
func publisherOf(system string) Publisher {
	tiersMu.Lock()
	defer tiersMu.Unlock()
	for _, t := range tiers {
		if t.Name == system {
			return t.publisher()
		}
	}
	return Publisher{}
}

func order() {
	sort.SliceStable(tiers, func(i, j int) bool { return tiers[i].Priority < tiers[j].Priority })
}

// RegisteredTiers names what this program has registered, in priority order —
// linked in, or loaded from a manifest. Useful in a status command:
// "registered, but not available here" and "not registered at all" are very
// different problems and a user needs to tell them apart.
func RegisteredTiers() []string {
	tiersMu.Lock()
	defer tiersMu.Unlock()
	out := make([]string, 0, len(tiers))
	for _, t := range tiers {
		out = append(out, t.Name)
	}
	return out
}

// Offer is one linked tier and what this machine can do with it right now.
//
// The reason a tier is unusable was computed and thrown away. Tier.New already
// works it out — "the share is not reachable", "no NAS is configured" — and a
// person deciding where their downloads should go needs that sentence far more
// than they need the yes or no. Without it a control surface can only draw a
// switch that does nothing and cannot say why.
type Offer struct {
	System   string `json:"system"`
	Priority int    `json:"priority"`
	// By is who published this tier. A person deciding where their bytes come
	// from is entitled to read "served by nas, published by someone else"
	// rather than a name that looks exactly like ours.
	By Publisher `json:"by"`
	// Over and Facility are what this tier is a provider over. They are here so
	// that which adoption level a provider occupies is a query over the
	// registration seat rather than somebody reading the tree by hand.
	Over     Over   `json:"over"`
	Facility string `json:"facility,omitempty"`
	// Usable is whether work could be handed here now: linked, configured,
	// reachable, and not switched off.
	Usable bool `json:"usable"`
	// Off says a person switched it off, which is a different fact from the
	// machine being unable to use it, and Why then carries their words.
	Off bool   `json:"off"`
	Why string `json:"why,omitempty"`
	// Disbelieved are claims this tier made and its own behaviour refuted. A
	// tier is still usable with them missing — it lied about resuming, not
	// about downloading — and a person choosing where their bytes go is owed
	// the fact that it lied.
	Disbelieved []Capability `json:"disbelieved,omitempty"`
}

// Publisher is who wrote a tier, and how firmly that is known.
//
// The grade is identity.Proof, the same ladder this project uses for a peer on
// a socket, because the question is the same one and a second ladder would
// drift from it. It is what keeps an Offer from reading stronger than its
// evidence: a name out of a manifest is somebody's word for it, which is
// exactly what ProofClaimed exists to name and refuse to launder.
//
// This is the seat a signature would fill, and it is deliberately only a seat.
// A tier whose publisher the operating system validated reports ProofSigned;
// nothing here decides what a machine should require, and a program that wants
// to require a grade asks identity.CanEver whether this platform can ever
// produce it before promising a person a policy it cannot enforce.
type Publisher struct {
	Name  string         `json:"name,omitempty"`
	Proof identity.Proof `json:"proof"`
}

func (p Publisher) String() string {
	if p.Name == "" {
		return "unknown"
	}
	return fmt.Sprintf("%s[%s]", p.Name, p.Proof)
}

// publisher is us unless the tier said otherwise.
//
// ProofBound, not ProofClaimed: nobody asserted this name. Program reads the
// running image from the operating system, which is a record about a running
// process and no stronger — the same thing ProofBound means everywhere else.
func (t Tier) publisher() Publisher {
	if t.Publisher.Name != "" {
		return t.Publisher
	}
	return Publisher{Name: Program(), Proof: identity.ProofBound}
}

func offers(cfg config.Config) ([]Offer, []Delegator) {
	tiersMu.Lock()
	snapshot := append([]Tier(nil), tiers...)
	tiersMu.Unlock()

	out := make([]Offer, 0, len(snapshot))
	var usable []Delegator
	for _, t := range snapshot {
		o := Offer{System: t.Name, Priority: t.Priority, By: t.publisher(),
			Over: t.Over, Facility: t.Facility, Disbelieved: Disbelieved(t.Name)}
		if why, off := cfg.Off[t.Name]; off {
			o.Off, o.Why = true, why
			out = append(out, o)
			continue
		}
		switch d, err := probe(t, cfg); {
		case err != nil:
			o.Why = err.Error()
		case d == nil:
			o.Why = "this machine has no " + t.Name
		default:
			// A declaration this package cannot read is a tier it must not
			// offer work to. See Declaration.Valid.
			if err := declaredBy(d).Valid(); err != nil {
				o.Why = err.Error()
				d.Close()
				break
			}
			o.Usable = true
			usable = append(usable, d)
		}
		out = append(out, o)
	}
	return out, usable
}

// ProbeBudget bounds Tier.New. Nothing in the Delegator contract mentioned time
// while every implementation was ours, because a COM call and a directory read
// cannot hang the way a pipe can — and a plugin is a program that may accept a
// request and never answer.
var ProbeBudget = 5 * time.Second

var (
	probesMu sync.Mutex
	probing  = map[string]bool{}
)

// probe builds one tier under that bound.
//
// What happens when it expires: the tier is reported unusable with a reason a
// person can read, Offers returns, and the control surface keeps drawing. The
// WAIT is abandoned, not the goroutine — nothing can cancel a blocking syscall
// on a share whose machine went away, and Tier.New takes no context to cancel
// even where one would help. So whatever it eventually builds is closed by the
// drain below, and while it is outstanding the next poll is answered at once
// rather than starting a second one. A control surface polling this on a timer
// otherwise accumulates one stuck probe per second.
//
// The remaining gap, which this does not close: Poll, Start and Finalize can
// hang the same way, and there the caller is a supervisor rather than a window,
// so the answer is a deadline on the transport (research/plugins/CONTRACT.md).
func probe(t Tier, cfg config.Config) (Delegator, error) {
	probesMu.Lock()
	if probing[t.Name] {
		probesMu.Unlock()
		return nil, fmt.Errorf("%s has not answered the probe before this one", t.Name)
	}
	probing[t.Name] = true
	probesMu.Unlock()

	built := make(chan struct {
		d   Delegator
		err error
	}, 1)
	go func() {
		d, err := t.New(cfg)
		built <- struct {
			d   Delegator
			err error
		}{d, err}
		probesMu.Lock()
		delete(probing, t.Name)
		probesMu.Unlock()
	}()

	late := time.NewTimer(ProbeBudget)
	defer late.Stop()
	select {
	case got := <-built:
		return got.d, got.err
	case <-late.C:
		go func() {
			if got := <-built; got.d != nil {
				got.d.Close()
			}
		}()
		return nil, fmt.Errorf("%s did not answer a probe within %s", t.Name, ProbeBudget)
	}
}

func declaredBy(d Delegator) Declaration {
	if dec, ok := d.(Declares); ok {
		return dec.Declared()
	}
	return Declaration{}
}

// Offers asks every linked tier what it can do here, in priority order.
//
// Asking is building: Tier.New is the probe, so every tier that answered has
// been constructed, and this closes each one before returning. A control
// surface polling this pays a probe per poll — for a plugin that is a process
// launched and ended — which is the price of an answer about now rather than
// about startup.
func Offers(cfg config.Config) []Offer {
	o, built := offers(cfg)
	NewDelegators(built...).Close()
	return o
}

// available builds every registered tier that this machine can actually use.
func available(cfg config.Config) []Delegator { _, d := offers(cfg); return d }

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
