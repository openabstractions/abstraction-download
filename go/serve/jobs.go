// Package serve is the download layer's resident half as something another
// program can call.
//
// It exists because `jobd run` was a main, and a main is the one shape a host
// program cannot host. One program registered once per capability is what every
// platform activates — a Windows service, a launchd job, a systemd unit — so
// the supervisor loop had to become a function. jobd still has it; so does openabstractions; the
// CLIs dl and jobctl are untouched.
package serve

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	config "github.com/openabstractions/abstraction-config/go"
	download "github.com/openabstractions/abstraction-download/go"
	_ "github.com/openabstractions/abstraction-download/go/all"
	identity "github.com/openabstractions/abstraction-identity"
	job "github.com/openabstractions/abstraction-job/go"
)

// StoreRoot finds the job store.
//
// ABSTRACTION_STORE is the name that belongs here. jobd supervises downloads of
// anything — it has never known what a model is — and reading MODELGET_STORE
// meant the generic tier was configured by a variable named after one consumer
// sitting above it. MODELGET_STORE is still honoured, because stores exist on
// disk under it and silently ignoring it would orphan jobs.
func StoreRoot() (string, error) {
	if v := os.Getenv("MODELGET_STORE"); v != "" {
		return v, nil
	}
	return config.JobStore()
}

// OpenRunner uses the same discovery every other application uses.
//
// It used to hand-wire the tiers itself, which meant the supervisor and the
// applications it supervises could disagree about what this machine has. Now
// this is just another caller of Discover: on a NAS that finds nothing — no
// BITS, no further NAS to pass work to — and the supervisor does the transfers
// itself, which is exactly what it is there for.
func OpenRunner(without ...string) (*download.Runner, job.Store, string, error) {
	root, err := StoreRoot()
	if err != nil {
		return nil, nil, "", err
	}
	store, err := job.NewFileStore(root)
	if err != nil {
		return nil, nil, "", err
	}
	r := download.DiscoverIn(store)
	// Whether other machines write this store is a property of the store, and
	// this used to be set unconditionally. That made installing the supervisor
	// take a capability away: `dl -o C:\models\x.gguf` delivered before jobd
	// existed and afterwards was refused on every sweep forever, because Get
	// absolutises a destination and a shared store refuses every absolute sink.
	r.SharedStore = download.SharedStoreRoot(root) || os.Getenv("ABSTRACTION_SHARED_STORE") != ""
	// An operator may run this supervisor one tier lower than the machine would
	// choose, to see what the next one down actually does. Applications get no
	// such control and should not: they do not know what a tier is.
	for _, w := range without {
		if w != "" {
			r.NotServing = append(r.NotServing, w)
		}
	}
	return r, store, r.Rebind(), nil
}

// Pass is one sweep, and the order matters.
//
// Reconcile first: a delegated job that has finished needs finalising and
// verifying, and doing that before adopting means the orphan pass does not pick
// up work the delegate has in fact already completed.
//
// The counts, and what went wrong. Both matter: a sweep that reports only what
// it managed describes a store where nothing needs attention exactly as it
// describes one where a job fails on every single pass. That is not
// hypothetical. `reconciled=2` printed every five seconds for two hours while
// one transfer could not progress at all — its error thrown away here, by
// taking the count only when err was nil.
func Pass(ctx context.Context, r *download.Runner) (reconciled, delegated, adopted, delivered int, problems []error) {
	note := func(stage string, err error) {
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", stage, err))
		}
	}

	if r.Delegators != nil {
		n, err := r.ReconcileAll(ctx)
		reconciled, _ = n, err
		note("reconcile", err)

		// Then offer anything unclaimed to a better tier. This is the second hop
		// of the chain, and it was missing: applications handed work to this
		// supervisor, and the supervisor downloaded everything itself because
		// nothing ever asked "should this go somewhere better?".
		n, err = r.DelegateAll(ctx)
		delegated = n
		note("delegate", err)
	}
	// Whatever nobody else wanted is still ours to finish.
	n, err := r.Adopt(ctx)
	adopted = n
	note("adopt", err)

	// And close out work that is demonstrably done. Without this a finished
	// download waits forever for an acknowledgement from a process that may
	// never come back, and shows up in a download manager as a row stuck at
	// 100% labelled "paused" — for a file that is complete on disk.
	n, err = r.TakeDeliveryAll(ctx)
	delivered = n
	note("deliver", err)

	return reconciled, delegated, adopted, delivered, problems
}

// DropFolder submits through the client so that a request for bytes the store
// already holds, or is already fetching, becomes that job rather than a second
// one.
func DropFolder(r *download.Runner) (download.Wanted, download.Client) {
	svc := download.NewClient(r)
	submit := func(s download.Spec) (string, error) {
		h, err := svc.Submit(s)
		if err != nil {
			return "", err
		}
		return h.ID(), nil
	}
	return download.Wanted{Store: r.Store, Submit: submit}, svc
}

// Sweep answers what has finished, then takes in what is new.
func Sweep(w download.Wanted) (taken []string, problems []error) {
	if err := w.Answer(); err != nil {
		return nil, []error{fmt.Errorf("wanted: %w", err)}
	}
	taken, err := w.TakeIn()
	if err != nil {
		problems = append(problems, fmt.Errorf("wanted: %w", err))
	}
	return taken, problems
}

// watchWanted is the drop folder's own loop, beside the sweep rather than
// inside it: a pass that fetches a file is one pass, and a person watching the
// folder should see the request move while that happens.
func watchWanted(ctx context.Context, w download.Wanted, store job.Store, every time.Duration) {
	sub := job.Watch(store, download.Kind)
	defer sub.Close()
	t := time.NewTicker(every)
	defer t.Stop()
	said := map[string]bool{}
	for {
		taken, problems := Sweep(w)
		if len(taken) > 0 {
			fmt.Printf("%s  wanted=%d\n", time.Now().Format(time.RFC3339), len(taken))
		}
		Complain(problems, said)
		select {
		case <-ctx.Done():
			return
		case <-sub.Changes():
		case <-t.C:
		}
	}
}

// Complain says each distinct problem once, not once per sweep. A stuck job
// fails identically every time, and a supervisor polling every five seconds
// would write the same line 17,000 times a day and bury everything else.
func Complain(problems []error, said map[string]bool) {
	for _, p := range problems {
		if msg := p.Error(); !said[msg] {
			said[msg] = true
			fmt.Fprintf(os.Stderr, "jobd: %s\n", msg)
		}
	}
}

// Systems collects a flag that may be given more than once.
//
// It was a plain string, and OpenRunner has always taken a variadic list, so
// `--without nas --without bits` parsed without complaint and silently kept
// only the last one. A documented escape hatch that quietly ignores half of
// what it is told is worse than one that refuses.
type Systems []string

func (s *Systems) String() string { return strings.Join(*s, ",") }

func (s *Systems) Set(v string) error {
	// Comma-separated too, because an operator typing this once should not have
	// to know which of the two spellings this program happens to accept.
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*s = append(*s, part)
		}
	}
	return nil
}

// Jobs supervises in the foreground until it is stopped. It is what `jobd run`
// and `openabstractions serve jobd` both are.
func Jobs(args []string) error { return JobsContext(context.Background(), args) }

// JobsContext is Jobs for a caller that has to be able to end it without a
// signal. On Windows a service stop is not a signal and SIGTERM is never
// delivered, so the SCM has no other way to reach the loop below.
func JobsContext(parent context.Context, args []string) error {
	fs := flag.NewFlagSet("jobd", flag.ContinueOnError)
	interval := fs.Duration("interval", 30*time.Second, "how often to sweep")
	endpoint := fs.String("endpoint", download.DefaultEndpoint(), "where applications connect")
	var without Systems
	fs.Var(&without, "without", `ignore a delegation system; repeatable, e.g. --without nas --without bits`)
	if err := fs.Parse(args); err != nil {
		return err
	}

	r, store, tier, err := OpenRunner(without...)
	if err != nil {
		return err
	}
	defer r.Close()
	root, _ := StoreRoot()
	fmt.Printf("jobd: watching %s (delegates to: %s)\n", root, tier)
	wanted, _ := DropFolder(r)
	dropDir, dropErr := wanted.Dir()
	if dropErr == nil {
		fmt.Printf("jobd: a text file put in %s becomes a download\n", dropDir)
	} else {
		fmt.Fprintf(os.Stderr, "jobd: no drop folder (%v)\n", dropErr)
	}

	// Announce, so applications on this machine stop downloading things
	// themselves. This is the entire discovery protocol: a file in the store both
	// sides can already see.
	owner := download.Owner()
	// What this supervisor delegates to is a live answer rather than one taken
	// at startup, so the sweep publishes it and the beat reads it. Two
	// goroutines, one field the sweep owns: r.Delegators is written by Rebind
	// and read all over the download package, and reading it from here would be
	// a race for a string.
	serving := &atomic.Pointer[string]{}
	serving.Store(&tier)
	// The bus before the heartbeat, because the heartbeat is how a caller
	// learns the bus's name. Without one the supervisor is still a supervisor:
	// it sweeps, and applications reach it through the store alone.
	var looks <-chan struct{}
	announce := ""
	if bus, err := download.ListenBus(*endpoint, owner, func() string { return *serving.Load() }); err == nil {
		defer bus.Close()
		looks, announce = bus.C(), bus.Endpoint
		l := identity.Ceiling()
		fmt.Printf("jobd: listening at %s (%s/%s; callers bound by %s)\n", announce, l.Platform, l.Transport, firstSentence(l.Binding))
	} else {
		fmt.Fprintf(os.Stderr, "jobd: no bus (%v); reachable through the store only, sweeping on the timer\n", err)
	}
	if err := download.Heartbeat(store, owner, tier, announce, *interval); err != nil {
		fmt.Fprintf(os.Stderr, "jobd: could not announce (%v); applications will download in-process\n", err)
	}
	// Stop announcing on a clean exit, so nothing hands work to a supervisor that
	// has gone. A kill leaves the heartbeat behind, which is why readers treat it
	// as stale rather than trusting it forever.
	defer download.StopHeartbeat(store)

	// Stop cleanly on Ctrl+C or a service stop. An interrupted sweep is safe —
	// the lease lapses and the next owner continues — but exiting tidily
	// releases it immediately instead of after the expiry.
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Announce on a clock of its own, because the thing that starves a heartbeat
	// is the work it is reporting on.
	//
	// This used to beat once per sweep, refreshed before the work rather than
	// after it, which is as far as one thread can get. It is not far enough: a
	// pass that fetches a 40 GB file IS a single sweep, so nothing was written
	// for as long as that took, the heartbeat aged past stale, and every
	// application on the machine was told nothing was watching — while this
	// process was in the middle of doing the work for them.
	//
	// Nothing is synchronised. The heartbeat is written by atomic rename, and it
	// reports liveness rather than progress, so a beat that lands mid-sweep says
	// exactly what it should: this process is still here.
	go func() {
		beat := time.NewTicker(*interval)
		defer beat.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-beat.C:
				download.Heartbeat(store, owner, *serving.Load(), announce, *interval)
			}
		}
	}()

	if dropErr == nil {
		go watchWanted(ctx, wanted, store, *interval)
	}

	t := time.NewTicker(*interval)
	defer t.Stop()
	// What has already been complained about, so a persistent failure is
	// reported when it starts rather than on every sweep forever.
	said := map[string]bool{}
	for {
		// Before the work, so a switch thrown in the control panel is obeyed by
		// this sweep rather than the one after it. Costs two small file reads
		// unless the answer changed.
		if now := r.Rebind(); now != *serving.Load() {
			serving.Store(&now)
			download.Heartbeat(store, owner, now, announce, *interval)
			fmt.Printf("%s  delegates-to=%s\n", time.Now().Format(time.RFC3339), now)
		}
		rec, del, ad, dlv, problems := Pass(ctx, r)
		if rec > 0 || del > 0 || ad > 0 || dlv > 0 {
			fmt.Printf("%s  reconciled=%d delegated=%d adopted=%d delivered=%d\n",
				time.Now().Format(time.RFC3339), rec, del, ad, dlv)
		}
		Complain(problems, said)
		select {
		case <-ctx.Done():
			fmt.Println("jobd: stopping. Anything in flight keeps its checkpoint.")
			return nil
		case <-looks:
		case <-t.C:
		}
	}
}

func firstSentence(s string) string {
	if i := strings.Index(s, ". "); i > 0 {
		return s[:i]
	}
	return s
}
