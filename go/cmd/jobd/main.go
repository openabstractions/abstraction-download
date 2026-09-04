// jobd is the supervisor: the thing that finishes work nobody is watching.
//
// It does not move bytes. BITS does that, or a NAS does, or the in-process
// fetchers do. jobd exists because every one of those leaves a gap that only
// something long-running can close:
//
//   - **Delivery.** A delegated transfer that finishes while no application is
//     open sits there. BITS will not release the file until someone calls
//     Complete(), and nothing verifies the digest until someone asks. Without a
//     supervisor that happens the next time a human runs a command, which may be
//     days later — the bytes arrived on Tuesday and the file appeared on Friday.
//   - **Orphans.** A machine that reboots mid-transfer leaves a job with an
//     expired lease and a half-written file. Something has to pick it up.
//   - **One answer to "what is downloading?"** Every tool keeps that in its own
//     memory today, which is why a console pull never shows up in an app's
//     download list and why restarting a server empties it while the partial
//     files are still on disk.
//
// It is deliberately small: a loop over the job store, plus the two calls the
// download layer already exposes. Everything hard lives below it.
//
// # Why a scheduled task rather than a Windows service
//
// A real service means SCM plumbing and a dependency, and buys one thing over a
// task: jobs owned by LocalSystem keep running while the user is logged off,
// because that account "is always logged on". Under a normal user account BITS
// still survives the application closing and a reboot — it suspends at logoff
// and resumes at logon. For a desktop that is nearly the whole win, at no cost
// and with no elevation. Install it as a SYSTEM task later if logged-off
// transfers turn out to matter.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	config "github.com/ReinisLusis/abstraction-config"
	"github.com/ReinisLusis/abstraction-download"
	_ "github.com/ReinisLusis/abstraction-download/all"
	job "github.com/ReinisLusis/abstraction-job"

	// Mozilla's CA bundle, compiled in. Used only when the system has no trust
	// store of its own, which is exactly the case in a FROM scratch container.
	//
	// This is what lets the image contain one file. Without it the binary needs
	// /etc/ssl/certs/ca-certificates.crt from somewhere, that somewhere was
	// Alpine, and the whole image existed to deliver one text file — with the
	// real payload bind-mounted in from the host, which is not an image at all.
	//
	// It lives here in main rather than in the library, so nothing that imports
	// the download package inherits a root store it did not ask for.
	_ "golang.org/x/crypto/x509roots/fallback"
)

func main() {
	// Bare `jobd` answers the question somebody typing it is actually asking —
	// is anything running, and what is it doing — rather than printing usage at
	// them. Usage is still one keystroke away as `jobd help`.
	if len(os.Args) < 2 {
		cmdStatus()
		return
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "start":
		cmdStart(os.Args[2:])
	case "stop":
		cmdStop()
	case "-h", "--help", "help":
		usage()
	case "once":
		cmdOnce(os.Args[2:])
	case "install":
		cmdInstall(os.Args[2:])
	case "uninstall":
		cmdUninstall()
	case "status":
		cmdStatus()
	case "setup":
		cmdSetup(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println(`jobd — finishes transfers nobody is watching

  jobd                         is one running, and what is it doing
  jobd start [--without nas]   replace any running supervisor with a detached one
  jobd stop                    stop the one watching this store
  jobd run [--interval 30s]    supervise in the foreground until stopped
  jobd once                    one pass, then exit (what a scheduled task runs)
  jobd install [--at-logon]    register a scheduled task, no elevation needed
  jobd uninstall               remove it
  jobd status                  what is in the store right now
  jobd setup --nas-store <p>   record what this machine has, once, so that every
                               application finds it without being configured
  jobd setup --show            what is configured, and which file said so

env:
  ABSTRACTION_STORE      the job store (default ~/.abstraction). Any tool that
                         speaks the job record shares it — jobd knows nothing
                         about what is being downloaded
  MODELGET_STORE         honoured as a legacy alias
  ABSTRACTION_NAS_STORE  a store on a share watched by a jobd elsewhere; when
                         set and reachable, work is handed there rather than run`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "jobd:", err)
	os.Exit(1)
}

// storeRoot finds the job store.
//
// ABSTRACTION_STORE is the name that belongs here. jobd supervises downloads of
// anything — it has never known what a model is — and reading MODELGET_STORE
// meant the generic tier was configured by a variable named after one consumer
// sitting above it. MODELGET_STORE is still honoured, because stores exist on
// disk under it and silently ignoring it would orphan jobs.
func storeRoot() string {
	// MODELGET_STORE is honoured for stores that already exist under it;
	// everything else comes from the shared discovery.
	if v := os.Getenv("MODELGET_STORE"); v != "" {
		return v
	}
	root, err := config.JobStore()
	if err != nil {
		fatal(err)
	}
	return root
}

// openRunner uses the same discovery every other application uses.
//
// It used to hand-wire the tiers itself, which meant the supervisor and the
// applications it supervises could disagree about what this machine has. Now
// jobd is just another caller of Discover: on a NAS that finds nothing — no
// BITS, no further NAS to pass work to — and the supervisor does the transfers
// itself, which is exactly what it is there for.
func openRunner(without ...string) (*download.Runner, job.Store, string) {
	store, err := job.NewFileStore(storeRoot())
	if err != nil {
		fatal(err)
	}
	r := download.DiscoverIn(store)
	// An operator may run this supervisor one tier lower than the machine would
	// choose, to see what the next one down actually does. Applications get no
	// such control and should not: they do not know what a tier is.
	for _, w := range without {
		if w != "" {
			r.Delegators = r.Delegators.Without(w)
		}
	}
	return r, store, r.Tier()
}

// pass is one sweep, and the order matters.
//
// Reconcile first: a delegated job that has finished needs finalising and
// verifying, and doing that before adopting means the orphan pass does not pick
// up work the delegate has in fact already completed.
// The counts, and what went wrong. Both matter: a sweep that reports only what
// it managed describes a store where nothing needs attention exactly as it
// describes one where a job fails on every single pass.
//
// That is not hypothetical. `reconciled=2` printed every five seconds for two
// hours while one transfer could not progress at all — its error thrown away
// here, by taking the count only when err was nil.
func pass(ctx context.Context, r *download.Runner) (reconciled, delegated, adopted, delivered int, problems []error) {
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
		// nothing ever asked "should this go somewhere better?". A configured,
		// reachable, registered NAS was never used.
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

func cmdOnce(args []string) {
	fs := flag.NewFlagSet("once", flag.ExitOnError)
	quiet := fs.Bool("quiet", false, "say nothing unless something happened")
	fs.Parse(args)

	r, _, tier := openRunner()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	rec, del, ad, dlv, problems := pass(ctx, r)
	// Problems are never quiet. --quiet means "say nothing when there was
	// nothing to do", not "hide work that failed".
	for _, p := range problems {
		fmt.Fprintf(os.Stderr, "jobd: %v\n", p)
	}
	if *quiet && rec == 0 && del == 0 && ad == 0 && dlv == 0 && len(problems) == 0 {
		return
	}
	fmt.Printf("%s  reconciled=%d delegated=%d adopted=%d delivered=%d delegates-to=%s\n",
		time.Now().Format(time.RFC3339), rec, del, ad, dlv, tier)
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	interval := fs.Duration("interval", 30*time.Second, "how often to sweep")
	var without systems
	fs.Var(&without, "without", `ignore a delegation system; repeatable, e.g. --without nas --without bits`)
	fs.Parse(args)

	r, store, tier := openRunner(without...)
	fmt.Printf("jobd: watching %s (delegates to: %s)\n", storeRoot(), tier)

	// Announce, so applications on this machine stop downloading things
	// themselves. This is the entire discovery protocol: a file in the store both
	// sides can already see. An application asks "is a system downloader alive
	// here", never "is there a NAS" — which tier this supervisor uses is its
	// business, one hop further down.
	owner := download.Owner()
	if err := download.Heartbeat(store, owner, tier, *interval); err != nil {
		fmt.Fprintf(os.Stderr, "jobd: could not announce (%v); applications will download in-process\n", err)
	}
	// Stop announcing on a clean exit, so nothing hands work to a supervisor that
	// has gone. A kill leaves the heartbeat behind, which is why readers treat it
	// as stale rather than trusting it forever.
	defer download.StopHeartbeat(store)

	// Stop cleanly on Ctrl+C or a service stop. An interrupted sweep is safe —
	// the lease lapses and the next owner continues — but exiting tidily
	// releases it immediately instead of after the expiry.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Listen for nudges. An application that has just submitted work can say
	// "look now" instead of leaving it to sit until the next tick. The socket is
	// an accelerant, never a source of truth: it carries no job id and no
	// payload, so losing it costs latency and nothing else — which is why the
	// ticker below stays exactly as it was.
	var nudges <-chan struct{}
	if n, err := download.ListenForNudges(store); err == nil {
		defer n.Close()
		nudges = n.C()
	} else {
		fmt.Fprintf(os.Stderr, "jobd: not listening for nudges (%v); sweeping on the timer only\n", err)
	}

	// Announce on a clock of its own, because the thing that starves a heartbeat
	// is the work it is reporting on.
	//
	// This used to beat once per sweep, refreshed before the work rather than
	// after it, which is as far as one thread can get. It is not far enough: a
	// pass that fetches a 40 GB file IS a single sweep, so nothing was written
	// for as long as that took, the heartbeat aged past stale, and every
	// application on the machine was told nothing was watching — while this
	// process was in the middle of doing the work for them. Observed on a 1.5 GB
	// download: `jobd status` said "treated as gone" about a supervisor that was
	// busy on its behalf.
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
				download.Heartbeat(store, owner, tier, *interval)
			}
		}
	}()

	t := time.NewTicker(*interval)
	defer t.Stop()
	// What has already been complained about, so a persistent failure is
	// reported when it starts rather than on every sweep forever.
	said := map[string]bool{}
	for {
		rec, del, ad, dlv, problems := pass(ctx, r)
		if rec > 0 || del > 0 || ad > 0 || dlv > 0 {
			fmt.Printf("%s  reconciled=%d delegated=%d adopted=%d delivered=%d\n",
				time.Now().Format(time.RFC3339), rec, del, ad, dlv)
		}
		// Once per distinct problem, not once per sweep. A stuck job fails
		// identically every time, and a supervisor polling every five seconds
		// would write the same line 17,000 times a day and bury everything else
		// in its own log.
		for _, p := range problems {
			if msg := p.Error(); !said[msg] {
				said[msg] = true
				fmt.Fprintf(os.Stderr, "jobd: %s\n", msg)
			}
		}
		select {
		case <-ctx.Done():
			fmt.Println("jobd: stopping. Anything in flight keeps its checkpoint.")
			return
		case <-nudges:
		case <-t.C:
		}
	}
}

func cmdStatus() {
	_, store, tier := openRunner()
	all, err := store.List()
	if err != nil {
		fatal(err)
	}
	fmt.Printf("store: %s\n", storeRoot())
	// Whether a supervisor is ALIVE is the first thing anyone typing this wants
	// to know, and it was the one thing missing: the old output described what
	// tier this process would use if it ran, which reads as a running service
	// when nothing is running at all.
	if sup, live := download.SupervisorOf(store); live {
		where := sup.Tier
		if where == "" {
			where = "here"
		}
		fmt.Printf("supervisor: %s, running, delegates to: %s\n\n", sup.Owner, where)
	} else if sup.Owner != "" {
		fmt.Printf("supervisor: none (%s last seen %s, treated as gone)\n", sup.Owner, sup.Seen.Format(time.RFC3339))
		fmt.Printf("            nothing is finishing these; `jobd start` fixes that\n")
		fmt.Printf("if started here it would delegate to: %s\n\n", tier)
	} else {
		fmt.Printf("supervisor: none — downloads only run while an application is open\n")
		fmt.Printf("            `jobd start` changes that\n")
		fmt.Printf("if started here it would delegate to: %s\n\n", tier)
	}
	n := 0
	for _, rec := range all {
		if rec.Kind != download.Kind {
			continue
		}
		n++
		spec, _ := download.SpecOf(rec)
		who := "here"
		if rec.Delegated() {
			who = rec.Delegation.System
		}
		pct := ""
		if rec.Progress.Total > 0 {
			pct = fmt.Sprintf("%3.0f%%", 100*float64(rec.Progress.Done)/float64(rec.Progress.Total))
		}
		// Only flag work that is genuinely stalled. StateTransferred is finished
		// and proven, waiting to be acknowledged, so nobody should be working on
		// it — saying "nobody is working on it" there reads as a problem when it
		// is the expected end state.
		note := ""
		switch {
		case rec.State == job.StateTransferred:
			note = "  (done, waiting to be taken delivery of)"
		case rec.Paused():
			// Before every other reading, because a paused job is delegated,
			// claimable and not terminal all at once — so every other branch
			// here would describe it as something it is not. Saying "nas is
			// working on it" about a transfer somebody just stopped is the same
			// class of lie as calling a finished one paused.
			by := ""
			if rec.Intent != nil && rec.Intent.By != "" {
				by = " by " + rec.Intent.By
			}
			note = "  (paused" + by + ")"
		case rec.Delegated() && !rec.Delegation.Delivered && !rec.State.Terminal():
			// A delegated job holds no lease HERE and never will, because the
			// work is happening somewhere else — so "claimable" says yes and
			// means nothing. Calling that stalled told a person their download
			// had died while a NAS was actively fetching it, which is the most
			// alarming possible way to be wrong.
			note = "  (" + rec.Delegation.System + " is working on it)"
		case store.Claimable(rec) && !rec.State.Terminal():
			note = "  (stalled — nobody is working on it)"
		}
		claimable := note
		fmt.Printf("%-12s %-5s %-10s %s%s\n", rec.State, pct, who, filepath.Base(spec.Sink.Final), claimable)
		// Which phase, when the work has more than one. This is what a delegated
		// download was missing: the far side finishes, and then the bytes still
		// have to cross a share and be re-hashed, which used to show as a job
		// sitting at 100% doing nothing for minutes.
		if st := rec.Progress.Step; st != nil {
			where := ""
			if st.Total > 0 {
				where = fmt.Sprintf(" %3.0f%%", 100*float64(st.Done)/float64(st.Total))
			}
			if st.Of > 0 {
				fmt.Printf("             step %d/%d %s%s\n", st.Ordinal, st.Of, st.Name, where)
			} else {
				fmt.Printf("             %s%s\n", st.Name, where)
			}
		}
		if rec.Error != "" {
			fmt.Printf("             %s\n", rec.Error)
		}
	}
	if n == 0 {
		fmt.Println("nothing here.")
	}
}

const taskName = "jobd"

// cmdInstall registers a scheduled task. No elevation: it runs as the current
// user, which is enough for BITS to keep transferring across an application
// exit and a reboot. Running as SYSTEM would additionally survive a logoff and
// does need elevation — see the package comment.
func cmdInstall(args []string) {
	fs := flag.NewFlagSet("install", flag.ExitOnError)
	atLogon := fs.Bool("at-logon", true, "also run at logon, not only on a timer")
	every := fs.Int("every-minutes", 5, "how often the task sweeps")
	fs.Parse(args)

	exe, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	cmd := fmt.Sprintf(`"%s" once --quiet`, exe)

	fmt.Println("Run these once, in a normal (non-elevated) terminal:")
	fmt.Println()
	if *atLogon {
		fmt.Printf("  schtasks /create /tn %s-logon /sc onlogon /tr %s /f\n", taskName, quote(cmd))
	}
	fmt.Printf("  schtasks /create /tn %s /sc minute /mo %d /tr %s /f\n", taskName, *every, quote(cmd))
	fmt.Println()
	fmt.Println("Then check it with:  jobd status")
	fmt.Println()
	fmt.Println("Deliberately printed rather than executed: registering a scheduled")
	fmt.Println("task is a change to your machine, and you should see exactly what it")
	fmt.Println("is before it happens.")
}

func cmdUninstall() {
	fmt.Println("Run these once:")
	fmt.Println()
	fmt.Printf("  schtasks /delete /tn %s /f\n", taskName)
	fmt.Printf("  schtasks /delete /tn %s-logon /f\n", taskName)
}

func quote(s string) string { return `"` + s + `"` }

// systems collects a flag that may be given more than once.
//
// It was a plain string, and openRunner has always taken a variadic list, so
// `--without nas --without bits` parsed without complaint and silently kept
// only the last one. A documented escape hatch that quietly ignores half of
// what it is told is worse than one that refuses: the operator watches the
// supervisor keep using the tier they just excluded.
type systems []string

func (s *systems) String() string { return strings.Join(*s, ",") }

func (s *systems) Set(v string) error {
	// Comma-separated too, because an operator typing this once should not have
	// to know which of the two spellings this program happens to accept.
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*s = append(*s, part)
		}
	}
	return nil
}
