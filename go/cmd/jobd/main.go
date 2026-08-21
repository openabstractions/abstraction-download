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
	"syscall"
	"time"

	"github.com/ReinisLusis/abstraction-download"
	"github.com/ReinisLusis/abstraction-download/bits"
	job "github.com/ReinisLusis/abstraction-job"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "once":
		cmdOnce(os.Args[2:])
	case "install":
		cmdInstall(os.Args[2:])
	case "uninstall":
		cmdUninstall()
	case "status":
		cmdStatus()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println(`jobd — finishes transfers nobody is watching

  jobd run [--interval 30s]    supervise until stopped
  jobd once                    one pass, then exit (what a scheduled task runs)
  jobd install [--at-logon]    register a scheduled task, no elevation needed
  jobd uninstall               remove it
  jobd status                  what is in the store right now

env:
  MODELGET_STORE   the job store (default ~/.modelget), shared with modelget`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "jobd:", err)
	os.Exit(1)
}

func storeRoot() string {
	if v := os.Getenv("MODELGET_STORE"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		fatal(err)
	}
	return filepath.Join(home, ".modelget")
}

func openRunner() (*download.Runner, *job.FileStore, bool) {
	store, err := job.NewFileStore(storeRoot())
	if err != nil {
		fatal(err)
	}
	host, _ := os.Hostname()
	r := download.NewRunner(store, fmt.Sprintf("jobd@%s:%d", host, os.Getpid()))
	b := bits.New()
	if err := b.Available(); err == nil {
		r.Delegators = download.NewDelegators(b)
		return r, store, true
	}
	return r, store, false
}

// pass is one sweep, and the order matters.
//
// Reconcile first: a delegated job that has finished needs finalising and
// verifying, and doing that before adopting means the orphan pass does not pick
// up work the delegate has in fact already completed.
func pass(ctx context.Context, r *download.Runner) (reconciled, adopted int) {
	if r.Delegators != nil {
		n, err := r.ReconcileAll(ctx)
		if err == nil {
			reconciled = n
		}
	}
	n, err := r.Adopt(ctx)
	if err == nil {
		adopted = n
	}
	return reconciled, adopted
}

func cmdOnce(args []string) {
	fs := flag.NewFlagSet("once", flag.ExitOnError)
	quiet := fs.Bool("quiet", false, "say nothing unless something happened")
	fs.Parse(args)

	r, _, haveService := openRunner()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	rec, ad := pass(ctx, r)
	if *quiet && rec == 0 && ad == 0 {
		return
	}
	fmt.Printf("%s  reconciled=%d adopted=%d service=%v\n",
		time.Now().Format(time.RFC3339), rec, ad, haveService)
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	interval := fs.Duration("interval", 30*time.Second, "how often to sweep")
	fs.Parse(args)

	r, _, haveService := openRunner()
	fmt.Printf("jobd: watching %s (os transfer service: %v)\n", storeRoot(), haveService)

	// Stop cleanly on Ctrl+C or a service stop. An interrupted sweep is safe —
	// the lease lapses and the next owner continues — but exiting tidily
	// releases it immediately instead of after the expiry.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	t := time.NewTicker(*interval)
	defer t.Stop()
	for {
		if rec, ad := pass(ctx, r); rec > 0 || ad > 0 {
			fmt.Printf("%s  reconciled=%d adopted=%d\n",
				time.Now().Format(time.RFC3339), rec, ad)
		}
		select {
		case <-ctx.Done():
			fmt.Println("jobd: stopping. Anything in flight keeps its checkpoint.")
			return
		case <-t.C:
		}
	}
}

func cmdStatus() {
	_, store, haveService := openRunner()
	all, err := store.List()
	if err != nil {
		fatal(err)
	}
	fmt.Printf("store: %s\nos transfer service: %v\n\n", storeRoot(), haveService)
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
		case store.Claimable(rec) && !rec.State.Terminal():
			note = "  (stalled — nobody is working on it)"
		}
		claimable := note
		fmt.Printf("%-12s %-5s %-10s %s%s\n", rec.State, pct, who, filepath.Base(spec.Sink.Final), claimable)
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
