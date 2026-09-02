// dl downloads a URL, and shows you what actually happened.
//
// It is the generic downloader — the layer below modelget, which knows about
// models. dl knows about bytes.
//
// Note what it does NOT import: no bits, no nas, no tier of any kind. An
// application asks one question — is there a system downloader on this machine —
// and if there is, the job is already sitting in the store that service watches,
// so dl can exit. Whether the service then uses a NAS, the Windows transfer
// service, or its own two hands is the service's business, configured once.
//
// A library that logs knows about a facade and a default. It does not know which
// file or which collector the sinks point at, and it would be a worse library if
// it did.
//
//	dl https://example.com/big.iso
//	dl https://example.com/big.iso -o D:/downloads/
//	dl list                    everything in the store, whoever is doing it
//	dl watch                   follow it live
//
// The interesting thing to try is closing it mid-transfer and running `dl list`
// afterwards.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"time"

	download "github.com/ReinisLusis/abstraction-download"
	job "github.com/ReinisLusis/abstraction-job"

	_ "golang.org/x/crypto/x509roots/fallback"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch os.Args[1] {
	case "list":
		cmdList(ctx)
	case "watch":
		cmdWatch(ctx)
	case "tiers":
		cmdTiers()
	case "-h", "--help", "help":
		usage()
	default:
		cmdGet(ctx, os.Args[1:])
	}
}

func usage() {
	fmt.Println(`dl — download a URL, and see where it actually happens

  dl <url> [-o <dir|file>]   download it
  dl list                    what is in the store, and who is doing it
  dl watch                   follow everything, live
  dl tiers                   what this machine can delegate to, and why

Where a download runs is a property of this machine, not of the command. Set it
up once with:  jobd setup --nas-store //nas/share/store`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "dl:", err)
	os.Exit(1)
}

func openRunner() (*download.Runner, *job.FileStore) {
	root, err := download.StoreRoot()
	if err != nil {
		fatal(err)
	}
	store, err := job.NewFileStore(root)
	if err != nil {
		fatal(err)
	}
	return download.DiscoverIn(store), store
}

func cmdTiers() {
	_, store := openRunner()
	root, _ := download.StoreRoot()
	fmt.Printf("store        %s\n", root)

	sup, live := download.SupervisorOf(store)
	if !live {
		if sup.Owner != "" {
			fmt.Printf("supervisor   none (last seen %s, treated as gone)\n",
				sup.Seen.Format(time.RFC3339))
		} else {
			fmt.Println("supervisor   none")
		}
		fmt.Println()
		fmt.Println("Downloads run in this process and stop when it does. That is the bottom")
		fmt.Println("of the chain, not a failure. Start one with:  jobd run")
		return
	}
	fmt.Printf("supervisor   %s\n", sup.Owner)
	if sup.Tier != "" && sup.Tier != "here" {
		fmt.Printf("             which itself delegates to %q\n", sup.Tier)
	}
	fmt.Println()
	fmt.Println("Downloads are left for the system downloader, which keeps going with this")
	fmt.Println("program closed. Where it sends them is its business, not dl's.")
}

func cmdGet(ctx context.Context, args []string) {
	url := args[0]
	if !strings.Contains(url, "://") {
		usage()
		os.Exit(2)
	}
	out := "."
	for i := 1; i < len(args)-1; i++ {
		if args[i] == "-o" {
			out = args[i+1]
		}
	}

	dest := out
	if isDir(dest) {
		dest = filepath.Join(dest, nameFromURL(url))
	}
	abs, err := filepath.Abs(dest)
	if err != nil {
		fatal(err)
	}

	r, store := openRunner()
	id, err := download.Submit(store, download.Spec{
		Sources: []download.Source{{Scheme: schemeOf(url), Locator: url}},
		Sink:    download.Sink{Final: abs},
	})
	if err != nil {
		fatal(err)
	}

	fmt.Printf("%s\n", url)
	fmt.Printf("  to        %s\n", abs)
	fmt.Printf("  job       %s\n", id)
	// The only question an application gets to ask. Not "is there a NAS" — dl has
	// never heard of one — but "is something else on this machine going to do
	// this". If yes, the job is already in the store that service watches and
	// there is nothing further to do.
	if r.Handoff() == download.LeftToSupervisor {
		sup, _ := download.SupervisorOf(store)
		fmt.Printf("  left for  %s\n\n", sup.Owner)
		fmt.Println("The system downloader has it. This program can exit and the download")
		fmt.Println("will not stop. Following it anyway, by reading the record:")
		fmt.Println()
		follow(ctx, r, store, id, false)
		return
	}

	fmt.Printf("  no supervisor on this machine, so downloading here\n\n")
	fmt.Println("(close this and run `dl list` — the job and its progress survive)")
	fmt.Println()

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, id) }()
	go follow(ctx, r, store, id, false)

	if err := <-done; err != nil {
		if ctx.Err() != nil {
			fmt.Println("\ninterrupted. Progress is on disk — `dl watch` to see it resume.")
			os.Exit(130)
		}
		fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // let the follower print the last line
	report(store, id)
}

// follow prints progress by reading the record, which is the same thing any
// other process would do. Nothing here is privileged and nothing is in memory:
// stop this program, start it again, and the picture is identical.
func follow(ctx context.Context, r *download.Runner, store *job.FileStore, id string, reconcile bool) {
	last := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		if reconcile {
			r.Reconcile(ctx, id)
		}
		rec, err := store.Load(id)
		if err != nil {
			return
		}
		line := statusLine(store, rec)
		if line != last {
			fmt.Printf("\r%-78s", line)
			last = line
		}
		if rec.State == job.StateTransferred || rec.State.Terminal() {
			fmt.Println()
			if rec.State == job.StateTransferred {
				report(store, id)
				// Take delivery: the requester saying "I have it". Without this a
				// finished job waits in the store forever for somebody who never
				// comes, and `dl watch` fills with downloads that ended days ago.
				if err := r.TakeDelivery(id); err != nil {
					fmt.Fprintf(os.Stderr, "dl: %v\n", err)
				}
			}
			return
		}
	}
}

func statusLine(store *job.FileStore, rec *job.Record) string {
	where := "here"
	if rec.Delegated() {
		where = rec.Delegation.System
	}
	pct := ""
	if rec.Progress.Total > 0 {
		pct = fmt.Sprintf("%5.1f%%  %s / %s",
			100*float64(rec.Progress.Done)/float64(rec.Progress.Total),
			human(rec.Progress.Done), human(rec.Progress.Total))
	} else if rec.Progress.Done > 0 {
		pct = human(rec.Progress.Done)
	}
	note := ""
	if store.Claimable(rec) && !rec.State.Terminal() && rec.State != job.StateTransferred && !rec.Delegated() {
		note = "  (nobody is working on it)"
	}
	return fmt.Sprintf("%-11s %-9s %s%s", rec.State, where, pct, note)
}

func report(store *job.FileStore, id string) {
	rec, err := store.Load(id)
	if err != nil {
		return
	}
	spec, err := download.SpecOf(rec)
	if err != nil {
		return
	}
	_, final := spec.Sink.Resolve(store.Root())
	switch rec.State {
	case job.StateTransferred, job.StateComplete:
		fmt.Printf("\n%s\n", final)
		fmt.Printf("  %s", human(rec.Progress.Done))
		if spec.Artifact.Digest != "" {
			fmt.Printf(", digest verified")
		}
		fmt.Println()
	case job.StateFailed:
		fmt.Printf("\nfailed: %s\n", rec.Error)
	}
}

// cmdList reconciles before printing.
//
// Without that it reports the local record, which for a delegated job is only
// as fresh as the last time somebody looked — so a download the NAS finished
// minutes ago still reads "running". "What is downloading?" has to be true, not
// merely cheap to answer, and reconciling is a few file reads.
func cmdList(ctx context.Context) {
	r, store := openRunner()
	if r.Delegators != nil {
		r.ReconcileAll(ctx)
	}
	all, err := store.List()
	if err != nil {
		fatal(err)
	}
	n := 0
	for _, rec := range all {
		if rec.Kind != download.Kind {
			continue
		}
		n++
		spec, _ := download.SpecOf(rec)
		_, final := spec.Sink.Resolve(store.Root())
		fmt.Printf("%-11s %s\n", statusLine(store, rec), filepath.Base(final))
	}
	if n == 0 {
		fmt.Println("nothing in the store.")
	}
}

func cmdWatch(ctx context.Context) {
	r, store := openRunner()
	fmt.Println("watching the store. Ctrl+C to stop; nothing stops downloading.")
	for {
		if r.Delegators != nil {
			r.ReconcileAll(ctx)
		}
		all, _ := store.List()
		fmt.Print("\033[H\033[2J")
		live := 0
		for _, rec := range all {
			// Terminal and transferred are both finished. Transferred used to be
			// shown forever, because nothing in the layer ever took delivery, so
			// watch filled with downloads that had ended days earlier.
			if rec.Kind != download.Kind || rec.State.Terminal() || rec.State == job.StateTransferred {
				continue
			}
			spec, _ := download.SpecOf(rec)
			_, final := spec.Sink.Resolve(store.Root())
			fmt.Printf("%s %s\n", statusLine(store, rec), filepath.Base(final))
			live++
		}
		if live == 0 {
			fmt.Println("nothing in flight.")
		}
		select {
		case <-ctx.Done():
			fmt.Println("\nstopped watching. Anything in flight is still going.")
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func isDir(p string) bool {
	if strings.HasSuffix(p, "/") || strings.HasSuffix(p, `\`) || p == "." {
		return true
	}
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

func nameFromURL(u string) string {
	if i := strings.Index(u, "?"); i >= 0 {
		u = u[:i]
	}
	name := path.Base(u)
	if name == "" || name == "/" || name == "." {
		return "download.bin"
	}
	return name
}

func schemeOf(u string) string {
	if i := strings.Index(u, "://"); i > 0 {
		return u[:i]
	}
	return "https"
}

func human(n int64) string {
	const u = 1024
	if n < u {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(u), 0
	for m := n / u; m >= u; m /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
