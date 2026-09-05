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

	download "github.com/ReinisLusis/abstraction/download/go"
	job "github.com/ReinisLusis/abstraction/job/go"

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
           [--digest sha256:<hex>]
  dl list                    what is in the store, and who is doing it
  dl watch                   follow everything, live
  dl tiers                   what this machine can delegate to, and why

Where a download runs is a property of this machine, not of the command. Set it
up once with:  jobd setup --nas-store //nas/share/store

--digest is an identity, not just a check. Given one, this looks for those exact
bytes in the content-addressed stores already on your machine — Ollama's,
HuggingFace's — and copies rather than downloads when it finds them. The digest
is verified either way, because those stores do not verify their own.`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "dl:", err)
	os.Exit(1)
}

// open is the whole of this program's setup. It used to be four lines that
// found a directory, opened a file store and built a runner — an application
// assembling the machinery it was supposed to be spared.
//
// dl still takes the job.Store alongside, because it reads and renders records
// and lives inside this layer. That is the INTERFACE, not the binding: dl cannot
// tell whether there is a directory behind it, which is exactly the property
// that was missing when this was a *job.FileStore.
func open() (download.Service, job.Store) {
	svc, store, err := download.Open()
	if err != nil {
		fatal(err)
	}
	return svc, store
}

func cmdTiers() {
	_, store := open()
	// A store that happens to be a directory says where. One that is not says
	// what it is, rather than inventing a path to look familiar.
	if sc, ok := store.(job.Scratch); ok {
		fmt.Printf("store        %s\n", sc.Root())
	} else {
		fmt.Printf("store        a service — no local directory\n")
	}

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
	digest := ""
	for i := 1; i < len(args)-1; i++ {
		switch args[i] {
		case "-o":
			out = args[i+1]
		case "--digest":
			digest = args[i+1]
		}
	}

	svc, store := open()
	// One call. Where the destination name comes from, whether a supervisor
	// exists, and who ends up moving the bytes are all settled below this line —
	// dl no longer asks and no longer branches.
	//
	// A digest changes what is possible rather than merely adding a check. It is
	// an identity, so the layer below can ask whether these exact bytes are
	// already on this machine — in somebody else's cache, under somebody else's
	// name — and turn the download into a local copy. Without one there is
	// nothing to match against and the network is the only answer.
	h, err := getWith(svc, url, out, digest)
	if err != nil {
		fatal(err)
	}
	id := h.ID()

	rec, err := h.Record()
	if err != nil {
		fatal(err)
	}
	spec, _ := download.SpecOf(rec)
	_, abs := download.LocalSink(store, spec.Sink)

	fmt.Printf("%s\n", url)
	fmt.Printf("  to        %s\n", abs)
	fmt.Printf("  job       %s\n", id)
	fmt.Printf("  fetched by %s\n\n", svc.Where())
	fmt.Println("(close this and run `dl list` — the job and its progress survive)")
	fmt.Println()

	done := make(chan error, 1)
	go func() { done <- waitForEnd(ctx, store, id) }()
	go follow(ctx, svc, store, id)

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

// waitForEnd blocks until the job stops being in flight, by watching the live
// collection — the only method that works when the process moving the bytes is
// not this one. It replaces waiting on a Runner, which only ever knew about a
// transfer this process was performing itself.
func waitForEnd(ctx context.Context, store job.Store, id string) error {
	sub := job.Watch(store, download.Kind)
	defer sub.Close()
	for {
		for _, rec := range sub.Records() {
			if rec.ID != id {
				continue
			}
			if rec.State == job.StateFailed {
				return fmt.Errorf("%s", rec.Error)
			}
			if rec.State.Terminal() || rec.State == job.StateTransferred {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub.Changes():
		}
	}
}

// follow prints progress by reading the record, which is the same thing any
// other process would do. Nothing here is privileged and nothing is in memory:
// stop this program, start it again, and the picture is identical.
func follow(ctx context.Context, svc download.Service, store job.Store, id string) {
	last := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
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
				if err := svc.TakeDelivery(id); err != nil {
					fmt.Fprintf(os.Stderr, "dl: %v\n", err)
				}
			}
			return
		}
	}
}

func statusLine(store job.Store, rec *job.Record) string {
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

func report(store job.Store, id string) {
	rec, err := store.Load(id)
	if err != nil {
		return
	}
	spec, err := download.SpecOf(rec)
	if err != nil {
		return
	}
	_, final := download.LocalSink(store, spec.Sink)
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
	_, store := open()
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
		_, final := download.LocalSink(store, spec.Sink)
		fmt.Printf("%-11s %s\n", statusLine(store, rec), filepath.Base(final))
	}
	if n == 0 {
		fmt.Println("nothing in the store.")
	}
}

func cmdWatch(ctx context.Context) {
	_, store := open()
	fmt.Println("watching the store. Ctrl+C to stop; nothing stops downloading.")

	// Redraw when something CHANGES, not on a timer.
	//
	// This used to reprint every two seconds and clear the screen with an ANSI
	// escape first. In a terminal that does not act on that escape the lines
	// simply accumulate, so a stalled download printed an identical row forever
	// and looked like several jobs. Worse, it was reprinting when nothing had
	// happened at all — the collection already knows the difference, so ask it.
	sub := job.Watch(store, download.Kind)
	defer sub.Close()

	for {
		all := sub.Records()
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
			_, final := download.LocalSink(store, spec.Sink)
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
		case <-sub.Changes():
		}
	}
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

// getWith is Get, plus an identity when the caller has one.
//
// Get takes a URL and a place to put it, which is all most callers have. When a
// digest is known it goes through Submit instead, because that is the path that
// can look the artifact up before fetching it.
func getWith(svc download.Service, url, out, digest string) (job.Job, error) {
	if digest == "" {
		return svc.Get(url, out)
	}
	dest := out
	if isDir(dest) {
		dest = filepath.Join(dest, nameFromURL(url))
	}
	abs, err := filepath.Abs(dest)
	if err != nil {
		return nil, err
	}
	return svc.Submit(download.Spec{
		Artifact: download.Artifact{Digest: digest},
		Sources:  []download.Source{{Scheme: schemeOf(url), Locator: url}},
		Sink:     download.Sink{Final: abs},
	})
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
