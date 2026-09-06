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
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"time"

	asks "github.com/openabstractions/abstraction-asks/go"
	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
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

// open is this program's whole setup. dl takes the job.Store alongside the
// client because it reads and renders records; it gets the interface, never the
// binding.
func open() (download.Client, job.Store) {
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

	mayReach(ctx, url)
	svc, store := open()
	// Where the destination name comes from, whether a supervisor exists, and
	// who ends up moving the bytes are all settled below this line.
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
	_, abs, err := download.LocalSink(store, rec.ID, spec.Sink)
	if err != nil {
		fatal(err)
	}

	fmt.Printf("%s\n", url)
	fmt.Printf("  to        %s\n", abs)
	fmt.Printf("  job       %s\n", id)
	fmt.Printf("  fetched by %s\n\n", svc.Where())
	fmt.Println("(close this and run `dl list` — the job and its progress survive)")
	fmt.Println()

	done := make(chan error, 1)
	go func() { _, err := svc.Deliver(ctx, id); done <- err }()
	shown, stop := context.WithCancel(ctx)
	followed := make(chan struct{})
	go func() { follow(shown, store, id); close(followed) }()

	err = <-done
	stop()
	<-followed
	if err != nil {
		if ctx.Err() != nil {
			fmt.Println("\ninterrupted. Progress is on disk — `dl watch` to see it resume.")
			os.Exit(130)
		}
		fatal(err)
	}
	report(store, id)
}

// follow prints progress by reading the record, which is the same thing any
// other process would do. Nothing here is privileged and nothing is in memory:
// stop this program, start it again, and the picture is identical.
func follow(ctx context.Context, store job.Store, id string) {
	sub := job.Watch(store, download.Kind)
	defer sub.Close()
	last := ""
	for {
		n, err := sub.Next(ctx)
		if err != nil {
			return
		}
		for _, rec := range n.Records {
			if rec.ID != id {
				continue
			}
			if line := statusLine(store, rec); line != last {
				fmt.Printf("\r%-78s", line)
				last = line
			}
			if rec.State == job.StateTransferred || rec.State.Terminal() {
				fmt.Println()
				return
			}
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
	_, final, err := download.LocalSink(store, rec.ID, spec.Sink)
	if err != nil {
		fmt.Printf("\n%v\n", err)
		return
	}
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
		// A record whose sink leaves the store root is listed as what it is,
		// rather than as a blank name. Somebody has to be able to see the bad
		// record in order to remove it.
		_, final, err := download.LocalSink(store, rec.ID, spec.Sink)
		if err != nil {
			fmt.Printf("%-11s %v\n", statusLine(store, rec), err)
			continue
		}
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
			_, final, err := download.LocalSink(store, rec.ID, spec.Sink)
			if err != nil {
				fmt.Printf("%s %v\n", statusLine(store, rec), err)
				live++
				continue
			}
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
func getWith(svc download.Client, url, out, digest string) (job.Job, error) {
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

func mayReach(ctx context.Context, rawurl string) {
	u, err := url.Parse(rawurl)
	if err != nil {
		fatal(err)
	}
	c := &asks.Client{Endpoint: asks.DefaultEndpoint()}
	ask := asks.Ask{Asker: "dl", Key: "download.reach", Slots: map[string]string{"host": u.Host}}
	a, err := c.Ask(ctx, ask)
	if errors.Is(err, asks.ErrNoService) {
		fmt.Fprintf(os.Stderr, "dl: nothing on this machine answers questions; fetching from %s unasked\n", u.Host)
		return
	}
	if err != nil {
		fatal(err)
	}
	if a.Pending {
		fmt.Printf("dl has asked whether it may fetch from %s\n  a person answers:  asks pending  then  asks answer %s allow|once|refuse|never\n", u.Host, a.ID)
		if a, err = c.Await(ctx, ask); err != nil {
			fatal(err)
		}
	}
	switch {
	case a.Yes:
	case a.Kept:
		fatal(fmt.Errorf("a person said never to fetching from %s;  asks forget %s  reopens it", u.Host, a.ID))
	default:
		fatal(fmt.Errorf("a person refused fetching from %s this time", u.Host))
	}
}
