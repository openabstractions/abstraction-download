# abstraction-download

*For applications that fetch large files through OA. Provider authors start at
[For provider implementers](#for-provider-implementers-and-the-deprecated-in-process-client).*

A download here is a job: the application submits it to OA's shared runtime and
reads the result later. The example is in
[Through the shared runtime](#through-the-shared-runtime).

**What's new here:** A download outlives the application that asked: the
runtime runs it as a job, it survives the service restarting, the bytes are
verified against a digest when one is given, and a digest already fetched is
copied instead of fetched again.

Give an application a download that can keep running after the application
closes. The request says where the bytes may come from, how to verify them and
where the finished content belongs. A digest identifies the bytes once: a
finished record with that digest is copied rather than fetched again, whoever
requested it first. The runtime delegates the transfer to an allowed
downloader, keeps progress in one job table and reports a recoverable result.

A finished download is not readable through
[`abstraction-storage`](https://github.com/openabstractions/abstraction-storage)'s
`content-reader@1` by its digest: the runtime never copies a download's bytes
into the content root on completion. [The worked bridge](#into-the-content-store)
below reads a finished download's bytes and writes them through
`content-writer@1`, the step that makes them readable by digest.

## Through the shared runtime

An application submits a download as a job through
[the facade](https://github.com/openabstractions/abstraction-facade), shows
its progress, and puts the finished file where it wants it:

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	facade "github.com/openabstractions/abstraction-facade/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
)

func main() {
	url, path := os.Args[1], os.Args[2] // an https URL, and where to save it
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	jobs, err := facade.Discover().ResolveJobs(ctx, facade.Requirements{})
	if err != nil {
		log.Fatal(err)
	}
	window, err := jobs.GetHistoryWindow(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// No digest and no size: both are unknown until the transfer reveals them.
	spec := request.Encode(&request.Request{Sources: []request.Source{{Scheme: "https", Locator: url}}})
	sum := sha256.Sum256([]byte(url)) // a key stays under 1 KiB whatever the URL
	id := api.RequestIdentity{Key: "fetch-" + hex.EncodeToString(sum[:]), HistoryEpoch: window.HistoryEpoch}
	accepted, err := jobs.Submit(ctx, api.Submission{Identity: id, Kind: download.Kind, Spec: spec})
	if err != nil {
		log.Fatal(err)
	}
	if accepted.Outcome != api.AcceptanceOutcomeAccepted {
		log.Fatalf("not accepted: %s %s", accepted.Outcome, accepted.Reason)
	}

	for { // progress until the job ends
		observed, err := jobs.ObserveWork(ctx, id)
		if err != nil {
			log.Fatal(err)
		}
		if observed.Outcome != api.ObservationOutcomeObserved {
			log.Fatalf("observe: %s", observed.Outcome)
		}
		job := observed.Snapshot
		fmt.Printf("\r%d of %d bytes", job.Progress.Done, job.Progress.Total)
		if job.State == api.WorkStateComplete {
			break
		}
		if job.State == api.WorkStateFailed || job.State == api.WorkStateCancelled {
			log.Fatalf("\ndownload %s", job.State)
		}
		time.Sleep(time.Second)
	}

	// Link the result into place; copy it where a link cannot reach.
	destination, err := filepath.Abs(path)
	if err != nil {
		log.Fatal(err)
	}
	link, err := jobs.LinkResult(ctx, id, destination)
	if err != nil {
		log.Fatal(err)
	}
	switch link.Outcome {
	case api.LinkOutcomeLinked:
	case api.LinkOutcomeRemote, api.LinkOutcomeOtherVolume, api.LinkOutcomeNoHardLinks,
		api.LinkOutcomeUnsupported, api.LinkOutcomeForbidden:
		file, err := os.Create(destination)
		if err != nil {
			log.Fatal(err)
		}
		_, err = jobs.CopyResult(ctx, id, file)
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("\nlink: %s", link.Outcome)
	}
	fmt.Println("\nsaved", destination)
}
```

`request.Encode` serializes the generated download request into the job's
`Spec`, and `Submit` with `download.Kind` routes it to the download service.
The digest and size are optional: an empty digest and a zero size mean
unknown (`request.thrift`). When the publisher gives a SHA-256, put it in
`Artifact.Digest` and the service verifies the bytes before it reports the
job complete. `ObserveWork` reports `Done` of `Total` bytes, with `Total` 0
until the size is known. `LinkResult` hard-links the finished file to an
absolute path that does not exist yet and never copies. On another volume, a
file system without hard links or a remote runtime it answers `other_volume`,
`no_hard_links` or `remote`; a runtime without linking or a refused link
answers `unsupported` or `forbidden`. In each case `CopyResult` writes the
bytes instead. A file already at the path stops the link with `exists`, so
remove the old file before a second run.

Running the program again with the same URL presents the same idempotency key,
derived from the URL's SHA-256,
and finds the same job, so an interrupted run resumes and fetches nothing
twice. A timeout or a closed program ends only the wait; `CancelWork` stops the
job.

The shipped command follows that path:

```console
openabstractions download https://example.invalid/artifact --out /destination
openabstractions jobs list
```

Use `--sha256` when the publisher supplies a digest. Credentials are registered
by name through `openabstractions credentials`; URLs should carry no secrets.

## Surviving a restart

A program that may restart before the job ends keeps two things from the
first run: the `RequestIdentity` it submitted and `jobs.Binding()`. A later
run rebinds and reconciles the same job; it never submits again:

```go
// resume finds a download a previous run submitted. saved is what that run
// kept from jobs.Binding(), and id is its RequestIdentity.
func resume(ctx context.Context, saved client.JobsBinding, id api.RequestIdentity) (*facade.JobsClient, error) {
	jobs, err := facade.Discover().RestoreJobs(ctx, saved)
	if err != nil {
		return nil, err
	}
	reconciled, err := jobs.Reconcile(ctx, id)
	if err != nil {
		return nil, err
	}
	if reconciled.Outcome != api.AcceptanceOutcomeAccepted {
		return nil, fmt.Errorf("reconcile: %s %s", reconciled.Outcome, reconciled.Reason)
	}
	return jobs, nil // observe and link the result as the example above does
}
```

`Reconcile` answers for the original submission, including a submission whose
reply was lost. [The job module's
example](https://github.com/openabstractions/abstraction-job#an-example-that-runs)
carries the full recovery pattern, including retry after a failed attempt.

## Into the content store

The content root is a fixed directory the runtime keeps under its own state
directory (`%LOCALAPPDATA%\openabstractions\runtime-v1` on Windows,
`~/.local/share/openabstractions/runtime-v1` on Linux,
`~/Library/Application Support/openabstractions/runtime-v1` on macOS, or the
`--state-dir` an isolated runtime was given), populated only by
`content-writer@1` commits. A job-service
download's destination is always its own result document (contract rule
DL-R36), a location the runtime owns, never that content root, and nothing
copies one into the other on completion. An application reaches the bytes
through `LinkResult`, `CopyResult` or `ReadResult`. To make them readable by
digest through
[`abstraction-storage`](https://github.com/openabstractions/abstraction-storage)'s
`content-reader@1`, page them out of `ReadResult` and write them through
`content-writer@1`'s `Begin`/`Append`/`Commit` yourself; nothing does this for
you.

The bytes end up hashed twice: once inside the download, proven against the
request's `Artifact.Digest` before it is reported complete, and again by
`content-writer@1`'s `Commit`, which independently recomputes the SHA-256 of
every appended byte and compares it against whatever digest `Begin` declared.
Because both hashes cover the identical bytes, the download's own already-
proven digest can be declared at `Begin` without a third, application-side
hash. `Commit` recomputes and checks the hash itself before it accepts the
upload, on its own evidence.

```go
// store writes a completed download into the content store, where
// content-reader@1 reads it by digest. digest and size are the request's
// Artifact, which the download proved before it completed.
func store(ctx context.Context, jobs *facade.JobsClient, id api.RequestIdentity, digest string, size int64) error {
	writer, err := facade.Discover().ResolveStorageWriter(ctx, facade.Requirements{})
	if err != nil {
		return err
	}
	writeID, err := storage.NewRequestID() // retain before writing
	if err != nil {
		return err
	}
	begun, err := writer.Begin(ctx, writeID, digest, size)
	if err != nil {
		return err
	}
	switch begun.Outcome {
	case content.BeginOutcomeCommitted, content.BeginOutcomePresent:
		return nil // another writer already stored these bytes
	case content.BeginOutcomeStarted:
	default:
		return fmt.Errorf("begin: %s", begun.Outcome)
	}
	upload := *begun.Upload
	for offset := upload.Received; ; {
		chunk, err := jobs.ReadResult(ctx, id, offset, 65536)
		if err != nil {
			return err
		}
		if chunk.Outcome != api.ResultOutcomeData {
			return fmt.Errorf("result: %s", chunk.Outcome)
		}
		appended, err := writer.Append(ctx, upload, offset, chunk.Chunk.Data)
		if err != nil {
			return err
		}
		upload.Received, offset = appended.Received, appended.Received
		if chunk.Chunk.EOF {
			break
		}
	}
	_, err = writer.Commit(ctx, upload)
	return err
}
```

`storage` is `github.com/openabstractions/abstraction-storage/go/client` and
`content` is
`github.com/openabstractions/abstraction-storage/go/abstraction/storage/content`,
the same package [the storage README](https://github.com/openabstractions/abstraction-storage#reading-content)
imports for `content-reader@1` and `content-writer@1`. The bridge needs the
digest and size at `Begin`, so it serves a download whose request named them.

The file-store examples below are explicitly selected native provider APIs with
separate lifecycle guarantees. Their historical conformance describes that
deprecated in-process client. It does not qualify current service packages or
every platform.


**Deprecated in-process client, kept (no removal release is named).** The Go
client is the in-process download implementation, and the example below runs
as shown. The Python and C++
download fetchers were removed on 2026-09-15; the parent project's
`docs/REMOVED.md` records them.
No version number is typed on this page: a tag is the only thing that cannot
drift, so [the tag list](https://github.com/openabstractions/abstraction-download/tags)
is the answer to "which release".

An interface for downloading a file in which the caller does not choose who
performs the transfer: this process, a background service on the same machine,
or another machine on the network.

An application that downloads a large file normally runs the transfer inside
itself, so the transfer ends when the application does. Windows ships a service
that keeps transferring after the requesting program exits — the Background
Intelligent Transfer Service, BITS. Linux and macOS have neither it nor each
other's answer, and none of the three is reachable through one call an
application can make on all of them. This module is that call.
What was asked for, how far it got and who may work on it live in a record on
disk defined by [abstraction-job](https://github.com/openabstractions/abstraction-job),
so a transfer one process starts, another process — or another machine — can
finish.

One module of [Open Abstractions](https://github.com/openabstractions/abstractions),
the parent project, which holds the scope rules, the method, the measured results
and the conformance suite that judges implementations of this contract.

## Install

Install the runtime first: https://openabstractions.org/adopt.html

    go get github.com/openabstractions/abstraction-download/go

[Releases, newest first](https://github.com/openabstractions/abstraction-download/tags).
`go get` with no version takes the newest; pin the exact tag you tested against.
**Go 1.26 or later is required.**

Python and C++ applications use the generated request vocabulary under `py/`
and `cpp/` through the job service; see Requirements. The Python and C++
download fetchers were removed on 2026-09-15; the parent project's
`docs/REMOVED.md` records them.

Whether to adopt this at all, what it costs and what is not proven:
[Adopting](CONTRIBUTING.md#adopting).

## API

An application uses `Client`, from `download.Open()`.

| call | what it does |
|---|---|
| `Get(source, destination)` | fetch a URL to a path on this machine. Returns a handle immediately; the job may outlive the call and the process |
| `Submit(spec, requires...)` | `Get` for a caller with a digest to check against, several sources, or capabilities an implementation must have |
| `ResumeOrGet` / `ResumeOrSubmit` | continue the download already heading for that destination, or start one. For a program rerun from a shell with no job id to remember |
| `Deliver(ctx, id)` | block until the bytes are here, then take delivery |
| `Open(id)` | a handle to a download submitted earlier, possibly by another program |
| `Jobs()` | every download on this machine, as a live collection to bind a user interface to |
| `Where()` | who would perform a job submitted now |
| `TakeDelivery(id)` | the requester saying "I have it". A finished transfer is not collected until somebody does, and uncollected ones accumulate |

A handle adds `Wait(ctx)`, `Destination()` and `TakeDelivery()` to the job handle
it already is.

A **`Spec`** is what to fetch: an `Artifact` (a digest and a size, spelled
`<algorithm>:<encoded>` as OCI spells it), an ordered list of `Source` locators,
and a `Sink` naming the destination, where the bytes go. Its shape is
[Metalink's](https://www.rfc-editor.org/rfc/rfc5854), `priority` lower-first
included. Sources are typed locators rather than one URL, so a single job can
name a mirror, an SMB path and a NAS.

An implementer supplies one of two things. A **`Fetcher`** streams bytes through
this process and declares which schemes and capabilities it serves. A
**`Delegator`** hands the job to something that does the work under its own
account and returns a handle to it. Hashing, resume point, progress, lease
renewal and the final rename stay in the `Runner` either way — the part of this
module that claims a record and drives it to delivery — which is what lets one
implementation finish what another started.

Capabilities a caller may require: `resume`, `survives_process_exit`,
`verifies_content`, `delegates`.

Every normative rule is on [CONTRACT.md](CONTRACT.md) with its id, and each
conformance scenario cites the id it tests. This page states none.

## Status

Experimental. Go is the only language with a tagged release, and no release
carries an API stability promise.

- **No service is needed.** With an empty job-module file store and no
  runner process anywhere, `NewClient` succeeds and the transfer runs in
  the calling process. If that process dies mid-transfer, the record and the
  partial file are on disk, the lease lapses, and the next run or the next
  runner adopts them.
- **In-process fetching cannot survive process exit**, and does not claim to.
  Requiring `survives_process_exit` without
  [service-jobd](https://github.com/openabstractions/service-jobd) or a configured
  NAS delegate is refused, not silently downgraded.
- **Delegating to another machine needs a filesystem both machines can open.**
  The channel is a shared job record — a UNC path, a mapped drive, a mount — and
  no network protocol of ours. Over SMB a record has been read 154 s stale by the
  Windows redirector
  ([`SMB1.txt`](https://github.com/openabstractions/abstractions/blob/main/docs/results/SMB1.txt)).
- **Platforms.** Every published transcript was produced on Windows or Linux.
  macOS is `UNPROVEN` in all of them.

## Requirements

**Go** 1.26 or later, depending on this project's other modules and
`golang.org/x/sys`.

**C++** 17 and **Python** 3.9 or later for the generated request vocabulary,
standard library only. `cpp/CMakeLists.txt` installs the header-only
`abstraction_download_request` package:

    cmake -S cpp -B build -DCMAKE_INSTALL_PREFIX=<prefix>
    cmake --install build

and then, in yours:

    find_package(abstraction_download_request 0.1 CONFIG REQUIRED)
    target_link_libraries(your_target PRIVATE abstraction::download_request)

`cpp/test/request-package` is an outside consumer of that installed package.

## For provider implementers and the deprecated in-process client

This is the in-process path: no service, no facade, the Go package called
directly, through the deprecated in-process client named above.

```go
package main

import (
	"context"
	"fmt"
	"log"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
)

func main() {
	store, err := job.NewFileStore("jobs")
	if err != nil {
		log.Fatal(err)
	}
	dl := download.NewClient(download.DiscoverIn(store))
	fmt.Println("who would fetch this:", dl.Where())

	h, err := dl.Get("https://raw.githubusercontent.com/openabstractions/abstractions/main/LICENSE", "LICENSE.txt")
	if err == nil {
		_, err = h.Wait(context.Background())
	}
	if err != nil {
		log.Fatal(err)
	}
	path, _ := h.Destination()
	fmt.Println("bytes are at:", path)
}
```

`store` is the same `job.NewFileStore` [the job module's own provider
implementers section](https://github.com/openabstractions/abstraction-job#for-provider-implementers)
builds, and `DiscoverIn` builds a `Runner` over it that finds whichever
delegation hops this machine's configuration names. No service needs to be
running. On a machine with none configured, it prints `who would fetch this:
here` and the path the file landed at.
`Where` names who would take the next job — `here`, `bits`, `nas`, `nobody` — as
text for a status line, not a value to branch on. Here, unlike the shared
runtime path, `h.Destination()` names a real path the caller owns.

## Licence

Apache-2.0. See [LICENSE](LICENSE).

<!-- BEGIN GENERATED PERMISSION DEFINITIONS -->

## Permission definitions

Generated from the existing compiled catalogue. [RIGHTS.json](RIGHTS.json) contains the complete action/profile records, disclosure, lifecycle, first-use behavior, selector support and finite bundles.

This projection has no named action definition for Download. Delegated jobs and caller/service ownership retain their existing admission; job, credential and storage actions are listed under their defining layers.

<!-- END GENERATED PERMISSION DEFINITIONS -->
