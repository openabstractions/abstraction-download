# abstraction-download

Give an application a download that can keep running after the application
closes. The request says where the bytes may come from, how to verify them and
where the finished content belongs. A digest identifies the bytes once: a
finished record with that digest is copied rather than fetched again, whoever
requested it first. The runtime delegates the transfer to an allowed
downloader, keeps progress in one job table and reports a recoverable result.

A finished download is not readable through
[`abstraction-storage`](https://github.com/openabstractions/abstraction-storage)'s
`content-reader@1` by its digest: the runtime never copies a download's bytes
into the content root on completion. [The worked bridge](#reading-a-finished-download-through-storage)
below reads a finished download's bytes and writes them through
`content-writer@1`, the step that makes them readable by digest.

## Through the shared runtime

Applications resolve durable jobs through
[the facade](https://github.com/openabstractions/abstraction-facade), serialize
the generated download request as the job specification, and retain the receipt
and binding before waiting. A lost submit reply is reconciled with the same
request identity. Waiting cancellation leaves the accepted job with the service.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	facade "github.com/openabstractions/abstraction-facade/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	content "github.com/openabstractions/abstraction-storage/go/abstraction/storage/content"
	storage "github.com/openabstractions/abstraction-storage/go/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobs, err := facade.Discover().ResolveJobs(ctx, facade.Requirements{})
	if err != nil {
		log.Fatal(err)
	}
	window, err := jobs.GetHistoryWindow(ctx)
	if err != nil {
		log.Fatal(err)
	}

	digest := "sha256:2fc237e65e1f963310e9c961d8e71e932734a72f90e3216a972d83edd1feb756"
	size := int64(328597408)
	spec := request.Encode(&request.Request{
		Artifact: request.Artifact{
			Digest: digest,
			Size:   size,
		},
		Sources: []request.Source{
			{Scheme: "https", Locator: "https://example.invalid/artifact"},
		},
	})

	id := api.RequestIdentity{Key: "artifact-fetch", HistoryEpoch: window.HistoryEpoch}
	accepted, err := jobs.Submit(ctx, api.Submission{Identity: id, Kind: download.Kind, Spec: spec})
	if err != nil {
		log.Fatal(err)
	}
	if accepted.Outcome != api.AcceptanceOutcomeAccepted {
		log.Fatalf("submission not accepted: %s %s", accepted.Outcome, accepted.Reason)
	}
	// Keep id and jobs.Binding() so a later process can reconcile this job.
	binding := jobs.Binding()

	// A restarted process rebinds with the saved binding and reconciles the
	// same identity instead of resubmitting.
	restored, err := facade.Discover().RestoreJobs(ctx, binding)
	if err != nil {
		log.Fatal(err)
	}
	reconciled, err := restored.Reconcile(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("reconciled:", reconciled.Outcome)

	// A later run, possibly after a restart, learns whether it finished.
	observed, err := restored.ObserveWork(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	switch observed.Outcome {
	case api.ObservationOutcomeObserved:
		if observed.Snapshot.State != api.WorkStateComplete {
			fmt.Println("still working:", observed.Snapshot.State)
			return
		}
	case api.ObservationOutcomeUnknown:
		log.Fatal("absence: no record under this identity on this binding")
	case api.ObservationOutcomeForbidden:
		log.Fatal("not authorized to observe this job")
	default:
		log.Fatalf("observation refused: %s", observed.Outcome)
	}

	result, err := restored.ReadResult(ctx, id, 0, 65536)
	if err != nil {
		log.Fatal(err)
	}
	switch result.Outcome {
	case api.ResultOutcomeData:
		fmt.Println("bytes:", len(result.Chunk.Data), "eof:", result.Chunk.EOF)
	case api.ResultOutcomeNotReady:
		fmt.Println("not ready yet")
	case api.ResultOutcomeForbidden:
		log.Fatal("not authorized to read this result")
	default:
		log.Fatalf("result unavailable: %s", result.Outcome)
	}

	// The bridge into abstraction-storage: page the finished download's
	// bytes out of ReadResult and write them through content-writer@1.
	writer, err := facade.Discover().ResolveStorageWriter(ctx, facade.Requirements{})
	if err != nil {
		log.Fatal(err)
	}
	writeID, err := storage.NewRequestID() // retain before writing
	if err != nil {
		log.Fatal(err)
	}
	begun, err := writer.Begin(ctx, writeID, digest, size)
	if err != nil {
		log.Fatal(err)
	}
	switch begun.Outcome {
	case content.BeginOutcomeStarted:
		upload := *begun.Upload
		for offset := upload.Received; ; {
			chunk, err := restored.ReadResult(ctx, id, offset, 65536)
			if err != nil {
				log.Fatal(err)
			}
			if chunk.Outcome != api.ResultOutcomeData {
				log.Fatalf("result unavailable: %s", chunk.Outcome)
			}
			appended, err := writer.Append(ctx, upload, offset, chunk.Chunk.Data)
			if err != nil {
				log.Fatal(err)
			}
			upload.Received = appended.Received
			offset = appended.Received
			if chunk.Chunk.EOF {
				break
			}
		}
		if _, err := writer.Commit(ctx, upload); err != nil {
			log.Fatal(err)
		}
	case content.BeginOutcomeCommitted, content.BeginOutcomePresent:
		// Another writer already stored these bytes under this digest.
	default:
		log.Fatalf("begin refused: %s", begun.Outcome)
	}

	reader, err := facade.Discover().ResolveStorage(ctx, facade.Requirements{})
	if err != nil {
		log.Fatal(err)
	}
	opened, err := reader.Open(ctx, digest)
	if err != nil {
		log.Fatal(err)
	}
	if opened.Outcome != content.OpenOutcomeOpened {
		log.Fatalf("storage refused open: %s", opened.Outcome)
	}
	fmt.Println("readable by digest:", digest)
}
```

The download's own result document is its downloaded bytes (contract rule
DL-R36). `ReadResult` pages through the fetched artifact itself: a caller
reads it in `65536`-byte chunks until `EOF`, exactly as the block above reads
its first chunk.

`ResolveJobs` binds a `JobsClient` to the service, and `GetHistoryWindow` supplies
the epoch a `RequestIdentity` needs. `request.Encode` serializes the generated
download request into the job's opaque `Spec` bytes; `Submit` carries
`download.Kind` so the service routes the job to the download service. The
receipt inside `accepted` and `jobs.Binding()` are what a second
process — run later, or after a restart — uses to find this same job again:
`ObserveWork` (or, for a caller that lists everything it owns rather than
remembering one identity, `ListWork`) reads its state without changing it.
`ReadResult` answers `not_ready` before the bytes are in, `forbidden` when the
caller may not read them, and the observation call answers `unknown` — absence
— when the service holds no record under that identity at all: the outcomes an
application branches on, rather than assuming success. `RestoreJobs` and
`Reconcile`, above, are the restart step; [the job module's own
example](https://github.com/openabstractions/abstraction-job#an-example-that-runs)
carries the full recovery pattern, including retry after a failed attempt.

The shipped command follows that path:

```console
openabstractions download https://example.invalid/artifact --out /destination
openabstractions jobs list
```

Use `--sha256` when the publisher supplies a digest. Credentials are registered
by name through `openabstractions credentials`; URLs should carry no secrets.

### Reading a finished download through storage

The content root is a fixed directory the runtime keeps under its own state
directory (`%LOCALAPPDATA%\openabstractions\runtime-v1` on Windows,
`~/.local/share/openabstractions/runtime-v1` on Linux,
`~/Library/Application Support/openabstractions/runtime-v1` on macOS, or the
`--state-dir` an isolated runtime was given), populated only by
`content-writer@1` commits. A job-service
download's destination is always its own result document (contract rule DL-R36,
above), a location the runtime owns, never that content root, and nothing
copies one into the other on completion. An application has no destination
path to open here: `ReadResult` is the only way to reach the bytes. To make
them readable by digest through
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
upload, on its own evidence. The bridge is the second half of [the example
above](#through-the-shared-runtime), from `ResolveStorageWriter` to the final
`reader.Open`: `storage` there is
`github.com/openabstractions/abstraction-storage/go/client` and `content` is
`github.com/openabstractions/abstraction-storage/go/abstraction/storage/content`,
the same package [the storage README](https://github.com/openabstractions/abstraction-storage#reading-content)
imports for `content-reader@1` and `content-writer@1`.

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
