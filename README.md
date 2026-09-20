# abstraction-download

Give an application a download that can keep running after the application
closes. The request says where the bytes may come from, how to verify them and
where the finished content belongs. The runtime chooses an allowed downloader,
keeps progress and reports a recoverable result.

## Application service path

Applications resolve durable jobs through
[the facade](https://github.com/openabstractions/abstraction-facade), serialize
the generated download request as the job specification, and retain the receipt
and binding before waiting. A lost submit reply is reconciled with the same
request identity. Waiting cancellation leaves accepted work with the service.

The shipped command follows that path:

```console
openabstractions download https://example.invalid/artifact --out /destination
openabstractions jobs list
```

Use `--sha256` when the publisher supplies a digest. Credentials are registered
by name through `openabstractions credentials`; URLs should carry no secrets.

The file-store examples below are explicitly selected native provider APIs with
separate lifecycle guarantees. Their historical conformance describes that
provider profile. It does not qualify current service packages or every platform.


**Legacy provider profile.** Recorded cross-language conformance passes and the example below runs as shown.
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
application can make on all of them. This layer is that call.
What was asked for, how far it got and who may work on it live in a record on
disk defined by [abstraction-job](https://github.com/openabstractions/abstraction-job),
so a transfer one process starts, another process — or another machine — can
finish.

One layer of [Open Abstractions](https://github.com/openabstractions/abstractions),
the parent project, which holds the scope rules, the method, the measured results
and the conformance suite that judges implementations of this contract.

## Install

    go get github.com/openabstractions/abstraction-download/go

[Releases, newest first](https://github.com/openabstractions/abstraction-download/tags).
`go get` with no version takes the newest; pin the exact tag you tested against.
**Go 1.26 or later is required.**

Python and C++ applications use the generated request vocabulary under `py/`
and `cpp/` through the job service; see Requirements. The Python and C++
download providers were removed on 2026-09-15; the parent project's
`docs/REMOVED.md` records them.

Whether to adopt this at all, what it costs and what is not proven:
[Adopting](CONTRIBUTING.md#adopting).

## An example that runs

```go
package main

import (
	"context"
	"fmt"
	"log"

	download "github.com/openabstractions/abstraction-download/go"
)

func main() {
	dl, _, err := download.Open()
	if err != nil {
		log.Fatal(err)
	}
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

Nothing needs configuring and no service needs to be running. On a machine with
neither, it prints `who would fetch this: here` and the path the file landed at.
`Where` names who would take the next job — `here`, `bits`, `nas`, `nobody` — as
text for a status line, not a value to branch on.

## API

An application uses `Client`, from `download.Open()`.

| call | what it does |
|---|---|
| `Get(source, destination)` | fetch a URL to a path on this machine. Returns a handle immediately; the work may outlive the call and the process |
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
and a `Sink` naming where the bytes go. Its shape is
[Metalink's](https://www.rfc-editor.org/rfc/rfc5854), `priority` lower-first
included. Sources are typed locators rather than one URL, so a single job can
name a mirror, an SMB path and a NAS.

An implementer supplies one of two things. A **`Fetcher`** streams bytes through
this process and declares which schemes and capabilities it serves. A
**`Delegator`** hands the job to something that does the work under its own
account and returns a handle to it. Hashing, resume point, progress, lease
renewal and the final rename stay in the `Runner` either way — the part of this
layer that claims a record and drives it to delivery — which is what lets one
implementation finish what another started.

Capabilities a caller may require: `resume`, `survives_process_exit`,
`verifies_content`, `delegates`.

Every normative rule is on [CONTRACT.md](CONTRACT.md), tagged, and each
conformance scenario cites the tag it tests. No rule is stated on this page.

## Status

Experimental. Go is the only language with a tagged release, and no release
carries an API stability promise.

- **No service is needed.** With an empty store and no supervisor process
  anywhere, `Open` succeeds and the transfer runs in the calling process. If that
  process dies mid-transfer, the record and the partial file are on disk, the
  lease lapses, and the next run or the next supervisor adopts them.
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

**Go** 1.26 or later, depending on this project's other layers and
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

## Licence

Apache-2.0. See [LICENSE](LICENSE).
