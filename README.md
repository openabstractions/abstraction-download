# abstraction-download

**Ready.** Cross-language conformance passes and the example below runs as shown.
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

**Python** is in this repository and on no package index —
[what to install, import and call](python/README.md). **C++** is here too, with
no tagged release; see Requirements, and read *What may break* before depending
on either.

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
- **On Linux the C++ build registers no https fetcher**, because the platform
  furnishes none this layer will use. An application whose whole job is pulling
  bytes off https gets nothing there. See Requirements.
- **`watch`, which the C++ build compiles in, carries no conformance verdict in
  any language.** [What is proven and what is not](https://openabstractions.org/coverage.html).
- **Platforms.** Every published transcript was produced on Windows or Linux.
  macOS is `UNPROVEN` in all of them.

## Requirements

**Go** 1.26 or later, depending on this project's other layers and
`golang.org/x/sys`. **Python** 3.9 or later, standard library only, on no package
index — the install line, the example and the API are on
[`python/README.md`](python/README.md).

**C++** 17, standard library only, no third-party dependency.
`cpp/CMakeLists.txt` builds it, runs its tests and installs a `find_package`
package:

    cmake -S cpp -B build -DCMAKE_INSTALL_PREFIX=<prefix>
    cmake --build build && ctest --test-dir build
    cmake --install build

and then, in yours:

    find_package(abstraction_download 0.1 CONFIG REQUIRED)
    target_link_libraries(your_target PRIVATE abstraction::download_runner)

That `0.1` is `project(abstraction_download VERSION 0.1.0)` in
`cpp/CMakeLists.txt` and corresponds to no tag: **there is no C++ release.** What
you can pin is a commit. `abstraction::download` alone is the header-only reader:
the record rules without the runner. `add_subdirectory(cpp)` gives the same two
names, so vendoring and installing are interchangeable at the call site.

`runner.h` and `wanted.h` include `<abstraction/job/store.h>`, and the job
sources that satisfies reach `cas` and `watch` in turn, so that build needs
[`abstraction-job`](https://github.com/openabstractions/abstraction-job),
[`abstraction-cas`](https://github.com/openabstractions/abstraction-cas) and
[`abstraction-watch`](https://github.com/openabstractions/abstraction-watch):
either installed already and on `CMAKE_PREFIX_PATH`, or cloned beside this
repository, in which case they are compiled in and travel in this package.
Discovery also requires the shared client byte runtime from
[`abstraction-identity`](https://github.com/openabstractions/abstraction-identity):
install its `cpp` CMake package `abstraction_ipc` on `CMAKE_PREFIX_PATH`, or
clone it beside this repository. Discovery links its `abstraction::ipc` target;
this dependency supplies client transport, not a C++ identity service.

Nothing is fetched while CMake configures — a build that reaches the network is
a dependency you did not choose, and handing you one would be the thing this
layer exists to stop.

**Which commits of those three go together** is
[`layers.lock`](https://github.com/openabstractions/abstractions/blob/main/layers.lock),
and it has a row for `abstraction-job` and none for `abstraction-cas` or
`abstraction-watch`. Until it does, the set a C++ build needs is not published
anywhere and you are choosing three commits yourself.

Without a build system, clone those three beside this repository and name the
files. `runner.h` refuses to compile unless the runner is linked, so the define
below is what the `abstraction::download_runner` target would have set for you:

    g++ -std=c++17 -DABSTRACTION_DOWNLOAD_RUNNER_LINKED \
      -I cpp/include -I ../abstraction-job/cpp/include \
      -I ../abstraction-cas/cpp/include -I ../abstraction-watch/cpp/include \
      cpp/test/test_runner.cpp cpp/src/runner.cpp cpp/src/spec.cpp \
      cpp/src/wanted.cpp cpp/src/sha256.cpp cpp/src/fetchers.cpp \
      ../abstraction-job/cpp/src/json.cpp ../abstraction-job/cpp/src/record.cpp \
      ../abstraction-job/cpp/src/ranges.cpp ../abstraction-job/cpp/src/store.cpp \
      ../abstraction-job/cpp/src/awake.cpp ../abstraction-cas/cpp/src/cas.cpp \
      -o test_runner

That is this layer's own test, so running it is how you check that your compiler
agrees with ours. Measured 2026-09-09 with g++ 15.2 and with MSVC 19.51, which
takes the same file list under `/std:c++17` and `/I` and needs `winhttp.lib` at
link. `fetchers.cpp` compiles on Linux and registers no https fetcher there,
because the platform furnishes none this layer will use — see
`https_available()` in `cpp/include/abstraction/download/fetcher.h`.

`python/` and `cpp/` are full implementations, not readers. The timestamps a C++
record may carry are limited by the build's `std::chrono::system_clock` — 1677 to
2262 on libstdc++, wider on MSVC — and an instant outside that is refused as
`bad_timestamp`, never wrapped. All three produce identical transcripts over the
shared scenario corpus
([`BEHAVIOUR1.txt`](https://github.com/openabstractions/abstractions/blob/main/docs/results/BEHAVIOUR1.txt)).

## Licence

Apache-2.0. See [LICENSE](LICENSE).
