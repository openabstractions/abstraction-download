# abstraction-download

A Go and Python library that fetches a file described by a job record, checks it against a
digest, and saves enough progress that another process can finish a transfer this one started.

## The problem it solves

An application that downloads a large file usually keeps the transfer inside its own process.
When the process exits the transfer is gone, and the partial file left behind carries no record
of how many of its bytes were ever checked, so resuming means starting again or appending onto
bytes nobody verified. This library keeps the file's identity, the progress made and who may
work on it in a record on disk, apart from whatever moves the bytes, so a transfer interrupted
by a crash, a sleep or a reboot can be finished by another process or machine.

## Terms

A layer on top of [abstraction-job](https://github.com/openabstractions/abstraction-job), whose
vocabulary it uses throughout.

| term | meaning |
|---|---|
| **job** | a record on disk describing one unit of work: an id, a state, a `kind`. This library handles jobs of kind `download` and ignores the rest |
| **store** | the directory those records live in. Any process that can read the directory can read the jobs |
| **spec** | the body of a job, which the job layer does not interpret. A download spec is an `Artifact` (a **digest**, written `sha256:<hex>`, and a size), a list of `Source`s, and a `Sink`. The digest comes from the caller, and bytes that do not match it are refused |
| **lease** | the time-limited right to work on a job. One holder at a time |
| **epoch** | a counter on the lease, raised at each claim. Every write presents the epoch it holds, so a process whose lease expired while it was asleep has its later writes refused |
| **checkpoint** | the resume point: how many leading bytes of the partial file are known to be the artifact's real bytes |
| **orphan** | a job whose lease expired with the work unfinished, because the process holding it died |
| **delegation** | handing a job to a system that keeps working after this process exits, recorded in the job as `{system, external_id}` |
| **supervisor** | a process that watches a store and finishes work nobody is doing: it adopts orphans and checks up on delegated jobs. `jobd` here is one |
| **tier** | a registered delegation target (a Windows service, a NAS) with a priority. The caller does not name a tier; whatever is registered and reachable is offered the job |
| **binding** | an implementation of `Fetcher`, which streams bytes through this process, or of `Delegator`, which hands the transfer to something else and gets back a handle |

## Install

The Go module lives in `go/`, so its path ends in `/go`, not the repository URL. `go get` also
fetches `abstraction-job/go`, `abstraction-config/go` and `abstraction-storage/go` at `v0.1.0`.

```
go get github.com/openabstractions/abstraction-download/go@v0.1.0
```

Python has no package index release. Clone this repository and the job repository side by side
and put both `python/` directories on `PYTHONPATH`, separated by `;` on Windows:

```
git clone https://github.com/openabstractions/abstraction-download.git
git clone https://github.com/openabstractions/abstraction-job.git
export PYTHONPATH="$PWD/abstraction-download/python:$PWD/abstraction-job/python"
```

## A minimal example

Each copies a local file through the library, checks its digest, and prints what was delivered.

```go
package main

import (
	"context"
	"fmt"
	"os"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
)

func check(err error) { if err != nil { panic(err) } }

func main() {
	payload := []byte("hello from the download abstraction\n")
	// The sha256 of payload, which a caller is expected to know in advance.
	digest := "sha256:59285083328ad8b69e30122940bddd0647570afa24b77532ac69f8d1ce6abfcc"
	check(os.WriteFile("source.bin", payload, 0o644))
	store, err := job.NewFileStore("store")
	check(err)
	id, err := download.Submit(store, download.Spec{
		Artifact: download.Artifact{Digest: digest, Size: int64(len(payload))},
		Sources:  []download.Source{{Scheme: "file", Locator: "source.bin"}},
		Sink:     download.Sink{Final: "out/hello.bin"}, // relative to the store
	})
	check(err)
	runner := download.NewRunner(store, "example")
	check(runner.Run(context.Background(), id))
	check(runner.TakeDelivery(id))
	got, err := os.ReadFile("store/out/hello.bin")
	check(err)
	fmt.Printf("delivered %d bytes: %s", len(got), got) // delivered 36 bytes: hello …
}
```

```python
import hashlib

from abstraction_job import FileStore
import abstraction_download as dl

payload = b"hello from the download abstraction\n"
with open("source.bin", "wb") as f:
    f.write(payload)
digest = "sha256:" + hashlib.sha256(payload).hexdigest()
store = FileStore("store")
job_id = dl.submit(store, dl.Spec(
    artifact=dl.Artifact(digest=digest, size=len(payload)),
    sources=[dl.Source(scheme="file", locator="source.bin")],
    sink=dl.Sink(final="out/hello.bin"),  # relative to the store
))
runner = dl.Runner(store, "example")
runner.run(job_id)
runner.take_delivery(job_id)
with open("store/out/hello.bin", "rb") as f:
    print("delivered:", f.read().decode(), end="")
```

## API overview

**Go, package `download`.** Describing work: `Kind`, `Spec`, `Artifact`, `Source`, `Sink`,
`Checkpoint`, `Spec.Validate`, `Submit`, `SpecOf`, `CheckpointOf`, `Sink.Resolve`, `Portable`,
`NormalDigest`. Doing work: `NewRunner`; on `*Runner`: `Run`, `Adopt`, `TakeDelivery`,
`TakeDeliveryAll`, `Delegate`, `DelegateAll`, `Reconcile`, `ReconcileAll`, `Tier`, `Handoff`.
Transports: `Fetcher`, `Capability` (`CapResume`, `CapSurvivesProcessExit`, `CapVerifies`,
`CapDelegates`), `Registry`, `NewRegistry`, `DefaultRegistry`, `HTTP`, `File`, `Delegator`,
`Delegators`, `NewDelegators`, `Status`, `DelegateState`, `Selective`, `Suspendable`,
`ReportingFinalizer`. Wiring: `Discover`, `DiscoverIn`, `Tier`, `RegisterTier`,
`RegisteredTiers`, `Owner`, `Program`, `Service`, `NewService`, `Open`, `WithStorage`,
`Credentials`, `EnvCredentials`, `CredentialAttr`, `Supervisor`, `Heartbeat`, `StopHeartbeat`,
`SupervisorOf`, `Nudge`, `ListenForNudges`, `LocalSink`. Resuming: `Resume`, `ResumeOrSubmit`,
`ResumeOrGet`, `Continuation`, `Disposition` (`Submitted`, `Resumed`, `Delivered`, `Busy`,
`Paused`). Subpackages: `go/bits` (Windows BITS), `go/nas` (a supervisor over a share), `go/all`
(blank-import both).

**Python, module `abstraction_download`:** `KIND`, `Spec`, `Artifact`, `Source`, `Sink`,
`Checkpoint`, `submit`, `spec_of`, `checkpoint_of`, `portable`, `local_root`, `local_sink`,
`store_root`, `owner`, `credentials`, `Supervisor`, `supervisor_of`, `Runner` (`run`, `adopt`,
`take_delivery`), and the errors `DownloadError`, `DigestMismatch`, `ShortTransfer`,
`RangeIgnored`, `NoSource`.

Unusual semantics, in both languages unless said otherwise:

- A `Source` is a `{Scheme, Locator}` pair, not a URL; `file` and `smb` locators are filesystem
  paths. Relative sink paths resolve against the store root on each machine, and a path
  absolute under either Windows or POSIX rules is left as written.
- `Run` leaves a finished job in state `transferred`; `TakeDelivery` is the requester saying it
  has the bytes, and completes it. `Delegate` releases the lease before returning, so another
  process can poll or finalise the job, and `Reconcile` catches up later.
- Resuming starts at the smaller of the checkpoint and the size of the partial file; anything
  past that is discarded and the hash rebuilt over what is kept. A `200` answer to a `Range`
  request is an error, not a restart, and a digest mismatch deletes the partial file and
  records the reason in the job. In Go only, `ResumeOrSubmit` and `ResumeOrGet` key work on the
  destination, not a job id: one record per path, continued, a `complete` one reused only while
  its file is there, no lease taken from another owner, and `Continuation` says which.

**Commands**, built from `go/`. `go build ./cmd/dl` gives `dl <url>`, plus `dl list`, `dl watch`
and `dl tiers`. `go build ./cmd/jobd` gives the supervisor: `jobd once` makes one pass, `jobd
run` supervises in the foreground, `jobd status` reports, and `jobd install` prints `schtasks`
commands without running them. `go run ./cmd/specread <spec.json>` prints how this reads a spec.
`ABSTRACTION_STORE` selects the job store (default `~/.abstraction`), `MODELGET_STORE` is a
legacy alias, and `ABSTRACTION_NAS_STORE` names one on a share a supervisor elsewhere watches.

## The `cpp/` directory

`cpp/specread.cpp` parses a download spec and prints what it read. **There is no C++
implementation of this library** — no fetcher, runner, delegator or supervisor. It exists so
the Go, Python and C++ readings of one spec can be compared, using the samples in
`testdata/specs/`. No build file for it ships here; it includes `abstraction/job/record.h`, so
it needs the job repository's `cpp/include` and `nlohmann/json` on the include path.

## Status

Experimental, at `v0.1.0`. Interfaces and the on-disk spec may change.

- **Go** is the complete implementation: the runner, the HTTP and file/SMB fetchers,
  delegation, the BITS and NAS delegators, and `jobd`.
- **Python** is partial. Its `Runner` runs, adopts and takes delivery over `http`, `https`,
  `file` and `smb`, but there is no delegation, reconcile, supervisor loop, tier registry or
  `ResumeOrSubmit`, and `adopt` skips delegated jobs. A Python process can neither hand work to
  BITS or a NAS nor recover work handed there. **C++** reads specs only, as above.
- The `bits` binding runs `powershell.exe` with the `BitsTransfer` module, so it works on
  Windows only; its tests skip when BITS cannot be driven, and the `nas` tests that reach a
  real NAS skip unless `ABSTRACTION_LIVE=1` and `ABSTRACTION_NAS_STORE` are set. Untested
  altogether: interrupting a real multi-gigabyte transfer, and anything at NAS or BITS scale.

## Tests

```
cd go && go build ./... && go test ./...
cd ../python && python -m unittest discover
```

76 Go test functions across 17 files (the `bits` package takes about a minute) and 32 Python
tests, which find the job layer through `PYTHONPATH` or a sibling `abstraction-job` checkout.

## Requirements

Go 1.26.0 or newer; Python 3.12, no dependencies. Windows, Linux, macOS; `bits` needs Windows.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
