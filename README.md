# abstraction-download

A Go and Python library that fetches a file described by a job record, checks it against a
digest, and saves enough progress that another process can finish a transfer this one started.

## The problem it solves

An application that downloads a large file usually keeps the transfer inside its own process. When
the process exits the transfer is gone, and the partial file left behind carries no record of which
of its bytes were ever checked, so resuming means starting again or writing onto bytes nobody
verified. This library keeps the file's identity, its progress and who may work on it in a record
on disk, so another process or machine can finish what a crash interrupted.

## Terms

From [abstraction-job](https://github.com/openabstractions/abstraction-job), the layer below.

| term | meaning |
|---|---|
| **job** | a record on disk describing one unit of work: an id, a state, a `kind`. This library handles jobs of kind `download` and ignores the rest |
| **store** | the directory those records live in. Any process that can read the directory can read the jobs |
| **spec** | the body of a job, which the job layer does not interpret. A download spec is an `Artifact` (a **digest**, written `sha256:<hex>`, and a size), a list of `Source`s, and a `Sink`. The digest comes from the caller, and bytes that do not match it are refused |
| **lease** | the time-limited right to work on a job, one holder at a time. Each claim raises the lease's **epoch**, and every write presents the epoch it holds, so a process whose lease expired while it was asleep has its later writes refused. An **orphan** is a job whose lease expired with the work unfinished, because the process holding it died |
| **checkpoint** | the resume point: which bytes of the partial file are known to be the artifact's real bytes, and the validators those bytes came from. It holds a set of **proven ranges** — half-open intervals `[start, end)`, sorted and merged, under `verified` — plus `verified_prefix`, the end of the range starting at zero, or 0. A reader that knows only `verified_prefix` resumes from it and re-fetches the rest. `verified` and the advisory content model `abstraction.download/ranges@1` appear only when the set has a hole; a set one prefix describes is written as `verified_prefix` alone. The job layer owns the encoding; the design is [docs/checkpoint-ranges.md](https://github.com/openabstractions/abstractions/blob/main/docs/checkpoint-ranges.md) |
| **validator** | a token identifying the version of the artifact a source served: an HTTP `ETag`, or `Last-Modified` when there is no strong one — a weak ETag (`W/"..."`) is neither stored nor sent. A resume returns it as `If-Range`, so a source whose file has changed answers with the whole new file rather than a range of it. Both implementations do this, on `http` and `https`; those requests also ask for `Accept-Encoding: identity`, since a byte offset is counted after decoding here and before it at the server |
| **delegation** | handing a job to a system that keeps working after this process exits, recorded in the job as `{system, external_id}`. A **tier** is a registered delegation target (a Windows service, a NAS) with a priority; the caller does not name one, and whatever is registered and reachable is offered the job |
| **supervisor** | a process that watches a store and finishes work nobody is doing: it adopts orphans and checks up on delegated jobs. [service-jobd](https://github.com/openabstractions/service-jobd) is one |
| **client** | what an application holds: it submits work and waits for delivery, and never learns who moved the bytes. If a supervisor is watching the store it takes the work and the application may exit; if not, the client's own process does it |
| **binding** | an implementation of `Fetcher`, which streams bytes through this process, or of `Delegator`, which hands the transfer to something else and gets back a handle |

## Install

The Go module lives in `go/`, so its path ends in `/go`, not the repository URL; `go get` also
fetches the layers it depends on, at the same version. Python has no package index release: clone
this repository, the job repository and the watch repository side by side and put the three
`python/` directories on `PYTHONPATH`, separated by `;` on Windows.

```
go get github.com/openabstractions/abstraction-download/go@v0.2.0
git clone https://github.com/openabstractions/abstraction-download.git
git clone https://github.com/openabstractions/abstraction-job.git
git clone https://github.com/openabstractions/abstraction-watch.git
export PYTHONPATH="$PWD/abstraction-download/python:$PWD/abstraction-job/python:$PWD/abstraction-watch/python"
```

## A minimal example

Each copies a local file through the library, checks its digest, and prints where it was
delivered. The application holds a client and nothing else: no store, no runner, no path. Jobs go
to the machine's store, `~/.abstraction` unless `ABSTRACTION_STORE` says otherwise, and a
supervisor watching that store would do the fetching instead of this process, unchanged.

```go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	download "github.com/openabstractions/abstraction-download/go"
)

func check(err error) { if err != nil { panic(err) } }

func main() {
	payload := []byte("hello from the download abstraction\n")
	digest := "sha256:59285083328ad8b69e30122940bddd0647570afa24b77532ac69f8d1ce6abfcc" // of payload
	check(os.WriteFile("source.bin", payload, 0o644))
	source, _ := filepath.Abs("source.bin")
	downloads, _, err := download.Open()
	check(err)
	h, err := downloads.Submit(download.Spec{
		Artifact: download.Artifact{Digest: digest, Size: int64(len(payload))},
		Sources:  []download.Source{{Scheme: "file", Locator: source}},
		Sink:     download.Sink{Final: "out/hello.bin"}, // relative to the store
	})
	check(err)
	fmt.Println("submitted", h.ID(), "to", downloads.Where())
	_, err = h.Wait(context.Background())
	check(err)
	path, _ := h.Destination()
	got, _ := os.ReadFile(path)
	fmt.Printf("delivered %d bytes to %s: %s", len(got), path, got) // delivered 36 bytes to …
}
```
```python
import hashlib
import os
import abstraction_download as dl

payload = b"hello from the download abstraction\n"
open("source.bin", "wb").write(payload)
digest = "sha256:" + hashlib.sha256(payload).hexdigest()
downloads = dl.discover()
job_id = downloads.submit(dl.Spec(
    artifact=dl.Artifact(digest=digest, size=len(payload)),
    sources=[dl.Source(scheme="file", locator=os.path.abspath("source.bin"))],
    sink=dl.Sink(final="out/hello.bin"),  # relative to the store
))
print("submitted", job_id, "to", downloads.where())
rec = downloads.deliver(job_id)
_, path = dl.local_sink(downloads.store, rec.id, dl.spec_of(rec).sink)
print("delivered to", path + ":", open(path, "rb").read().decode(), end="")
```

`Get(url, "models/")` is `Submit` for a caller that has only a URL and a directory. A program
that also uses jobs, storage and logging takes its client from
[abstraction-facade](https://github.com/openabstractions/abstraction-facade) instead of calling
`Open`; the client is the same.

## API overview

**Go, package `download`.** What an application uses: `Open` returns a `Client`; on it, `Get`,
`Submit`, `ResumeOrGet`, `ResumeOrSubmit`, `Open(id)`, `Jobs`, `Where`, `Deliver`, `TakeDelivery`.
Each submission returns a `Handle`: `ID`, `Record`, `Cancel`, `Destination`, `Wait`,
`TakeDelivery`, and `Pause`/`Resume` where the store supports them. `ResumeOrSubmit` reports an
`Outcome` whose `Decision` is one of `Submitted`, `Resumed`, `Delivered`, `Busy`, `Paused`.
`NewClient`, `Option`, `Options` and `WithStorage` build a client by hand.

Describing work: `Kind`, `Spec`, `Artifact`, `Source`, `Sink`, `Checkpoint` (`Proven`), `Range` and
`Ranges` (the job layer's), `Validators`, `StrongValidators`, `Spec.Validate`, `SpecOf`,
`CheckpointOf`, `Sink.Resolve`, `Portable`, `NormalDigest`, `PartialFor`. Errors: `ErrNoFetcher`,
`ErrNoDelegator`, `ErrNotDelegated`, `ErrStrandedHere`, `ErrDigestMismatch`, `ErrShortTransfer`,
`ErrRefused`, `ErrCannotRestart`, `ErrOverrun`, `ErrStalled`, `ErrEscapesRoot`, `ErrForeignPath`,
`ErrReservedPath`, `ErrNoLocalArea`, `ErrNoTrustStore`; `Permanent(err)` says whether retrying can
help.

The layer beneath the client, which a supervisor or a test uses and an application does not:
`Runner` (`NewRunner`, `Discover`, `DiscoverIn`; `Run`, `Adopt`, `TakeDelivery`, `TakeDeliveryAll`,
`Delegate`, `DelegateAll`, `Reconcile`, `ReconcileAll`, `Tier`), `Submit(store, spec)`, and the
bindings: `Fetcher`, `RangeFetcher`, `Fetchers` (`NewFetchers`, `DefaultFetchers`), `HTTP`, `File`,
`Capability` (`CapResume`, `CapSurvivesProcessExit`, `CapVerifies`, `CapDelegates`), `Delegator`,
`Delegators`, `NewDelegators`, `Status`, `DelegateState`, `Selective`, `Suspendable`,
`ReportingFinalizer`, `Tier`, `RegisterTier`, `RegisteredTiers`, `Owner`, `Program`, `Credentials`,
`EnvCredentials`, `CredentialAttr`, `Supervisor`, `Heartbeat`, `StopHeartbeat`, `SupervisorOf`,
`Nudge`, `ListenForNudges`, `LocalSink`, `RetryAfter`. Subpackages `go/bits` (Windows BITS),
`go/nas` (a share a supervisor elsewhere watches), `go/all` (both); a blank import registers the
tier, and `Open` offers it work. Application code that holds a `Runner` and calls
`Submit(store, …)` or `Delegate` itself is doing the client's job by hand; those names stay for
supervisors and are not going away.

**Python, `abstraction_download`:** `discover()` returns a `Client` with `get`, `submit`, `open`,
`jobs`, `where`, `deliver`, `take_delivery`. Describing work: `KIND`, `Spec`, `Artifact`, `Source`,
`Sink`, `Checkpoint`, `Validators` (`empty`, `if_range`), `strong_validators`, `spec_of`,
`checkpoint_of`, `portable`, `local_root`, `local_sink`, `store_root`, `owner`, `credentials`.
Beneath the client: `submit(store, spec)`, `Runner` (`run`, `adopt`, `take_delivery`),
`Supervisor`, `supervisor_of`, `resume_or_submit`, `resume_or_get`, `Continuation`, `SUBMITTED`,
`RESUMED`, `DELIVERED`, `BUSY`, `PAUSED`. Errors: `DownloadError`, `Permanent`, `DigestMismatch`,
`ShortTransfer`, `NoSource`, `RefusedBySource`, `EscapesRoot`, `ReservedPath`, `ForeignPathError`,
and `Asked`, which is a pause or cancel somebody requested, not a failure.

Unusual semantics, in both languages unless said otherwise:

- A `Source` is a `{Scheme, Locator}` pair, not a URL; `file` and `smb` locators are filesystem
  paths. A relative sink path resolves against each machine's store root; an absolute one does not,
  and is never offered to a supervisor on another machine.
- A finished transfer is `transferred` until the requester takes delivery. `Wait` and `Deliver` in
  Go and `deliver` in Python wait for the bytes and take delivery; `TakeDelivery` and
  `take_delivery` alone are for a caller that watched the collection itself. Asking twice for the
  same source and destination while the first is in flight returns the first job.
- Resuming asks for the gaps between the checkpoint's proven ranges — Go for every gap, Python from
  the prefix onwards — and never reads the partial's length as progress: a partial too short to hold
  what its checkpoint claims loses the claim, never the other way round. Each gap is written where
  it belongs, as `Range: bytes=<start>-<end>` where proven bytes follow and `Range: bytes=<start>-`
  where none do. The digest is hashed on the way in for one stream and read back from the finished
  file otherwise; a mismatch deletes the partial. A checkpoint also records which VERSION its bytes
  came from, sent back as `If-Range`: a strong `ETag`, else a `Last-Modified` parsing as an HTTP
  date; a weak `ETag` (`W/"..."`) is neither stored nor sent. A `200` answering a range is a whole
  artifact, not an error: offset, partial and hash reset to zero and any remaining gaps are
  abandoned, and a `206` starting elsewhere or a `416` is re-requested whole.
- `ResumeOrSubmit` and `ResumeOrGet` (`resume_or_submit`, `resume_or_get` in Python) key work on the
  destination rather than on a job id, for a command re-run from a shell that has no id to remember:
  one record per destination whatever source is asked for, a `complete` one reused only while its
  file is still there, `failed` and `cancelled` history, another owner's lease untouched, the resume
  point measured against the partial file, and concurrent callers given one record. The report says
  which of those happened, whether the source differs, how many bytes are discarded, and — in Go —
  `ResumeFrom` and `Proven`, which differ when proven ranges sit past the first gap. A destination
  is a path made absolute, cleaned and symlink-resolved, and no further: hardlinks, two mounts of
  one filesystem and case-folding volumes off Windows defeat it.

**Commands**, built from `go/`. `go build ./cmd/dl` gives `dl <url>`, plus `dl list`, `dl watch` and
`dl tiers`; `go run ./cmd/specread <spec.json>` prints how this reads a spec. The supervisor is
[service-jobd](https://github.com/openabstractions/service-jobd):
`go install github.com/openabstractions/service-jobd@v0.2.0`, then `jobd once` makes one pass,
`jobd run` supervises in the foreground, `jobd status` reports, and `jobd install` prints
`schtasks` commands without running them. `ABSTRACTION_STORE` selects the job store (default
`~/.abstraction`, legacy alias `MODELGET_STORE`), `ABSTRACTION_NAS_STORE` one on a share a
supervisor elsewhere watches.

**The `cpp/` directory.** `cpp/specread.cpp` parses a download spec and prints what it read, so the
Go, Python and C++ readings of one spec can be compared against the samples in `testdata/specs/`.
**There is no C++ implementation of this library** — no fetcher, runner, delegator, supervisor or
build file; it needs the job repository's `cpp/include` and `nlohmann/json` on the include path.

## Status

Experimental, at `v0.2.0`. Interfaces and the on-disk spec may change. **Go** is complete: the
client, the runner, HTTP and file/SMB fetchers, delegation, BITS and NAS; `jobd` is in service-jobd.

- **Python** is partial. Its `Client` and `Runner` run, adopt and take delivery over `http`,
  `https`, `file` and `smb`, `resume_or_submit` keys work on the destination as Go's does, and
  conditional resuming matches Go — save that HTTP dates go through `email.utils.parsedate`, which
  needs an added timezone check to stay as strict as Go's `http.ParseTime`. Missing: delegation,
  reconcile, the supervisor loop, the tier registry; `adopt` skips delegated jobs, and it can use
  neither BITS nor a NAS nor recover work handed there. **C++** reads specs only. Neither can pin a
  version a source offers no validator for; only a digest catches a change then.
- **Proven ranges are Go's alone.** Python reads `verified_prefix` and ignores `verified`, so a
  transfer with a hole in it resumes at the first gap and re-fetches the rest; the record is the same
  either way. Go fetches gaps concurrently; Python does not.
- The `bits` binding drives `powershell.exe`, so it is Windows only, and its tests skip where BITS
  cannot be driven; `nas` tests reaching a real NAS skip unless `ABSTRACTION_LIVE=1` and
  `ABSTRACTION_NAS_STORE` are set. Untested: real NAS and multi-gigabyte scale.

## Tests

```
cd go && go build ./... && go vet ./... && go test ./...
cd ../python && python -m unittest discover
```

128 Go test functions across 22 files in `go/`, 23 more in `bits` and `nas` (`bits` takes about a
minute), and 60 Python tests, which find the job layer on `PYTHONPATH` or beside this checkout. Both
pin the id a destination gets; a local HTTP server covers each answer a resumed request can draw and
asserts the `Range` headers it sent.

## Requirements

Go 1.26.0 or newer; Python 3.12, no dependencies. Windows, Linux, macOS; `bits` needs Windows.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
