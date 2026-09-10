# abstraction-download, in Python

A download that outlives the process that asked for it. What was asked for, how
far it got and who may work on it live in a record on disk, so a transfer this
process starts, the next launch — or another machine — can finish. Standard
library only.

This page is the Python package. The contract, the Go and C++ implementations,
what is measured and what is `UNPROVEN` are on
[the repository](https://github.com/openabstractions/abstraction-download).

## Install

Not on PyPI, and the names on PyPI are not ours. Clone the five repositories
beside each other and install them in dependency order:

    git clone https://github.com/openabstractions/abstraction-cas
    git clone https://github.com/openabstractions/abstraction-watch
    git clone https://github.com/openabstractions/abstraction-job
    git clone https://github.com/openabstractions/abstraction-config
    git clone https://github.com/openabstractions/abstraction-download

    pip install ./abstraction-cas/python ./abstraction-watch/python
    pip install ./abstraction-job/python ./abstraction-config/python
    pip install ./abstraction-download/python

Python 3.9 or later. The order is not a style: `pyproject.toml` names these
dependencies by the names they would have on an index, and pip can only satisfy
them from what is installed already.

Copying the modules works too, and is the route the one shipped adopter takes.
`abstraction_cas.py`, `abstraction_watch.py`, `abstraction_job.py`,
`abstraction_config.py` and `abstraction_download.py` are pure standard library,
so a `_vendor/` directory of your own is a complete installation — see
[adopter-comfyui](https://github.com/openabstractions/adopter-comfyui).

## An example that runs

```python
import abstraction_download as dl

svc = dl.discover()
print("who would fetch this:", svc.where())

job = svc.get("https://raw.githubusercontent.com/openabstractions/abstractions/main/LICENSE", "LICENSE.txt")
record = svc.deliver(job)
print("bytes are at:", dl.spec_of(record).sink.final)
```

Nothing needs configuring and no service needs to be running. On a machine with
neither it prints `who would fetch this: here` and the path the file landed at.
`get` returns a job id at once and the transfer may outlive this call and this
process; `deliver` waits for the bytes and takes delivery, which is what a caller
that reads the file on the next line needs. `where` names who would take the next
job — `here`, `bits`, `nas`, `nobody` — as text for a status line, not a value to
branch on.

Your application was closed mid-transfer. This is the next launch finishing what
nobody is working on, before it asks for anything new:

```python
import abstraction_download as dl

print("resumed:", dl.discover().runner.adopt())
```

## What an application calls

| call | what it does |
|---|---|
| `discover()` | a `Client` for this machine's store. Names no path |
| `Client.get(source, destination)` | fetch a URL to a path. Returns a job id now; the work may outlive the process |
| `Client.submit(spec, requires=[...])` | `get` for a caller with a digest to verify, several sources, or capabilities an implementation must have |
| `Client.deliver(job_id, timeout=None)` | block until the bytes are here, take delivery, return the record |
| `Client.take_delivery(job_id)` | "I have it". A finished transfer is not collected until somebody says so, and uncollected ones accumulate |
| `Client.jobs()` | every download on this machine, as a snapshot |
| `Client.where()` | who would perform a job submitted now |
| `Client.runner.adopt()` | finish downloads nobody is working on. Returns how many |

A `Spec` is what to fetch: an `Artifact` (`digest`, `size`), an ordered list of
`Source` locators, and a `Sink` naming where the bytes go.

    dl.Spec(
        artifact=dl.Artifact(digest="sha256:...", size=328597408),
        sources=[dl.Source(scheme="https", locator="https://...")],
        sink=dl.Sink(final="models/x.gguf"),
    )

Capabilities `requires` accepts: `dl.CAP_RESUME`, `dl.CAP_SURVIVES_PROCESS_EXIT`,
`dl.CAP_VERIFIES`, `dl.CAP_DELEGATES`. Every normative rule is on
[CONTRACT.md](https://github.com/openabstractions/abstraction-download/blob/main/CONTRACT.md),
tagged; none is restated here.

## What may break

- **The bytes are fetched by this process and they stop when it stops.** They
  resume when it starts again; nothing arrives while your application is closed.
  Requiring `CAP_SURVIVES_PROCESS_EXIT` from Python is refused, not silently
  downgraded — that needs [service-jobd](https://github.com/openabstractions/service-jobd),
  which is a Go binary a Python application cannot ship.
- **No tagged release and no package index.** Pin a commit you have read.
- **`watch`, which this depends on, carries no conformance verdict.**
  [What is proven and what is not](https://openabstractions.org/coverage.html).
- Every published transcript was produced on Windows or Linux. macOS is
  `UNPROVEN` throughout.

Apache-2.0. See [LICENSE](LICENSE).
