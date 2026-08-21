# download — one interface, many ways to actually get the bytes

Every local-AI tool ships its own downloader. Each one re-solves resume, retries,
progress and verification privately, and each one loses your 40 GB model when the
machine sleeps — because the transfer lives inside the application, and when the
application goes away the transfer goes with it.

This is the interface those tools should have been coding against. It sits on top
of [`job`](../job/README.md), which owns the part that has to survive: what is
wanted, where it may be had, how far it got, and who is allowed to work on it
right now. This layer adds the one thing missing — something that can fetch.

## The split, and why it is where it is

```
  job         id · state · lease · progress · delegation · spec(opaque)  ← survives
  ─────────────────────────────────────────────────────────────────
  download    owns the spec: artifact · sources · sink
              Runner: claim → resume → hash → verify → deliver
  ─────────────────────────────────────────────────────────────────
  Fetcher     http · file/smb · (BITS) · (NAS) · (peer)            ← replaceable
```

**The job layer does not know what a download is.** `artifact`, `sources` and
`sink` live in this package's `Spec`, which the job record carries as an opaque
blob tagged `kind: "download"`. So downloading can grow mirrors, chunk manifests
and webseeds without touching a record that Go, Python and eventually C++ all
have to agree about.

That was not the first design. An earlier version put those three fields in the
job record itself, and the schema had to move the moment a real implementation
(BITS) turned up that did not fit. Moving them out is what stopped that.

## Two shapes, and they cover everything

Every implementation category the surveys turned up falls into one of two:

| shape | who does the work | examples |
|---|---|---|
| **`Fetcher`** | streams bytes through us | http, a local file, an SMB path |
| **`Delegator`** | the other system does it | BITS, a curl subprocess, a NAS daemon, a torrent client, a durable engine |

That is not a taxonomy invented for tidiness. A Fetcher gives us an
`io.Writer`'s worth of control and dies with our process. A Delegator writes the
file itself, under its own account, keeps going while every process that asked is
closed, and hands back nothing but a handle. Forcing BITS through the Fetcher
interface would have meant reimplementing BITS badly.

Because the two shapes are now both known, adding the BITS or NAS binding needs
no new interface — which is the answer to the fair objection that an abstraction
changing shape per tool is not an abstraction.

**Fetchers are deliberately small and dumb.** A Fetcher appends bytes and reports
how many. It does not verify, does not retry across sources, does not decide
where the file goes, and never touches the job record.

Everything that must be identical no matter who ran the transfer — hashing,
resume position, progress persistence, lease renewal, the final rename — lives in
the Runner. That is what makes a transfer started by one implementation
finishable by a *different* one, which is the entire premise.

It also keeps the interesting implementations possible. The best downloader on
Windows is not ours: **BITS** already runs transfers that survive logoff and
reboot. The right way to reach a NAS is not a protocol, it is opening a UNC path
and letting the OS redirector be the SMB client. A facade that assumed it did the
transferring itself could never delegate to either.

## Use it

```go
store, _ := job.NewFileStore("/var/lib/jobs")
runner := download.NewRunner(store, "my-app")

id, _ := download.Submit(store, download.Spec{
    Artifact: download.Artifact{Digest: "sha256:74a4da…", Size: 491400032},
    Sources: []download.Source{
        {Scheme: "https", Locator: "https://huggingface.co/…/model.gguf"},
        {Scheme: "smb",   Locator: `\\nas\models\model.gguf`, Priority: 1},
    },
    Sink: download.Sink{Final: "D:/models/model.gguf"},
})

err := runner.Run(ctx, id)
```

Note the sources: an ordered list of typed locators, **not a URL**. BitTorrent v2
and HuggingFace Xet were designed independently for exactly this payload class
and both describe a transfer as a content identity plus a list of places to get
it. Lock the descriptor to a URL and multi-source, mirrors, delegation to a NAS
and dedup across model revisions all become impossible to add later.

On start, rescue whatever was in flight when the machine slept or the app was
closed:

```go
n, _ := runner.Adopt(ctx)         // claims every orphan and finishes it here
n, _ := runner.ReconcileAll(ctx)  // catches up with work handed to a service
```

To hand a job to something that outlives this process:

```go
runner.Delegators = download.NewDelegators(bits.New())
err := runner.Delegate(ctx, id)   // returns as soon as the service has it
// this process may now exit; the transfer continues
```

`Delegate` records `{system, external_id}` in the job and **releases the lease** —
holding it would stop anyone else polling or finalising, which would make the
delegation pointless. Any later process calls `Reconcile` to catch up.

## What the Runner guarantees

**It resumes from what was proven, not from what is on disk.** The resume point
is the *smaller* of the checkpoint's `verified_prefix` and what the file actually
holds. Those differ after a crash — the record is written periodically, so the
file can run ahead of it, and a partial can also be truncated or missing. The
unproven tail is discarded, and the hash is rebuilt over the prefix that is kept.
That last part is the cost of resuming honestly: a sequential read of what you
already have, at disk speed, instead of re-downloading it at network speed.

**It refuses a server that ignores a Range request.** Ask for bytes from 40,000,
get back `200` and the whole file from zero, append it, and you have a file of
plausible length and impossible content. `curl -C -` will do exactly that. This
fails instead.

**It refuses bytes that do not match their digest**, deletes the partial rather
than leaving known-bad bytes for the next runner to resume onto, and records why
in the job so a human can read it without finding the log of a process that no
longer exists.

**It verifies what a delegate delivered.** BITS "guarantees that the version of
the file it transfers is consistent based on the file size and time stamp, not
content" — so a delegate reporting success is not evidence the file is right.
After `Finalize`, this layer hashes the delivered file itself and refuses it on a
mismatch.

**It survives the delegate disappearing.** BITS reaps jobs after 90 days, its
queue database gets discarded wholesale when corrupt, and machines get rebuilt.
A handle that no longer resolves returns the job to `pending` with its sources
and checkpoint intact, so an ordinary in-process run can finish it.

**It honours required capabilities.** A job that asks for a fetcher which
survives process exit is not quietly served by one that does not — the in-process
HTTP fetcher does not claim `survives_process_exit`, because it dies with its
caller. Bindings differ enormously; pretending otherwise lies to the caller on
the tier most people actually run.

## jobd — the supervisor

`Fetcher` and `Delegator` both leave the same gap: they only run when something
calls them. A delegated transfer that finishes while no application is open sits
there — BITS will not release the file until someone calls `Complete()`, and
nothing verifies the digest until someone asks. Without a supervisor that happens
the next time a human types a command, which may be days later.

```bash
jobd once          # one sweep — what a scheduled task runs
jobd run           # supervise until stopped
jobd status        # what is in the store, and what is stalled
jobd install       # prints the schtasks lines; does not run them for you
```

It does not move bytes. It reconciles delegated jobs, finalises and verifies the
finished ones, and adopts orphans. Reconcile runs before adopt, so the orphan
pass never picks up work a delegate has in fact already completed.

**Proved** ([`docs/results/SUPERVISOR1.txt`](../docs/results/SUPERVISOR1.txt)): a
real 313 MB download killed with `SIGKILL`, then **no human runs the downloader
again** — a single `jobd once` finds the abandoned job, finishes it, and delivers
a file matching the digest HuggingFace published. A second sweep correctly does
nothing.

**A scheduled task, not a Windows service, and on purpose.** A real service means
SCM plumbing and a dependency, and buys exactly one thing: jobs owned by
LocalSystem keep running while the user is *logged off*, because that account is
always logged on. Under a normal user account BITS still survives the application
closing and a reboot — it suspends at logoff and resumes at logon. For a desktop
that is nearly the whole win, at no cost and with no elevation. Note that BITS
itself never needed elevation; only the SYSTEM account does.

`jobd install` prints the `schtasks` commands rather than running them.
Registering a scheduled task changes your machine and you should see exactly what
it is first.

## What ships today

| Fetcher | Schemes | Resume | Survives process exit | Status |
|---|---|---|---|---|
| `HTTP` | `http`, `https` | yes, `Range` | **no** | working |
| `File` | `file`, `smb` | yes, seek | **no** | working |
| BITS | `http`, `https`, `smb` | yes | **yes** | interface ready, binding not written |
| NAS delegation | any | yes | yes | interface ready, binding not written |

The `Delegator` interface those two need is written and tested against a fake
that behaves like BITS — including delivering the wrong bytes, and vanishing.
What is missing is the binding itself.
`research/transfer/SUMMARY.txt` says adopt BITS rather than write it: it already
has persistent jobs with a GUID any process can open, documented ownership
transfer, auto-resume on logon and network recovery, and it speaks SMB in the
same job — so on Windows one binding covers the NAS case too.

## Why these two exist and aria2 does not

From the survey, and both answers were "write it, honestly":

- **aria2** is GPL-2.0 — a hard no for this project even at subprocess distance —
  and has shipped one release in about two years.
- **curl** is licence-clean but buys nothing from Go: `net/http` already does
  ranges, redirects, proxies and TLS with no CGO and no cross-compilation tax.
  What was worth taking was the *lesson*: `CURLOPT_RESUME_FROM` is 32-bit and
  breaks silently past 2 GB, so every offset here is `int64`.
- **rsync** is GPL-3.0, absent from Windows without vendoring Cygwin, has no job
  identity, and its delta algorithm is actively counterproductive on single
  opaque 40 GB blobs.

## Tested

```bash
cd download/go && go test ./...
```

19 tests. The transfer path: resume from a partial; discarding an unproven tail;
a checkpoint that claims more than the file holds; refusing a server that ignores
Range; refusing a wrong digest and deleting the bad partial; falling back to a
second source; honouring a required capability; adopting orphans.

The delegation path, against a fake that behaves like BITS: recording the handle
and releasing the lease; a second process tracking progress without holding a
lease; two-phase finalisation; **refusing a delegate that delivered the wrong
bytes**; and falling back to an in-process run when the delegate vanishes.

**Not yet tested:** a real kill in the middle of a real multi-gigabyte transfer,
and anything at NAS or BITS scale. Those need the service tier.
