# History: abstraction.download

Linked from [CONTRACT.md](CONTRACT.md)'s "Reading this page."

## Superseded ids

No id is retired this release. Three ids were each declared for two
different rules; each keeps one meaning, and the other rule takes a new id
or folds into its partner.

- **DL-R30**, second declaration, "A service applies a named credential for
  the caller that submitted the work" (commit `158feb59`, 2026-09-16), is
  now **DL-K4**. `K` is this page's credential letter, beside DL-K1 to
  DL-K3, which already cited it as the rule an execution-time refusal ends
  under. DL-R30 keeps its first meaning, a runner over a shared store
  refusing an absolute destination, which the
  [`deputy-absolute-sink`](testdata/scenarios/deputy-absolute-sink.txt)
  scenario cites. Citations of the credential sense were moved to DL-K4 in
  DL-K1, DL-K2, `docs/BASE-PROTOCOL-CHANGES.md` §20,
  `research/first-run/DECISION.md` and `research/first-run/REVIEW.md`.
- **DL-R31**, second declaration, "A capability nothing on the machine would
  perform is refused at submission," is now **DL-R38**. DL-R31 keeps "Two
  paths, one file," the meaning DL-S12 and DL-S24 cite on this page.
  `research/triage126/TRIAGE.md` asked for one of the two to be renumbered.
- **DL-E15** named two sentences: which parser reads a locator's host, and
  the refusal of an authority that is not a plain host name. DL-E15 keeps the
  refusal, which a scenario can observe, and the parser sentence is now part
  of its text, the change `research/triage126/TRIAGE.md` recommended.

Dated run transcripts under `research/` (`conf109`, `gate120`, `pred166`,
`rust161`, `show199`, `refresh182`) and `docs/results/BEHAVIOUR1.txt` quote
DL-R30, DL-R31 and DL-E15 with the titles they carried when the run was
made. They are historical record and were left as written.

Every other id keeps its letter and number. Each declaration moved from an
inline `[DL-R1]` after whichever sentence it ended to `**[DL-R1] Title.**`
at the head of the paragraph that states the rule
(`research/vocabulary/DECISION.md` S3).

New ids, declared for the first time:

| id | rule | source |
|---|---|---|
| DL-R37 | a result document and a partial are `tmp` | `research/storage-classes/DECISION.md` §8, REVIEW.md finding 2 |
| DL-R38 | a capability nothing would perform is refused at submission | second DL-R31, above |
| DL-R39 | a stalled transfer is checkpointed on time alone | an untagged bold paragraph of "What the Runner guarantees" |
| DL-R40 | resumed bytes are proven by the digest | an untagged bold paragraph of "What the Runner guarantees" |
| DL-R41 | progress carries a total when one is known | an untagged bold paragraph of "What the Runner guarantees" |
| DL-R42 | recoverable submission is claimed only by a path that keeps it | the untagged "Required downstream submission recovery" section |
| DL-R43 | recovery never submits new work | the same section |
| DL-K4 | the service applies a named credential | second DL-R30, above |
| DL-E17 | a typed cause rides under its own key (`failure@2`) | an untagged bold paragraph of "Two endings" |
| DL-E18 | a record written before the key keeps its meaning | an untagged bold paragraph of "Two endings" |

One citation was corrected. The host rule now in DL-E15's text cited
DL-E7, the `416`/`412` restart rule, for "whether a credential may be
attached"; the credential-host rule is DL-R29, and DL-E15 cites it.

## Design records

- `research/vocabulary/DECISION.md` §2: D1 (job), D4 (service), D6
  (module), D28 (implementation), D30 (deprecated, with a removal release
  or kept), D36 (destination; the spec field `sink` stays until its own
  release), D38 (runner), D51 (outcome), D79 (host, the URL sense only),
  D80 (hop).
- `research/vocabulary/DECISION.md` §3 (S1 to S13): the contract shape this
  page follows. The page code is `DL` (S4). S12 reserves `A`, `E` and `X`;
  every other letter this page uses (`R`, `V`, `K`, `N`, `S`, `W`) is
  declared in its own table.
- `research/vocabulary/RENAME-PLAN.md` §3, download (12 steps), cited by
  number in [Applied and skipped](#applied-and-skipped); §4, the third
  release, schedules download `request@2` for `destination`.
- `research/storage-classes/DECISION.md` §3, §8 and D6, with
  `research/storage-classes/REVIEW.md` finding 2 and its disposition row:
  the partial's lifetime is the job record's, until a terminal state and
  collection, because DL-R35 adopts a stopped job after its 30 s lease
  lapses. DL-R37 states the class; the collector and the runtime's `tmp/`
  directory are under CONTRACT.md's "Not built."

## Applied and skipped

- Step 1, the `**[DL-R1] Title.**` form and the letter table (R runner, S
  spec, E error and outcome per S12, N network, V version, K credential, W
  wanted): applied. The plan's letters agree with S12: every `DL-E` rule
  decides or carries how a failure ends, and `A` and `X` are unused, so no
  id changed letter for S12. The plan left `V`, `K` and `W` unnamed; this
  page names them version, credential and wanted.
- Step 2, job, work, operation → job (D1): applied in prose. `CancelWork`,
  `ObserveWork`, `ListWork` and `WorkState` are wire names of
  `abstraction.job/acceptance@1` and appear in the prose-to-wire table.
  "No operation field" in the delegate channel keeps "operation": it names
  a store call, a different sense.
- Step 3, sink → destination (D36): applied in prose. The spec document's
  `sink` object, the Go type `Sink` and the three refusal texts
  `download: sink path …` are wire and stay, in the prose-to-wire table.
  The plan places the rename at download `request@2`, but `request.thrift`
  has no sink field: the service allocates its own result document
  (DL-R36), and `sink` lives in the spec document the Go runner and
  `specread` read. The release that renames it has to name that document's
  own version.
- Step 4, supervisor → runner (D38): applied. The reserved file names
  `supervisor.json`, `supervisor.json.tmp` and `supervisor.sock` stay as
  names (DL-S9), because a store written by an earlier release may hold them.
- Step 5, tier → hop (D80): applied at the plan's two lines (DL-R24's
  "the tier most people actually run", DL-R33's chain head) and in "names a
  tier" under Bounds; README's "delegation tiers" became hops.
- Step 6, binding → implementation (D28): applied to "Bindings differ"
  (DL-R24), "the file binding's layout" (DL-S9), "a `job` file store
  binding" (Not built), and "the `nas` binding" (below). "The service
  binding" in the delegate channel became "the job service."
- Step 7, legacy in-process profile → deprecated in-process client (D30):
  applied in README.md. No removal release is named in `RENAME-PLAN.md`,
  `DECISION.md` or `ARCHITECTURE.md`, so it is recorded kept, as config and
  asks recorded theirs.
- Step 8, service profile → service (D4): applied at DL-R36 and at DL-K4
  (the second DL-R30). At DL-R40, the plan's DL-R16 area, "a service
  profile that ends digest mismatches" became "the download service's
  execution profile": `download-http-request-v1` is the string
  `HTTPExecution.Profile()` returns, a code identifier that keeps its name.
- Step 9, verdict → outcome (D51): applied at DL-S19 and DL-S25. The
  directory `testdata/verdicts/`, the removed `verdict-conformance.sh` and
  the job module's `Verdict` enum keep their names.
- Step 10, host stays the URL host (D79): nothing to change. The plan lists
  DL-E7 among the host rules; DL-E7 names no host, and the citation it
  reflects is the one corrected above.
- Step 11, layer → module (D6): applied throughout.
- Step 12, "Why these two exist," "Tested" and "Why" → `HISTORY.md`:
  applied, below. The sweep's account of `jobd`, the designation
  measurement, the DL-E15 measurement and the dated incidents behind several
  rules moved with them.

## DL-R13

Go and C++ both recorded the failure without first writing down the bytes
the run had proved; Python did not, and Python was right. On a 64-byte
scenario the difference is 32 bytes. On a 40 GB model over a link that drops
every ten minutes it is the whole product, and no 40 GB fetched twice is
what this module is for.

## DL-E6

RFC 9110 §15 requires a client meeting an unrecognised status code to treat
it as the `x00` of its class, so an unknown 4xx must behave as `400`, which
is on the refusal list. Obeying it would end a job forever on any 4xx
nobody had heard of. The cost is asymmetric and the population is not the
one the RFC assumes: an unfamiliar 4xx in front of a large file is far more
often an intermediary than an origin refusing the artifact. `499` is nginx
closing on its own client and `440` is an IIS session timing out, and
neither is a statement about the file. Of three implementations measured,
none had implemented the published rule either; DL-E6 departs from RFC 9110
as written.

## DL-E15

Measured over 56 locators against all three parsers and against the
address each transport actually dialled, 22 rows had at least two answers.
`https://hf.co\@evil.com/x` is `hf.co` to a hand-written scan and
`evil.com` to `WinHttpCrackUrl`, so a credential bound to `hf.co` was
attached to a request whose socket opened elsewhere; the reverse spelling
`https://evil.com\@hf.co/x` fails the other way and merely does not work.
`https://ｈｕｇｇｉｎｇｆａｃｅ.ｃｏ/x` and `https://huggingface。co/x` are
distinct strings to every parser and `huggingface.co` to Go's dialler, so a
refusal list naming the site did not refuse them;
`https://huggingface.co./x` resolves to the same four addresses as
`https://huggingface.co/x` and was not refused either. `urllib.parse`
deletes tabs and newlines from an authority before reading it, so
`hf.co<TAB>.evil.com` becomes a plain host that appears nowhere in the
locator.

## Authority is designation

The runner bus, a local transport `jobd` listened on for `look` and `who`,
was removed with `jobd` in 0.1.8. It carried no authority in either
direction. The measurement behind that choice still decides this module.
On macOS 15.7.4, 2026-09-05
([`probe-evidence.txt`](https://github.com/openabstractions/abstraction-identity/blob/main/probe-evidence.txt)),
a helper forked before the connection existed, then `exec`ed `/bin/cat`
while holding the peer socket. The kernel then described that peer as pid
52137, `exec=/bin/cat`, `package=com.apple.cat`, code signed by Apple
Software Signing, with `startedAfterConnect=false`, so a "started before it
connected" check passed too. `anchor apple` accepted what was an unsigned
caller a moment earlier. Every identity signal the platform offers bound to
the wrong process; only uid survived. The Linux and Windows equivalents
were not probed.

## The sweep before jobd was removed

`jobd`, the separate runner program that ran the sweep over the earlier job
store from a scheduled task or the Windows per-user service, was removed in
0.1.8 with its `supervisor.json` heartbeat and its bus
([docs/REMOVED.md](https://github.com/openabstractions/abstractions/blob/main/docs/REMOVED.md)).
The daemon `docker-jobd` and `addon-synology` call `jobd` today is a
different, still-shipping program built from the separate `service-jobd`
repository. Before its removal the command proved the sweep
([`docs/results/SUPERVISOR1.txt`](https://github.com/openabstractions/abstractions/blob/main/docs/results/SUPERVISOR1.txt)):
a real 313 MB download killed with `SIGKILL`, then, with no person running
the downloader again, a single `jobd once` found the abandoned job, finished
it, and delivered a file matching the digest Hugging Face published. A
second sweep correctly did nothing. No shipped program has watched
`wanted/` since.

`scripts/verdict-conformance.sh` fed every file in `testdata/verdicts/` to
every registered implementation in both modes and compared the outcome
(accepted, refused, or neither). It was removed with the Python and C++
implementations in 0.1.8, when one implementation had nothing to disagree
with. The corpus stays and only grows.

## Why these two exist and aria2 does not

From the survey, and both answers were to write it:

- **aria2** is GPL-2.0, a hard no for this project even at subprocess
  distance, and had shipped one release in about two years.
- **curl** is licence-clean and buys nothing from Go: `net/http` already
  does ranges, redirects, proxies and TLS with no CGO and no
  cross-compilation tax. The lesson worth taking: `CURLOPT_RESUME_FROM` is
  32-bit and breaks silently past 2 GB, so every offset here is `int64`.
- **rsync** is GPL-3.0, absent from Windows without vendoring Cygwin, has
  no job identity, and its delta algorithm is counterproductive on single
  opaque 40 GB blobs.

`https://github.com/openabstractions/research/blob/main/transfer/SUMMARY.txt`
said adopt BITS rather than write it, and that held up: persistent jobs with
a GUID any process can open, documented ownership transfer, auto-resume on
logon and network recovery. The `nas` implementation needed no new wire
format and no network protocol. It writes a job record into a store the far
side can also see, and polls it by reading a file on a share. That it
required nothing new is the strongest evidence the job module was cut in
the right place.

## Measured and dated, behind rules

- DL-R3: an earlier rule deleted a file shorter than its checkpoint claimed,
  on the argument that something outside the library edited it and none of
  it could be believed. Deleting a verified prefix to avoid a check that
  happens anyway is 40 GB fetched twice.
- DL-R11: before the header was sent by name, one legal gzipped response
  was decoded and completed by Go's `net/http`, hashed as the envelope by
  Python, and failed a third way by C++.
- DL-R28: on 2026-09-06 the NAS fetched 386 MB from Hugging Face twice over
  a file it already held, digest verified both times.
- DL-R34: a qualification run on Windows restarted a 16 MiB range from its
  first byte after 4 MiB had landed.
- DL-E3: this module kept one list of permanent errors in Go and one in
  Python; they disagreed by a row for as long as both existed, and neither
  language could see it.
- DL-E9: `3` for a permanent refusal was `dlc`'s, written independently of
  the Go tool, which arrived at the same number; two implementations
  converging on a code is better evidence than either arguing for it.
  `wget`'s nine codes are half causes and half classes, and `rsync`'s are
  causes with two exceptions, `23` and `24`, which exist because partial
  transfer needed a class.
- DL-S5: for four months this page claimed a foreign absolute path fails
  with no such file and quietly creates no directory. That is true of a
  path being read and false of a destination.
- The spec reading: a digest written bare reached a reader that built
  `"sha256:" + hex` and compared strings; the error read
  `got sha256:1fc70f… want 1fc70f…`, the same digest twice, and a correct
  1.5 GB download was deleted and fetched again. `specread` exists because
  of it.

## The conformance harness

The internal `behaviour-conformance.sh` maintainer check starts
[`testdata/fixture.py`](testdata/fixture.py), and a scenario names the
answer it wants in the URL: a range honoured, a range ignored, a `416`, a
`206` starting somewhere else, a `Content-Range` that lies about the body,
a body that disagrees with its own `Content-Length`, a redirect, a version
that changed under a resume. A new wire case costs a fixture answer and a
scenario, and no driver changes. A `# expect <step>: <phrase>` line is
checked against that step's transcript in every implementation, so the
contract is a fourth party to the comparison, and a rule every
implementation breaks the same way goes red. It did not for `terminal`,
where all three allowed an update to a record this project's own page
calls history. The assertion is partial by design: a scenario pins the
fields a written rule settles, and the harness counts the scenarios that
assert nothing. Every rule on this page carries an id a scenario can cite,
so the harness prints the rules nothing exercises.

## Tested

```bash
go -C go test ./...
```

The transfer path: resume from a partial; discarding an unproven tail; a
checkpoint that claims more than the file holds; refusing a server that
ignores `Range`; refusing a wrong digest and deleting the bad partial;
falling back to a second source; honouring a required capability; adopting
orphans. The delegation path, against a fake that behaves like BITS:
recording the handle and releasing the lease; a second process tracking
progress without holding a lease; two-phase finalisation; refusing a
delegate that delivered the wrong bytes; falling back to an in-process run
when the delegate vanishes.

Not yet tested: a real kill in the middle of a real multi-gigabyte
transfer, and anything at NAS or BITS scale.

The Python and C++ download fetchers were removed on 2026-09-15. The Python
test DL-R6 cited, `test_restarts_at_zero_when_a_server_ignores_range` in
`python/test_abstraction_download.py`, went with them; `go/runner_test.go`
and the `wire-ignored-range` scenario hold the rule.

## Not carried forward here

- The job module (`openabstractions-flat/abstraction-job/`) is another
  worker's this release. Its `SPEC.md` cites `abstraction-download/README.md`
  for DL-R1 and DL-R27, which CONTRACT.md declares; that citation is the
  job worker's to move.
- `scripts/behaviour-conformance.sh` reads a rule's wording from the
  sentence before its bracketed id, and the head-of-paragraph form puts the
  title after it; its listing of unexercised rules prints an empty title
  for every converted page.
