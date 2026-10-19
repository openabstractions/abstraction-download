# abstraction.download contract

Remote profile: the current service methods retain `local` (explicit or
default); the general paired listener does not offer this capability. Retained
capability-specific delegation keeps its existing contract and is separate
from that listener. Held by: `idl/gen/service_test.go`
(`TestDispatcherTableCarriesRemoteProfile`) and `serve/runtime_remote_test.go`
(`TestRemoteRuntimeDescribeUsesLiveGrantAndExplicitServices`).

Binds: `request.thrift`

`request.thrift` defines the download request: the job specification an
application submits with kind `download` to `abstraction.job/acceptance@1`,
naming the artifact's digest and size, its ordered sources and an optional
network-cost constraint. It also names the guarantees a request may require,
`abstraction.download/recoverable-submission@1`,
`abstraction.download/credentials@1`, `abstraction.download/credentials@2` and
`abstraction.download/network-cost@1`, and the extension key
`abstraction.download/waiting@1`. `download.thrift` defines the failure
payloads a job record carries, `abstraction.download/failure@1` and
`abstraction.download/failure@2`. The runner is the part of this module that
executes a download: it claims a job record, moves the bytes or hands them to
a delegate that outlives this process, proves them against their digest,
resumes from what was proven and delivers the file. The record itself is
[abstraction-job](https://github.com/openabstractions/abstraction-job)'s.
[README.md](README.md) is the door: what this module is, how to obtain it,
one example that runs. That page cites rules by their ids and defines none.

## Reading this page

The conventions every OA contract carries are stated once in [CONVENTIONS.md](https://github.com/openabstractions/abstractions/blob/main/idl/CONVENTIONS.md); a rule here that applies one cites its `CONV-` id.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY" and "OPTIONAL" in this page are to be
interpreted as described in RFC 2119 and RFC 8174, when, and only when, they
appear in all capitals, as shown here.

A rule id such as `DL-R1` is declared once, in bold brackets before its
title, at the head of the rule it names. A conformance scenario in
[testdata/scenarios/](testdata/scenarios/) cites it in brackets at the end of
its `# expect` line, and a citation that resolves to no rule here is a defect
in one of the two. Prose on this page cites a rule as `DL-R1`, without
brackets. A retired id is never reused; [HISTORY.md](HISTORY.md) keeps it
with the release it left. `HISTORY.md` also carries this contract's design
records, the longer reasons behind several rules, and what was measured and
tested, linked from here and linking back.

| letter | topic |
|---|---|
| R | runner: submission and dispatch, resuming and proving bytes, destinations and the store, intent |
| V | version: the validator a resumed request sends |
| K | credential: admitting, applying and delegating a named credential |
| N | network cost: the `unmetered` constraint |
| G | egress: the schemes, redirects and addresses an application's download may reach |
| S | spec: reading, normalising and refusing a spec document and its paths |
| W | wanted: the drop folder a sweep answers |
| E | error and outcome: *not now* against *no*, the failure payloads, the exit status |

`A` (admission), `E` (error and outcome) and `X` (extension) are reserved
with one meaning in every contract (`research/vocabulary/DECISION.md` S12).
`A` and `X` are unused here: admission of a named credential is `DL-K1`,
stated with the other credential rules, and admission of a capability is
`DL-R38`, stated with the other dispatch rules. Every `DL-E` rule decides or
carries how a failure ends, the meaning S12 reserves.

| Prose | Wire (until its own release) |
|---|---|
| destination | `sink` in the spec document (`sink.final`, `sink.partial`), the Go type `Sink`, and the refusal texts `download: sink path …` (DL-S8, DL-S9, DL-S5); `research/vocabulary/RENAME-PLAN.md` §4 schedules the rename with download `request@2` |
| job | `CancelWork`, `ObserveWork`, `ListWork` and `WorkState` of `abstraction.job/acceptance@1`, renamed at `acceptance@2` (RENAME-PLAN §4, first release) |

## Submission and dispatch

**[DL-R24] Required capabilities are honoured.** The runner MUST NOT give a
job to a fetcher or delegate that does not claim every capability the job
`requires`. The in-process HTTP fetcher does not claim
`survives_process_exit`, because it ends with its caller. `requires` is a
placement constraint in the sense of Kubernetes' `nodeSelector` and Nomad's
`constraint`, and the capability names are declared features in
WebDriver's and POSIX's sense. Implementations differ enormously, and the
hop most callers actually run is this process. A caller may require
`resume`, `survives_process_exit`, `verifies_content`, `delegates`, and the
guarantees `request.thrift` names.

**[DL-R38] A capability nothing on the machine would perform is refused at
submission.** The service MUST refuse such a submission before writing a
record, and the refusal MUST say which of three conditions it met. A word
the roster does not offer is a caller's typo and is *no*. Nothing
registered promising the capability is *no*. A delegate promising it with
no runner watching the store is *not now*, because starting a runner serves
the identical request. An unknown capability name MUST be refused: a
capability only ever grants, so an unknown one matches nothing, and the job
would be refused later for the wrong reason. An accepted, unperformable
record stays in the store for a guarantee nobody could keep, and the store
is what an application lists to show a person their downloads.

**[DL-R32] A refusal names the condition that was not met.** Choosing a
fetcher asks two questions: does anything serve this scheme, and does
anything serving it promise these capabilities. A refusal MUST name the
question that failed. Reporting the scheme for both told an application its
URL scheme was wrong while the fetcher for that scheme was registered and
serving.

**[DL-R33] One rule decides who performs a job, and what is published is
that rule's answer.** A delegate is reached only through a runner; on a
machine with no runner watching the store, the head of the delegation chain
is a hop the job will never touch. What is published before a
job exists (`Where()`) MUST be a prediction made by the same rule that
dispatches, answered for a job with no requirements and a destination
anybody could resolve. What is true of one job MUST be read from that job's
own record, its delegation and then its lease, and MUST NOT be predicted
from the machine's configuration. The published string is what an adopter
puts in a status bar. Naming a hop the job never touches tells a person
their download survives closing the application when it does not.

`request.thrift`'s `downstream_recovery_guarantees` roster holds
`abstraction.download/recoverable-submission@1`. An application requests it
through job submission `required_guarantees`. The same identifier is the
adapter's capability claim and the persisted job requirement, carried
unchanged in the record's `requires`; the download request has no second
requirements field.

| Claim | Meaning |
| --- | --- |
| `survives_process_exit` | Execution can continue after the submitting process exits. It does not establish recovery of an uncertain acceptance reply. |
| `abstraction.download/recoverable-submission@1` | Reconcile downstream acceptance under its original owner and stable request key after a lost reply or restart. Reconciliation never submits a new job. |

**[DL-R42] Recoverable submission is claimed only by a path that keeps it.**
A provider MUST NOT accept `abstraction.download/recoverable-submission@1` in
the receipt unless its complete execution path preserves it. An advertising
adapter MUST retain the association between stable request identity,
original arguments and the accepted downstream job for that job's recovery
lifetime, and a fresh adapter instance MUST recover the same external
handle. Reusing an identity with conflicting arguments MUST NOT become
another job. Missing or expired history MUST NOT imply definite
nonacceptance. This trusted claim requires independent behavioural evidence
and grants no identity or authorization.

**[DL-R43] Recovery never submits new work.** When recovery is required,
selection MUST check both the explicit claim and the existing `Locator`
interface before `Start`; method presence alone is insufficient. An absent
or incapable provider MUST refuse before any downstream effect and MUST NOT
fall back to local fetching. After an uncertain `Start`, the runner MUST
retain the original owner and request key. `Locate` returns the existing
handle, proven definite nonacceptance, or uncertainty, and only proven
nonacceptance MAY lead to a new submission decision. An unreachable
adapter, a missing handle, a timeout or unreadable history MUST preserve
uncertainty and MUST NOT authorize another `Start` or a local transfer.
Settled handoffs keep their owner. Cancelling a wait MUST NOT cancel the
accepted job. This requirement uses the existing job admission and adapter
boundaries and defines no adapter IPC server; clients in every language use
the generated guarantee roster with the generated job submission and
receipt interfaces.

## Resuming and proving bytes

**[DL-R1] Resume from what was proven.** The runner MUST resume from the
smaller of the checkpoint's `verified_prefix` and what the file actually
holds. The two differ after a crash: the record is written periodically,
so the file can run ahead of it, and a partial can also be truncated or
missing.

**[DL-R2] The unproven tail is discarded and the hash rebuilt.** The runner
MUST discard the bytes past the resume point and MUST rebuild the hash over
the prefix it keeps. The cost is a sequential read of what is already on
disk, at disk speed, in place of re-downloading it at network speed.

**[DL-R3] The file is the floor.** The runner MUST NOT delete a partial to
make a record true. A file shorter than its checkpoint claims is kept and
resumed. A file of the right length that a second writer replaced is
accepted anyway, because length is the only witness there is, so refusing
the short case detects no class of corruption and declines the one case
where the damage announced itself. The digest over the whole artifact,
before anything is delivered, is what decides whether the bytes can be
trusted. See [`resume-short-file`](testdata/scenarios/resume-short-file.txt).

**[DL-R4] A checkpoint claiming more than the file holds is cut down.** The
runner MUST reduce the checkpoint to what the file holds, per range when the
checkpoint has holes, and MUST carry on from it.

**[DL-R5] The answer to a Range request the server ignored is never
appended.** When a ranged request is answered `200` with the whole body
from zero, the runner MUST NOT append it. Appending gives a file of
plausible length and impossible content; `curl -C -` does exactly that.

**[DL-R6] Such a body is taken from zero.** The runner MUST discard the
prefix and take that body as the complete artifact from byte zero. The
bytes already moved are wasted and the download still arrives; Chromium
settles it the same way in
`components/download/internal/common/download_utils.cc`. Held by
`go/runner_test.go` `TestRestartsWhenServerIgnoresRange` and
[`wire-ignored-range`](testdata/scenarios/wire-ignored-range.txt).

**[DL-R7] A 206 answers this request only if it names the offset the next
byte goes at.** The runner MUST accept a `206` only when its
`Content-Range` begins at the offset the next byte is written to. Bytes
beginning elsewhere belong at an offset nobody asked about; written where
they were asked for, they put the artifact's content in the wrong place,
which no length check and no transport error can see.

**[DL-R8] A multipart body is never an answer.** The runner MUST NOT accept
a `multipart/byteranges` body. Its boundary line and per-part headers would
be written into the artifact. RFC 9110 permits a server to answer a single
range that way and a coalescing proxy does; the runner never sends a
multi-range request.

**[DL-R9] An unasked 206 is accepted exactly when it starts at zero.** A
`206` answering a request that carried no `Range`, which CDNs send, MUST be
accepted exactly when it starts at zero, because zero is where an unranged
request writes.

**[DL-R10] A response that fails DL-R7 to DL-R9 is thrown away.** The runner
MUST discard such a response and the prefix with it, MUST ask for the
artifact whole, and MUST NOT record the response as an error. Refusing
loses a download the next request would have completed, and asking again
for the same range gets the same unusable answer on every sweep. Held by
[`wire-wrong-offset`](testdata/scenarios/wire-wrong-offset.txt),
[`wire-multipart`](testdata/scenarios/wire-multipart.txt),
[`wire-marked-multipart`](testdata/scenarios/wire-marked-multipart.txt)
(the same MIME body under a correct `Content-Range`, so the media type is
the only thing left standing) and
[`wire-unasked-206`](testdata/scenarios/wire-unasked-206.txt).

**[DL-R11] Every request asks for `identity`.** Every request MUST send
`Accept-Encoding: identity`, and a body that arrives under another content
coding MUST be refused. A digest is over the artifact, and decoding changes
what the bytes are. A request with no `Accept-Encoding` accepts every
coding, and the answer then depends on which language's transport is
underneath. Setting the header is also what turns off Go's transparent
decompression. See [`wire-encoded-body`](testdata/scenarios/wire-encoded-body.txt).

**[DL-R12] That refusal is *not now*.** A mirror, or the same server
tomorrow, may serve the identity that was asked for, so the refusal MUST
leave the job adoptable.

**[DL-R13] Bytes a run proved are written down before the failure is.** The
runner MUST checkpoint the bytes a run proved before it records the run's
failure. The periodic checkpoint fires on a byte count or an interval, so a
transfer that stops before the first one has proven nothing to the store,
and the next owner would re-fetch bytes sitting on the disk in front of it.
The reason this is a rule is in [HISTORY.md](HISTORY.md#dl-r13). See
[`wire-truncated-body`](testdata/scenarios/wire-truncated-body.txt),
[`wire-short-range`](testdata/scenarios/wire-short-range.txt) and
[`wire-undeclared-length`](testdata/scenarios/wire-undeclared-length.txt).

**[DL-R14] A checkpoint only grows.** `done` is the total of every proven
range and a stop offers a prefix; a checkpoint that already knows more MUST
keep what it knows.

**[DL-R34] A range fetched in parallel keeps what it landed.** The runner
MUST checkpoint each range still arriving as the part from its start to the
last byte written, after a sync, every persistence interval and when the
transfer stops. The next owner MUST ask only for the rest of that range, and
so MUST a fallback source in the same run. Go only; held by
`go/serve/range_resume_test.go`.

**[DL-R35] A stopped job is adopted within its [lease](https://openabstractions.github.io/reference.html#concept-job.lease).** An attempt MUST clear
the failure an earlier attempt recorded when it starts and MUST record its
own failure if it fails. Retry backoff (`abstraction-job/CONTRACT.md`
JOB-B2) MUST follow only a failed last attempt. A runtime that stops
gracefully MUST release the lease, and its successor adopts the job on its
first sweep. A runtime that is killed leaves the lease to expire, and the
successor adopts the job within the lease TTL (30 s) plus one sweep. Go
only; `go/serve/range_resume_test.go` holds a graceful successor to 5 s and a
crashed one to the lease plus 5 s.

**[DL-R39] A stalled transfer is checkpointed on time alone.** While a
transfer is open, the runner MUST sync and checkpoint the bytes its file
accepted every `PersistInterval`, including while it waits on a source that
stopped sending. A checkpoint that only a write can trigger leaves the bytes
already on disk unrecorded through that silence, and a crash during it loses
them. A resumed run keeps the checkpoint's proven prefix and truncates
everything past it (DL-R1, DL-R2).

**[DL-R40] Resumed bytes are proven by the digest.** A checkpoint records
how many bytes were synced, and a crash can still leave corrupt bytes inside
that prefix. When a run that started from proven bytes fails its digest, the
runner MUST delete the partial and its checkpoint (DL-R16) and MUST mark the
error `ErrResumedMismatch`, which is *not now*; the next run fetches from
byte zero. The download service's execution profile
`download-http-request-v1` ends a digest mismatch (DL-E17), and only one over
bytes fetched from zero.

**[DL-R41] [Progress](https://openabstractions.github.io/reference.html#concept-job.progress) carries a total when one is known.** A declared artifact
size MUST be the record's `progress.total` before any byte moves. Without a
declared size, the first response that discloses a length MUST set it, and
delivery MUST set it to the delivered size.

**[DL-R15] Bytes that do not match their digest are refused.** The runner
MUST refuse an artifact whose bytes do not match its digest.

**[DL-R16] A refused partial is deleted and the reason recorded.** The
runner MUST delete the partial it refused, leaving no known-bad bytes for
the next runner to resume onto, and MUST record why in the job, where a
person can read it without the log of a process that no longer exists.

**[DL-R17] What a delegate delivered is verified.** After `Finalize`, the
runner MUST hash the delivered file itself and MUST refuse it on a
mismatch. BITS "guarantees that the version of the file it transfers is
consistent based on the file size and time stamp, not content"; a
delegate's success is no evidence the file is right.

**[DL-R18] A delegate disappearing loses no job.** The runner MUST keep a
job whose delegate vanished. BITS reaps jobs after 90 days, discards its
queue database wholesale when corrupt, and machines get rebuilt.

**[DL-R19] A handle that no longer resolves returns the job to `pending`.**
The runner MUST return such a job to `pending` with its sources and
checkpoint intact, so an ordinary in-process run can finish it.

**[DL-R28] Bytes the store already proved are not fetched again.** Before it
opens any source the record names, the runner MUST look in its store for a
finished record with the same digest whose file is still there at the size
it proved, and MUST copy from that file first. A digest is an identity, so
whose destination that file was does not matter: a caller's earlier one, or
a delegate's own `files/`, which the submitter cannot see and the record
cannot name. See [`already-here`](testdata/scenarios/already-here.txt).

**[DL-R44] The download archive is checked before the sources.** A service
runner configured with the person's download archive, a folder the person
names in the Panel and often a NAS share, MUST look there before it opens
any source of a job whose digest is a SHA-256 and that has proven no byte.
Any app whose download job the service admits gets this, with no further
right. The runner looks for the file under its content name `sha256/<hex>`,
then under the names a stored listing of the folder records by SHA-256, by
the hex in the name and by the job's size. With no candidate, a folder that
answers and a listing older than ten minutes, it lists the folder again
first, within the walk's bounds of 30 seconds and 50,000 entries. It copies
a candidate into the job's own partial file, hashing every byte, and the
app sees the bytes only after the SHA-256 of the whole copy equals the
digest. A content-name entry whose bytes or size differ is renamed to
`sha256/.bad/<hex>` with one log line; another candidate that differs,
stalls or is reached through a link is skipped with one log line. After two
such copies the job goes to its sources. A folder that does not answer
costs one probe of at most three seconds, bounded also by the caller's
deadline, and is not asked again for a minute.

After the sources deliver and verify a file of 50 MB or more, the runner
queues it, and a background copy in the runtime writes it to
`sha256/<hex>.partial` and renames it to `sha256/<hex>`. It skips the copy
with one log line when the archive volume's free space after it would fall
below the smaller of 10% of the volume and 50 GB, when the person turned
Keep a copy of new downloads here off, or when the listing records that
SHA-256 under another name. Every archive path is built from the person's
folder and a validated lowercase 64-hex digest. No result, progress,
receipt or error an app sees names the archive folder or a credential; the
runtime opens the share with the person's own logon. The local queue keeps
at most 32 GB, the archive has no per-app quota, and the runtime never
evicts from it. The runtime counts the bytes it wrote for each app in its
own state. The archive is never served in place, and it never fails a job.

Architect ruling of 2026-11-11, as given:

> Read and write go through any app's download job with no new right, a
> 50 MB floor for copies and a free-space reserve on the NAS.
>
> 1. Read: accepted as is. The membership probe equals DL-R28's local one.
>    Conditions:
>    - The runtime verifies the bytes against the app's digest before the
>      app sees any of them.
>    - On a mismatch it falls back to the internet, renames the bad archive
>      entry to `sha256\.bad\<hash>`, and writes a log line. OA repairs its
>      own folder.
>    - If the NAS is unreachable, it falls back to the internet within the
>      caller-derived timeout.
>
> 2. Copy-into-archive: any app whose job the download service already
>    admits. Third-party apps get what our own adopters get. Content
>    addressing makes write poisoning impossible, since a file under
>    `sha256\<H>` hashes to H, and every reader verifies its own digest
>    anyway. Conditions:
>    - Copy only after the internet download is verified.
>    - Copy only files of 50 MB or more. The archive holds models, per the
>      owner rule.
>    - The runtime does the copy with the NAS credential. The app never gets
>      the NAS path or credential.
>    - Write to a temporary name in the archive, then rename.
>    - Build the path only from a validated lowercase 64-hex digest. No
>      app-supplied path parts.
>
> 3. Caps: a free-space reserve, with no per-app quota.
>    - Skip the copy when the NAS volume's free space after it would fall
>      below the smaller of 10% of the volume and 50 GB.
>    - Keep the 32 GB local copy queue.
>    - Never evict from the archive automatically.
>    - A skipped copy writes one log line.
>    - The Panel's storage page shows the archive size and the per-app
>      bytes written, read from the log or index.
>
> 4. Security review: rides the single R30 pass. Checklist:
>    - verification before hand-off;
>    - digest format validation and path build;
>    - no NAS credential or path in any app-visible result or error;
>    - temporary-then-rename on both sides;
>    - the fallback when the NAS is unreachable or an entry mismatches.

**[DL-R45] A managed download is refused only at the disk's floor.** The
runtime's managed download keeps a floor of free space on the volume that
holds its work: the smaller of 1 GiB and 2% of the volume. A download of
known size MUST be refused, before any byte moves, only when its remaining
bytes would leave less than the floor. The refusal ends the job as failed,
and its failure message names the bytes the download needs, the bytes the
disk has free and the floor, as in `not enough disk space: this download
needs 20.0 GiB and the disk has 12.0 GiB free; OpenAbstractions keeps 1.0
GiB free`. A download of unknown size MUST be admitted. Its writes ask for
room 16 MiB at a time, and it ends the same way before the volume falls
below the floor. A volume whose space the runtime cannot read takes the
download. The larger comfort reserve, the smaller of 20 GiB and 10% of the
volume, never refuses: a download of 64 MiB or more that would leave less
than it writes one log line saying the disk is nearly full. The NAS
archive's reserve in DL-R44 is separate.


## Versions

**[DL-V1] A resumed request names the version it continues.** A bare
`Range` names no file. A source that replaced the artifact answers honestly
with a valid range of something else, and the splice has exactly the right
length and the wrong contents. A ranged request MUST therefore name the
version its proven bytes came from. A validator is scoped to the source that
issued it: a mirror serving the same bytes names them with its own `ETag`
and `Last-Modified`.

**[DL-V2] The validator travels as `If-Range`.** The checkpoint MUST carry
the strong validator the proven bytes came from, and every ranged request to
the source that issued it MUST send it as `If-Range`. The source answers
`206` for the same file and `200`, the whole new one, for a different one,
which DL-R5 and DL-R6 already handle. Single-stream and parallel requests
both apply the issuing source's scope. A job with several sources follows
these rules on its parallel run:

- The checkpoint MUST record the issuing source as `validators.source`.
- A range asked of another source MUST go without the recorded validator.
  The artifact digest guards that range's bytes when the job has one.
- Within one run, each other source MUST be held to the first validators
  it named: they go with its later ranges as `If-Range`, and a range it
  serves under another version is a version change.
- A version change reported by any source MUST end the run, and the
  runner MUST restart it from byte zero once, after clearing the
  checkpoint. The range MUST NOT fail over to the next source. On a job
  without a digest, this restart keeps two versions out of one file.

`validators.source` is the source's `locator` exactly as the job's
`sources` list spells it, scheme included, for example
`"https://host/path"`, and it is compared with each locator byte for byte.
A checkpoint without it sends its validators to every source, as every
checkpoint written before the field did. The single stream records the
issuing source and sends its validators only to that source. A source naming
another version answers `200` and restarts it from byte zero (DL-R6).
File and SMB sources name no validators. A resume holds only the recorded
source to a version; every other
source starts afresh. See `TestAMirrorWithItsOwnValidatorsCompletesTheRun`,
`TestAResumeSendsAValidatorOnlyToTheSourceThatNamedIt`,
`TestAnOwnersVersionChangeRestartsTheRunFromZero` and
`TestAMirrorReplacedMidRunAfterTheOwnerWentDownRestarts`.

**[DL-V3] Only a strong validator is kept.** A weak `ETag` MUST NOT be kept.
It asserts semantic equivalence, and a resume depends on byte equality.

**[DL-V4] `Last-Modified` stands in for a missing `ETag`.** The runner MUST
use `Last-Modified` when there is no usable `ETag`, and only when it is
strong: the response carries a `Date` at least one second later (RFC 9110
section 8.8.2.2). Two versions written within one second share a
`Last-Modified`. A source whose only validator is a `Last-Modified` that is
not strong is recorded as weak, retaining its timestamp for comparison, and
the runner MUST restart such a resume
from byte zero, because neither a bare `Range` nor `If-Range` on that value
can tell the two versions apart. A weak timestamp MUST NOT travel as
`If-Range`. Within a run, the same source strengthening the same timestamp
upgrades its held metadata; a different timestamp or validator remains a
version change. An older weak record without a timestamp cannot prove this
upgrade. See `TestAWeakLastModifiedBecomesStrongWithoutRestart`,
`TestWeakValidatorUpgradeRequiresSameSourceAndVersion`,
`TestSingleStreamScopesValidatorsToTheirSource`,
`TestAWeakLastModifiedResumeNeverSplices`,
`TestAWeakLastModifiedParallelResumeNeverSplices` and
`TestAWeakRestartParallelProgressSurvivesInterruption`.

**[DL-V5] Three date spellings are recognised, by shape.** The accepted
`Last-Modified` spellings MUST be the three RFC 9110 requires a recipient to
accept: IMF-fixdate, `Sun, 06 Nov 1994 08:49:37 GMT`, the only one a sender
may use, and the two obsolete forms `Sunday, 06-Nov-94 08:49:37 GMT` and
`Sun Nov  6 08:49:37 1994`. Recognition MUST be a shape check, and no time
is parsed out of the value except for the DL-V4 comparison with `Date`. The
value is echoed back verbatim and an origin server evaluates `If-Range` by
exact match, so the only question is which spellings are recognised. Each
standard library disagrees about what an HTTP date is: Go's grammar excludes
a three-letter zone the obsolete forms carry, and Python's `email.utils`
parses RFC 2822 with numeric zones. A validator one implementation records
and another drops is a resume that cannot name its version. Go is the one
implementation since 2026-09-17 (`docs/REMOVED.md` entry 10); the rule holds
any second one to the same shapes. See
[`wire-version-changed`](testdata/scenarios/wire-version-changed.txt),
[`wire-version-modified`](testdata/scenarios/wire-version-modified.txt) and
[`wire-obsolete-date`](testdata/scenarios/wire-obsolete-date.txt).

## Destinations and the store

**[DL-R20] A record another machine wrote is worked here.** A relative
destination MUST be resolved against this machine's own view of the store
root. A record written by Windows into `\\nas\models\store` is the same
record a container reading `/store` acts on, with no NAS-specific code
anywhere. A relative destination is the portable form (DL-S5 to DL-S7).

**[DL-R21] A foreign absolute destination is refused.** An absolute
destination MUST be used unchanged by a machine whose convention it is
written in and MUST be refused by any other, as *not now* (DL-E5). Taken
literally, `D:\models\x.gguf` on Linux becomes a file of that name in
whatever directory the process happened to be in.

**[DL-R30] A runner over a store several machines can write refuses an
absolute destination.** Such a runner MUST refuse an absolute destination,
and the refusal MUST be *not now*: the record is valid where it was
written. An absolute path is never joined onto the root, so containment
never sees it, and a path in this machine's own convention is not foreign:
`/etc/cron.d/x` on the NAS passes every lexical check and would be written
with the runner's authority where a record somebody else wrote chose. An
absolute destination is legitimate only for a caller writing to its own
machine, which `Client.Get` to a local path is and an adopted record never
is. See [`deputy-absolute-sink`](testdata/scenarios/deputy-absolute-sink.txt).

**[DL-R22] A destination cannot name the store.** A destination MUST NOT
name the store's own layout (DL-S9). Staying inside the store root is
insufficient: `jobs/<id>.json` and `work/<another id>` are both inside it,
and both would have been written.

**[DL-R23] A job writes one path the store owns.** Inside the store's own
layout, the runner MUST let a job write only its own `work/<id>`.

**[DL-R31] Two paths name one file when the volume says so.** Whether two
destinations name one file is a question about the volume holding them. An
implementation MUST answer it by asking that volume and MUST NOT decide it
from the bytes of the path or from the name of the host operating system.
Every platform furnishes the question: `os.SameFile` in Go,
`os.path.samefile` and `os.path.samestat` in Python,
`std::filesystem::equivalent` in C++17, `java.nio.file.Files.isSameFile` in
Java, which may open both files. All four compare device and inode, or
volume serial and file id. [`go/samepath_test.go`](go/samepath_test.go) is
the reproducer: `TestSamePathAgreesWithTheVolume` names the pairs
implementations were seen disagreeing about (`café` against `CAFÉ`, `K`
U+212A against `k`, NFC against NFD), creates one name, and looks for the
other on the volume under the test. NTFS folds `café` against `CAFÉ` and
holds `K` apart from `k`; APFS folds both; ext4 folds neither; a
case-sensitive APFS volume, a `mount -o casefold` ext4 and an SMB share each
falsify any rule read off the host's name. Two records for one file are two
leases, two partials and two writers racing into it. One record for two
files hands the caller a job fetching a different file and says nothing.

A destination usually does not exist yet, and its parent does. An
implementation MUST settle the parent by asking the volume and MUST settle
the final component by asking that directory to demonstrate its own rule,
creating a name it owns and looking for the other spelling of it. It MUST
NOT create either destination to find out and MUST leave nothing behind.

Two questions stay outside DL-R31 (VISION.md L623 decides both alike: no
rule is read off the host's name and no case is folded off the volume's
rule). Containment, whether a resolved path stays under the store root, is
lexical (DL-S8): a resolved destination is the root joined to a relative
path, so it carries the root's own spelling, both sides compare exactly, and
neither may exist. A destination key, the string a record id is derived from
before any record exists, is the deepest existing folder's final path, with
links, junctions and 8.3 aliases resolved by the volume, and the names still
to be created below it, compared as the file system compares names
(`abstraction-identity` `PlaceKey`); an implementation that keys on one MUST
first scan by DL-R31, and the key is then a race window.

**[DL-R36] A job-service download's result document is the downloaded
bytes.** The download service MUST assign every job-service download's
destination to its own result document, a location the runtime owns and
the caller never names. `ReadResult` (`abstraction-job/CONTRACT.md` JOB-A11)
reads that document in chunks once the job is complete.

**[DL-R37] A result document and a partial are `tmp`.** A job's result
document MUST be of the `tmp` storage class, kept at least the declared
`result_retention_ms` (JOB-A11). A job's partial, `work/<id>` or
`<final>.part`, MUST be of the `tmp` class under its job: its lifetime MUST
be the job record's, from submission until the job reaches a terminal state
and is collected, and it MUST NOT end when the job's lease lapses, because
DL-R35 adopts a stopped job after its lease lapses and resumes from that
partial. Abandoning a job removes its partial. A caller-named destination
belongs to the user and carries no class of this runtime's. The classes are
`abstraction-storage/CONTRACT.md` STO-T1, and STO-T2 asks every module to
state the class of each kind it keeps (`research/storage-classes/DECISION.md`
§3, §8, D6; `research/storage-classes/REVIEW.md` finding 2). The explicit
service-owned collection pass adds the declared result retention and configured
temporary grace, retains aliased or unobserved link counts, and excludes migrated
and delegated destinations. Runtime scheduling remains off in 0.8.0; activation
requires the separate installed/data-loss acceptance.

The runtime's `download-provider.json` is provider-binding `data`: it retains
the selected backend and canonical NAS root beside the managed job store.
It survives process restarts and is retained with that store. A changed binding
is refused at startup so existing accepted jobs keep their provider. The NAS
share is owned by its serving runtime and carries no storage class of this
requesting runtime.

## [Intent](https://openabstractions.github.io/reference.html#concept-job.intent)

**[DL-R25] What somebody asked for is honoured.** The runner MUST check the
job's `intent` (`abstraction-job/CONTRACT.md`) before any byte moves, on
adoption, and again at every checkpoint, and MUST converge on it. An owner
that dies between a pause being asked for and the pause being carried out
leaves the record `running` under a lapsed lease, and a sweep offers that
record back precisely so the next owner can finish what the last one
started. Honouring without sweeping makes such a record unreachable
forever; sweeping without honouring restarts a download seconds after a
person stopped it.

**[DL-R26] `cancel` writes `cancelled`.** The runner MUST end a job whose
intent is `cancel` in state `cancelled`.

**[DL-R27] `pause` lets the lease go.** The runner MUST release the lease of
a job whose intent is `pause`, so the record reads `pending` and nobody is
shown an owner that has stopped.

## Credentials

**[DL-R29] A credential is bound to hosts on the fetching machine.** The
runner MUST attach a named credential only to a request whose host is among
the hosts this machine binds to that name, and MUST end any other source
naming it *not now*, before a connection is opened. A record names a
credential (`attrs.credential=hf`), never a secret, and it also names the
host the request goes to. Anyone who can write the store can write both, so
a machine that resolved the name without asking where it was going would
send the owner's token to a server the record chose. The binding lives
beside the secret, on the machine (`ABSTRACTION_CRED_HF_HOSTS=huggingface.co`,
each entry covering its subdomains), and MUST NOT be read from the record
or the store. A credential with no binding reaches nobody. The job stays
adoptable: a machine that binds the name to that host runs the same record
unchanged. See [`deputy-credential`](testdata/scenarios/deputy-credential.txt)
and [`credential-bound`](testdata/scenarios/credential-bound.txt).

**[DL-K4] The service applies a named credential for the caller that
submitted the job.** In the download service a request's
`Source.credential` names a credential registered in the submitting account.
The request MUST require exactly one of
`abstraction.download/credentials@1` and `abstraction.download/credentials@2`.
`credentials@1` retains `abstraction.credentials/applier@1` and its original
exact authorization. Explicit `credentials@2` selects
`abstraction.credentials/applier@2` and scoped authorization for every named
credential in that request. Requiring both MUST be refused `invalid` before
journaling. A provider MUST advertise `credentials@1` only with its original
applier and `credentials@2` only with a configured scoped checker, applier and
typed policy. Preparation MUST record the name and the
submitting caller's opaque scope in the source attributes; neither goes on
the wire. Scoped preparation MUST also record
`credential_route=abstraction.download/credentials@2` on every named source.
An absent route retains the original version. Unknown, malformed or mixed
source routes MUST be refused before any fetch or handoff.
For every request, redirect and resumed range, the executor MUST
ask its selected applier with consumer
`abstraction.download/http-execution@1`, MUST send the returned headers
once, and MUST keep them out of the record. An applier refusal MUST end the
attempt *no*, with message `credential:<outcome>:<name>` and cause
`credential`. The applier MUST decide each execution-time use without
raising a first-use question (abstraction-rights RIGHTS-R23); DL-K1
admission asks. A rule removed after admission refuses `not_permitted`,
which ends the attempt as above. An applier that cannot answer MUST leave the attempt *not
now*, with the same cause and the outcome `unavailable`. A service with no
applier MUST answer every such request `unavailable`. The environment
reader `EnvCredentials` is the library's own bridge and MUST NOT be
configured in the service.

The scoped route MUST obtain the authenticated subject from the receiving
job owner for the claimed operation and submitting scope. A source attribute
MUST NOT select another operation or supply caller authority. Missing subject
evidence or a missing scoped dependency MUST answer `unavailable`. Recovery
MUST reconstruct the same selected route from the journaled required
guarantees while its dependency is unavailable; it MUST never substitute the
original route. Accepted original requests retain their preparation and exact
authorization. A full GET sent after an unusable ranged response is a new request
and MUST recheck current authorization on either route.

**[DL-K1] Admission refuses a credential the caller may not apply.** Before
the job provider journals a submission naming a credential, the service
MUST ask the applier's `Check` for the submitting caller, consumer
`abstraction.download/http-execution@1`, the credential and each source host
(`abstraction-job/CONTRACT.md` JOB-A16). The outcome `not_permitted`, a
rights refusal, MUST refuse the submission `forbidden`, and any other
outcome except `applied` and `unavailable` MUST refuse it `invalid`, each
with reason `credential:<outcome>:<name>`; `unavailable` MUST refuse it
`unavailable` with `credential:unavailable:<name>`. Nothing is journaled or
sealed, and the same identity is admissible once the credential or its rule
changes. A credential revoked between admission and execution ends the
attempt as DL-K4 says.
The check MUST use the request's selected applier version. Scoped admission
MUST use the receiving boundary's authenticated subject, and an opaque scope
alone MUST return `unavailable`.

**[DL-K2] Each redirect host is its own use.** A redirect of a request that
carried an applied credential MUST drop that credential's headers and MUST
ask the applier again for a redirect target within the source's origin;
a redirect to another origin goes without the credential (DL-K5). The credential's own target
list decides: `target_refused` sends the redirected request without the
credential, `applied` sends the headers applied for that host, including on
a same-host redirect, and any other refusal ends the request as DL-K4 says.
Headers the request itself supplied MUST follow a redirect only to the
source host and its subdomains.

**[DL-K5] A named credential travels only over https, to one origin.** A
request, redirect or resumed range MUST carry an applied credential only to
an `https` URL, and only to the origin (scheme, host and port, the default
port written out) of the source it was applied for. Admission MUST refuse a
source that names a credential and is not `https` `invalid` with reason
`credential:insecure_transport:<name>`, before asking the applier. Execution
MUST end such a request *no* with the same outcome and cause `credential`,
and nothing is sent. A redirect to any other origin, an `http` hop included,
MUST drop the credential's headers and MUST NOT ask the applier, as curl does
without `--location-trusted`; DL-K2's re-application holds for a redirect
within the origin. The ruling is design B of
`decisions/RULINGS-2026-11-22-owner-designs.md`, rule 2.

**[DL-K3] Delegation carries the credential name, never its value.**
Delegated execution and remote job handoff MUST refuse `credentials@2` until
their explicit protocol preserves its authenticated subject and scoped
authorization semantics. They MUST NOT reinterpret it as `credentials@1`.
Delegated execution MUST refuse a request naming a credential unless a
configured delegate claims `abstraction.download/credentials@1`. The remote
job delegate submits the generated request with each source's credential
name to a remote OpenAbstractions job service over mutually authenticated
TLS, and the local caller scope, applied headers and secret bytes MUST NOT
cross. The remote service applies its own credential in the scope its host
maps from the verified client certificate, and its own rights decide. A
remote admission refusal `credential:<outcome>:<name>` MUST end the local
job with that reason unchanged and cause `credential`, and the local service
MUST fetch nothing itself. The remote result bytes are read back into the
local destination and verified there.

## Egress

The download service applies design B of the architect ruling of
2026-11-22 (`decisions/RULINGS-2026-11-22-owner-designs.md`) to every
download an application asks it for. Rule n of the ruling is DL-Gn:
DL-G1 schemes, DL-G2 credentials (stated as DL-K5), DL-G3 redirects, DL-G4
addresses, DL-G5 the local network, DL-G6 the person's refused hosts (stated
with DL-E8), DL-G7 the system proxy and DL-G8 refusals. The ruling, verbatim:

> **B. Download egress, as established downloaders do it. 0.8.1**
>
> The owner: "smart but not too smart … how would any normal downloader handle
> this".
>
> 1. **Schemes.**
>    - `https` is always allowed.
>    - `http` is allowed only when the request carries a SHA-256 digest
>      (integrity is then proven), as apt and package mirrors do.
>    - Every other scheme (`file`, `ftp`, `smb`, `data`) is refused.
> 2. **Credentials.**
>    - Sent only over https, and only to the exact origin (scheme, host and
>      port) the credential is bound to.
>    - Dropped on any redirect to another origin, as curl does without
>      `--location-trusted` and as browsers drop Authorization.
> 3. **Redirects.**
>    - At most 10.
>    - Every hop is re-checked against rules 1, 4 and 5.
>    - An https-to-http downgrade is refused unless the request carries a
>      digest. Credentials are already gone by rule 2.
> 4. **Addresses, checked at connect time on the address actually dialled.**
>    This is safe against DNS rebinding because the dialer checks the socket
>    address, not a name resolved earlier. Always refused:
>    - loopback (127.0.0.0/8, ::1);
>    - link-local (169.254.0.0/16, which includes cloud metadata, and
>      fe80::/10);
>    - unspecified, multicast and broadcast;
>    - IPv4-mapped and IPv4-compatible forms of any of these.
> 5. **LAN (private ranges).** 10/8, 172.16/12, 192.168/16, 100.64/10 and
>    fc00::/7 are refused for app-requested downloads by default. This matches
>    browsers' Private Network Access rule: public content can't reach the
>    person's network.
>    - The person's "allowed local hosts" list opens a named host, e.g. a NAS.
>    - OA's own NAS archive and paired-machine traffic are unaffected; they
>      don't go through app download jobs.
> 6. **The person's refused-host list** applies at every hop, to the host name
>    (subdomains included) and to its resolved addresses. Their allowed-hosts
>    list is the counterpart for the LAN.
> 7. **System proxy** is honoured. The proxy resolves names, so through a proxy
>    rules 1–3 and 6 apply by host name, and rule 4 by name only. This
>    limitation is documented.
> 8. **Refusals** name the rule: "Refused: address is on your local network".
>    These are security boundaries, so no fail-open.
>
> **The existing branches fit:**
>
> - `fix/download-credential-https` is rule 2. Extend it with same-origin
>   binding and stripping on redirect.
> - `fix/download-refused-hosts` is rule 6. Apply it per hop and to resolved
>   addresses.
>
> The allowed-local-hosts UI can follow in 0.9.0. Until then the list is a
> setting with a command-line edit.

**[DL-G1] Schemes.** The download service MUST allow `https` always and
`http` only for a request that carries a SHA-256 digest, and MUST refuse every
other scheme (`file`, `ftp`, `smb`, `data`).

**[DL-G2] Credentials.** A credential MUST travel only over https to the exact
origin it is bound to, and MUST be dropped on a redirect to another origin.
DL-K5 states this rule.

**[DL-G3] Redirects.** A download MUST follow at most 10 redirects, MUST check
every hop against DL-G1, DL-G4 and DL-G5, and MUST refuse an https-to-http
downgrade unless the request carries a digest.

**[DL-G4] Addresses.** The service MUST check the address it dials and MUST
refuse loopback, link-local, unspecified, multicast and broadcast addresses and
the IPv4-mapped and IPv4-compatible forms of each, whatever any list names.

**[DL-G5] The local network.** The service MUST refuse the private ranges 10/8,
172.16/12, 192.168/16, 100.64/10 and fc00::/7 for app-requested downloads
unless the person's allowed local hosts list names the host.

**[DL-G6] The person's refused hosts.** The refused-host list MUST apply at
every hop, to the host name with its subdomains and to each resolved address.
DL-E8 states how a refused host ends the attempt.

**[DL-G7] The system proxy.** The service MUST honour the system proxy. Through
a proxy DL-G1, DL-G2, DL-G3 and DL-G6 MUST apply by host name and DL-G4 by name
only, and the service MUST document that limitation.

**[DL-G8] Refusals.** A refusal MUST name the rule in plain words, such as
"Refused: address is on your local network", and MUST NOT fail open.

What the service does with it:

- Admission refuses a source whose scheme breaks DL-G1 `invalid`, and a
  request whose every source host a name rule refuses `unavailable`, each
  with the rule's plain reason, before anything is journaled.
- The HTTP fetcher checks the scheme of the source and of every redirect hop,
  and the name of every hop. The dial checks each address the name resolves
  to, on the address it connects to (`GuardedHTTP`). A refused attempt is
  *not now* with cause `refused`, and its message is the rule's reason.
- The allowed local hosts are `download/allowed-local.json` beside the
  refused list, a host name and a note each, matched by exact name. An entry
  opens only the local network ranges of DL-G5. Loopback and every other
  DL-G4 class stay refused whatever the list names. The architect's ruling on
  this branch, 2026-11-22, verbatim: "Loopback is where the person's
  unauthenticated local services live (ComfyUI's API, engine ports, other
  apps' localhost servers), the main SSRF target. A same-user-writable list
  entry shouldn't open it, and the product shouldn't carry a test-shaped
  exception." A test mirror listens on the machine's own local-network
  address and is named there.
- Through a proxy, the proxy resolves names: the address rules apply to a
  host written as an address or naming this computer, and the refused list
  to the name. This is DL-G7's limitation.
- `AllowLoopbackForTesting` lets a test binary reach loopback fixtures,
  http without a digest to a loopback host included. It does nothing outside
  a test binary (`testing.Testing`), and the runtime never calls it.

## Network cost

`request.thrift` `Request.constraints` states when the executing service
may move the bytes. Its one member, `network`, is `unmetered` or `any`.
Decided in `research/conditions/DECISION.md`.

**[DL-N1] No constraint is `any`.** An absent `constraints`, an absent
`network` and `network: any` MUST be one request to a provider, and each
MUST prepare the job it prepared before the member existed.

**[DL-N2] `unmetered` requires `abstraction.download/network-cost@1`.** A
request with `network: unmetered` MUST require the one entry of
`network_cost_guarantees`. A provider MUST advertise it only while it owns a
cost source for its platform: Network List Manager (`INetworkCostManager`)
on Windows, NetworkManager's `Metered` property on Linux,
`nw_path_monitor` on macOS. A provider without one leaves it out of its
offer, so resolution refuses a caller requiring it as unmet and admission
refuses a submission requiring it. Prepared jobs carry the constraint in
their spec as `constraints.network` and the guarantee in the record's
`requires`, so a runner without a cost source takes no fetcher for them
(DL-R24).

**[DL-N3] The executor waits while the path is metered.** The executor MUST
evaluate the constraint before each source open, before each range request,
and on every cost-change notice from the platform. It MUST wait on that
notice and MUST NOT re-read the cost on a timer. Metered is a path the
platform reports as costed: a metered or data-limited connection, roaming,
over its data limit, or constrained. A path the platform reports no cost
for, such as while nothing is connected, is `unknown`, and the transfer
proceeds and meets the network as it is.

**[DL-N4] A metered notice stops a transfer at the proven boundary.** On a
metered notice during a transfer, the runner MUST sync and checkpoint the
bytes already written as ranges and MUST release the lease; the attempt
stays accepted and `pending`. The runner MUST NOT open a source while the
path stays metered. On the notice that it is unmetered, the service adopts the job
again, and the transfer resumes from the checkpoint with one `Range`
request per remaining gap under DL-R1 to DL-V5.

**[DL-N5] A waiting attempt names what it waits for.** The runner MUST write
`extensions["abstraction.download/waiting@1"]` (`waiting_extensions`), a
JSON string holding one word, `<constraint>:<state>`. This version writes
`network:metered` and `network:unavailable` (DL-N8). The word is written in
the same run that then releases the lease, so an observer can briefly read
it on a job still `running`. The key MUST be removed when the transfer moves
again and when the job ends. Progress, the checkpoint, the failure payloads
and the accepted guarantees are unchanged by it, and a waiting attempt
records no failure. Observation reports the word beside the job's state.

**[DL-N6] Waiting belongs to the accepted attempt.** The caller exiting
MUST leave the attempt waiting. `CancelWork` while waiting MUST end it
`cancelled` with no source opened. A waiting attempt MUST hold no lease and
no awake lease. A moving one holds its execution lease and requests wake under
the authenticated job owner's existing rule. The Resource Book owns that wake
lease; an absent or refused owner never becomes the runner's own wake grant.

**[DL-N7] Constraints are evaluated where the bytes move.** A delegated or
remote submission MUST carry `constraints` unchanged; the remote executor
evaluates its own network, and its `waiting@1` word MUST be relayed
verbatim. The submitting machine's network state MUST NOT be sent.

**[DL-N8] A cost source that has not answered holds constrained jobs, never
the store.** A service whose cost source cannot be opened when it starts,
such as a Linux runtime starting before NetworkManager answers, MUST reopen
its accepted jobs requiring `network-cost@1` (`abstraction-job/CONTRACT.md`
JOB-A15) and MUST ask for the source again with backoff. Until the source
answers, those jobs wait with the word `network:unavailable`, open no source
and record no failure, while other jobs proceed. The service MUST offer
`network-cost@1` to new submissions only while a source answers. On the
notice that the source answers, waiting jobs are evaluated under DL-N3: each
moves, or waits again with `network:metered`.

The same rule applies when an opened source stops answering. The runner MUST
stop active constrained transfers at a proven boundary and record
`network:unavailable`. It MUST withdraw the guarantee from new submissions
while the source reports unavailable. A Linux NetworkManager source observes
its bus-name owner; owner loss invalidates the last Metered value, and a new
owner's value is read before the source reports a cost again. An unreadable
replacement remains unavailable.

## The spec, exactly

The job module never looks inside a spec, so nothing underneath this module
can notice two implementations reading one differently. Each implementation
therefore ships a `specread` that prints the meaning it arrived at, and the
`spec-conformance.sh` maintainer check requires every registered
implementation's printout to be identical. This section is the whole of the
reading: a reader written against this page passes, and nothing here is
recoverable from the source.

### The fields

| field | meaning | absent |
|---|---|---|
| `artifact.digest` | `sha256:` + 64 lowercase hex | unknown, which matches nothing |
| `artifact.size` | bytes, decimal integer | `0`, meaning unknown |
| `sources[]` | where the bytes may be had, ordered | nothing to try |
| `sources[].scheme` | `https`, `file`, `smb`, … | — |
| `sources[].locator` | the address, in that scheme's own spelling | — |
| `sources[].priority` | integer, **lower is tried first**, may be negative | `0` |
| `sources[].attrs` | string→string, describes the source **to us** | empty |
| `sources[].headers` | string→string, sent **to the server** verbatim | empty |
| `sink.final` | the destination: where the bytes end up | — |
| `sink.partial` | where they accumulate first | the module invents one (DL-S16 to DL-S18) |
| `constraints.network` | `unmetered`: open sources only while the path is unmetered (DL-N3) | `any` |

`attrs` is an annotation bag in the sense OCI and Kubernetes use the word:
non-behavioural metadata, keyed by whoever wrote it, that this module
carries and does not act on. `attrs` and `headers` are two bags, and the
split is the contract: a key in `attrs` MUST NOT be sent to a server. When
they were one bag, a key nobody remembered to exclude went out as an HTTP
header, and no unit test in one language can see another language
classifying the same key differently, which is why the classification is
printed and compared.

**[DL-S1] Unknown keys are ignored, at every level.** A reader MUST ignore
a key it does not know, at every level of a spec. A spec is opaque payload
the module above extends, and a reader that refuses `group_id` cannot carry
a record ComfyUI wrote. The job record refuses an unknown field; that
difference is listed under [Divergences](#divergences).

**[DL-S2] A digest is read as its hex.** A reader MUST strip `sha256:` and
`sha256-` (how Ollama names its blobs), MUST fold case down and MUST trim
surrounding space.

**[DL-S3] An unreadable digest is empty.** Anything that is not exactly 64
hex characters after DL-S2 MUST read as empty, and an empty digest MUST NOT
compare equal to another.

**[DL-S24] A locator is compared byte for byte.** Two locators MUST be the
same source exactly when their bytes are the same. Splitting one source
into two costs at worst a second attempt at the same bytes, which DL-R28
catches by digest before the network is touched. Merging two locators that
name different resources fetches the wrong file, and a normalisation ladder
can only merge.

**[DL-S4] A path is absolute if it is absolute under either convention.** A
leading `/` or `\`, or an `X:` drive letter, MUST make a path absolute.
Absolute paths MUST be kept byte for byte as given; relative ones are
printed with forward slashes.

**[DL-S5] An absolute destination is honoured only where its convention
holds.** Windows spells one with a drive letter or a leading `\`, POSIX
with a leading `/`, and a machine MUST refuse the other. A foreign path
used as a destination is dangerous: `open("D:\models\x.gguf", O_CREAT)` on
Linux succeeds and makes a file of that literal name in the working
directory, with a `.part` beside it, and the runner creates the parent
first, so `/mnt/models` on Windows lands on whatever drive the process was
on.

**[DL-S6] A record carrying such a path stays valid.** It is correct on the
machine that wrote it, and a reader MUST NOT treat it as invalid.

**[DL-S7] The machine about to write decides.** The refusal of DL-S5 MUST be
made by the machine about to do the writing and MUST NOT be made at
submission. A record meant to be adopted by another machine names a
relative destination, the only portable form.

**[DL-S8] A relative destination that resolves outside the store root is
refused.** A reader MUST resolve first and ask where the answer landed;
containment is measured from the root, so one `..` climbs out of it wherever
it sits. Scanning the input for `..` fires on the contained `a/../b`. A
backslash in a relative path is a separator: `..\..\Startup\evil.bat` is a
climb, because a backslash cannot legally be in a Windows file name. The
resolved path is the root joined to the relative path, so it is compared
with the root exactly, folding no case on any host (VISION.md L623; DL-R31).
That lexical check is the backstop. When the root exists on a local drive,
the filesystem also decides (`abstraction-identity` `Within`, which
resolves the deepest existing folder above the path): a folder inside the
root that is a junction or link to a folder outside it carries the path out,
and the destination MUST be refused. A root that does not exist, or that is
not on a local drive, leaves the lexical check to decide; any other failure
to answer MUST refuse. A link made inside the root after this check and
before the write is outside this rule.

**[DL-S9] A relative destination that names the store's own layout is
refused.** A final of `jobs/<id>.json` overwrites a job record and a final
of `work/<other>` overwrites another job's partial, and both are contained
by DL-S8. The reserved set is the file store implementation's layout,
asked of the job module (`job.Reserved`) and never spelled here, plus the
names the removed runner program used. Measured from the store root, with
`.` and `..` resolved first:

| reserved | who owns it |
|---|---|
| `jobs`, and anything under it | the record, its claim tokens, its temporaries |
| `work`, and anything under it except `work/<this job's id>` | another job's scratch |
| `supervisor.json`, `supervisor.json.tmp`, `supervisor.sock` at the root | the heartbeat and bus of the runner program removed in 0.1.8; a store written by an earlier release may hold them |

**[DL-S10] A job's own `work/<id>` is free to it.** `work/<id>` and
everything below it MUST NOT be reserved against job `<id>`; that is where
its own partial goes.

**[DL-S11] Asked for no job, all of `work/` is reserved.** A reservation
asked on behalf of no job (an empty id, which is what a caller has before
an id exists) MUST reserve the whole of `work/`.

**[DL-S12] The reserved comparison folds case and trims, on every
platform.** It MUST fold case and trim a trailing dot or space from every
segment on every host. `Jobs/x.json` and `jobs./x.json` both open `jobs/`
on NTFS, and a record refused by Windows and accepted by Linux would mean
the refusal depends on who looked. Containment (DL-S8) folds no case: both
of its sides carry the root's spelling (DL-R31). This comparison is against
names the contract fixed.

**[DL-S13] A segment of only dots and spaces is left alone.** Such a segment
MUST be compared as written: it is a real name on POSIX and not one Windows
will open.

**[DL-S14] Absolute paths are outside the reserved comparison.** An absolute
path MUST NOT be compared against the reserved set. It is never joined onto
the root, so it names the store's contents only by a coincidence no lexical
rule could see.

DL-S8 to DL-S14 are lexical and claim nothing more. A symlink or a junction
inside the root that points at `jobs/` defeats them, and nothing here
resolves a path at the moment of the write.

### What `specread` prints

`specread <spec.json>` writes to **stdout**, in this order, one
`key=value` per line, and exits `0`:

```
digest=sha256:2fc237e65e1f963310e9c961d8e71e932734a72f90e3216a972d83edd1feb756
size=328597408
final=models/model.gguf
partial=work/abc
final_refusal=
partial_refusal=
source0=https|https://example.invalid/model.gguf
source0.attrs=credential=hf,store=ollama
source0.headers=X-Repo=openabstractions
```

- The first six keys are always present, in that order, even when empty.
- Then three lines per source, `source0` … `sourceN`, indexed from `0` in
  the order the sources would be tried.
- `sourceN=` is `scheme`, one `|`, `locator`.
- `digest` is the normalised form, `sha256:<hex>`, or empty.
- `final` and `partial` are the portable form of `sink.final` and
  `sink.partial`.
- `*_refusal` is empty when the path is acceptable. Otherwise it is the
  refusal text, and the path in it is quoted **as the spec spells it**,
  before the portable form is taken.
- A map prints as `k=v` pairs joined by `,`, keys sorted ascending by byte
  value, an empty map as the empty string. Values are raw, and a value
  containing `,` or `=` is out of contract.

**[DL-S15] Sources are tried in stable ascending priority.** The order MUST
be a stable sort on ascending `priority`, so equal priorities keep the order
the spec gave them. That order is behaviour: a local copy at `-100` is what
turns a download into a copy.

Every line ends in LF, including the last, and no CR appears anywhere. The
record pins this too, for the same reason: a CRLF build disagrees with every
other implementation about every line at once, and the diff shows nothing.
On Windows, Python needs `sys.stdout.reconfigure(newline="\n")` and C++ needs
the stream opened in binary mode; Go is already right.

`specread --partial <final> <id>` prints exactly one line and exits `0`:

```
partial=work/1757000000000-deadbeef
```

The partial name is the one field a caller may leave out, so the module
invents it, and a successor in another language finds a predecessor's bytes
only by inventing the same name.

**[DL-S16] A relative final's partial is `work/<id>`.** A `final` that is
relative under both conventions MUST give `work/<id>`, in the store's own
work directory, because the record must not name the machine that submitted
it.

**[DL-S17] Any other final's partial is `<final>.part`.** Any other `final`
MUST give `<final>.part`, beside the artifact. Delivery across volumes is a
copy, and a copy into the final name leaves a truncated file under the name
an application reads as an installed model.

**[DL-S18] An empty final counts as relative.** An empty `final` MUST give
`work/<id>`.

`specread --portable <path>` prints exactly one line and exits `0`:

```
portable=C:/Users/r/.cache/huggingface/hub/models--o--r/snapshots/41ba88db/x.gguf
```

**[DL-S20] A path in a record has one separator, `/`.** Every backslash MUST
become a forward slash, whatever wrote the path and whether it is relative or
absolute. A drive letter and a UNC root say Windows on their own, and Windows
accepts either separator in every path it is handed. Two spellings of one
destination do not compare equal, so a store holding both would fetch the
artifact twice.

**[DL-S21] A path rooted at a single `/` is returned byte for byte.** A
backslash is a legal character in a POSIX file name, and rewriting one would
name a different file.

**[DL-S22] A UNC root comes out as `//server/share`.** It is still an
absolute Windows path to `--foreign`.

**[DL-S23] The rewrite is stable.** A path already in this form MUST come
back unchanged. Comparing two paths is a wider normalisation (`.` and `..`
resolved, case folded where the filesystem folds it) applied to both sides at
the moment of the comparison, because records written before DL-S20 are on
disk and keep resolving.

`specread --reserved <owner-id> <path>` prints exactly one line and exits
`0`:

```
reserved=download: sink path is reserved by the store: jobs/1757000000001-cafebabe.json
```

It is empty when the path is free space. The owner id is an argument
because the answer depends on it: `work/<owner>` is the one reserved path
that job may write (DL-S10). A spec file names no job, which is why this is
an argument and not a fixture.

`specread --foreign <path>` prints exactly one line and exits `0`:

```
foreign=download: sink path names another platform's filesystem: /mnt/models/x.gguf
```

It is empty when this machine may write the path. This is the only line
whose answer depends on the host. The harness compares every implementation
running on the same host, and it requires that at least one of a
Windows-shaped and a POSIX-shaped absolute path is refused, so the check
cannot pass because nothing is ever refused.

### Refusing, and where a refusal goes

A contract refusal is a value: `final_refusal` and `partial_refusal`,
printed on stdout with exit status `0`. Its wording is part of the
comparison, because every implementation must refuse the same records for
the same stated reason, or a record one of them will not touch is quietly
acted on by another. This is the whole catalogue:

| when | exactly |
|---|---|
| a relative destination resolves outside the store root (DL-S8) | `download: sink path escapes the store root: <path as spelled in the spec>` |
| a relative destination names the store's own layout (DL-S9) | `download: sink path is reserved by the store: <path as spelled in the spec>` |
| an absolute destination is written in the other platform's convention (DL-S5) | `download: sink path names another platform's filesystem: <path as spelled in the spec>` |

The first two judge the record and are the same on every host, so they are
checked at submission and again by the machine that writes. The third
judges this machine and is checked only by the one about to write (DL-S7).

A tool failure belongs to the implementation. A missing file, bytes that
are not JSON, or wrong arguments go to **stderr**, with a non-zero exit
status, in whatever wording the implementation chooses. The harness reads
stdout only and requires exit `0`, so it can never mistake one for the
other.

### Whether a document is readable at all

**[DL-S19] Strictness is a cross-language decision.** Whether a spec is
readable at all MUST be decided identically by every implementation. A spec
one implementation reads and another refuses is a record the first writes
and the second cannot open, and the job stops at whichever host picks it
up. This holds when the strict reader is the safer one: refusing a
repeated key in one implementation of three converts a parser differential
into an availability split, after the record is already written.

**[DL-S25] A reader that builds a tree of a spec refuses nesting deeper
than 1000 levels.** Depth counts each object and array opened, the
document's own value being level 1, and an empty container counts like any
other. A reader that builds a document tree, whether a DOM parser or a
recursion whose stack grows with depth, MUST refuse at the first container
that would open level 1001, with a typed refusal and never a crash. It MUST
count explicitly and give the same outcome on every host, interpreter
version and toolchain, whatever its language's parser or recursion limit
would allow. A streaming reader with constant per-level stack and memory is
not bound by that number. A spec needs four levels. Inside a job record the
record's own lower limit applies (`abstraction-job/CONTRACT.md` JOB-J8, 64
levels).

`specread --echo <spec.json>` prints exactly one line and exits `0`:

```
echo={"artifact":{"digest":"sha256:…","size":1},"sources":[…],"sink":{…}}
```

It is the spec as this implementation would **carry** it inside a record,
compact. Go holds a spec as raw bytes; an implementation that holds it
parsed and re-emits it can respell a number or apply an escape policy on
the way through, and every reader downstream then sees changed bytes.
Whitespace is the record writer's choice and is compacted away here;
escapes and number spellings are compared.

The inputs in `testdata/verdicts/` are a corpus that only grows: any input
that has ever produced a disagreement stays in it, so a new implementation
meets every divergence closed before. `specread` with that corpus is
protobuf's `conformance_test_runner`: hand one payload to every
implementation, compare what each says it means, and never delete a case.
CommonMark's spec tests, `sqllogictest` and Wycheproof are the same
instrument aimed at a grammar, a query language and a crypto library.

A further implementation joins the comparison with one line, which need not
live in this repository:

```bash
SPECREAD_EXTRA='node=node /path/to/specread.js' bash scripts/spec-conformance.sh
```

One `name=command` per line registers several.

## The sweep and the drop folder

`Fetcher` and `Delegator` both run only when something calls them. A
delegated transfer that finishes while no application is open waits: BITS
releases the file only when someone calls `Complete()`, and nothing verifies
the digest until someone asks. The sweep closes that gap. It moves no bytes
itself: it reconciles delegated jobs, finalises and verifies the finished
ones, and adopts orphans. Reconcile runs before adopt, so the orphan pass
never picks up a job a delegate has already completed. The installed
runtime runs the sweep (`serve.Pass`) inside its download service whenever
its job store changes. The separate runner program that swept the earlier
job store, `jobd`, was removed in 0.1.8;
[HISTORY.md](HISTORY.md#the-sweep-before-jobd-was-removed) keeps its account
and the proof it gave.

### `wanted/`

A person with a share and no toolchain has a folder, so a sweep can watch
one: a text file put in `wanted/` inside the store is a request, and the
folder answers it by renaming the file. `download.Wanted` implements the
rules below, and the [`wanted`](testdata/scenarios/wanted.txt) scenario
judges them. No shipped program has watched `wanted/` since 0.1.8.

```
https://huggingface.co/HuggingFaceTB/SmolLM2-135M-Instruct-GGUF/resolve/main/smollm2-135m-instruct-q8_0.gguf
https://example.com/big.iso sha256:2fc237e6…feb756 isos/
```

One download per line: the URL, then in either order an optional
`sha256:<hex>` and an optional destination. The destination is relative to
the store; absent, it is `files/<the name in the URL>`; ending in `/`, it is
a directory and the name comes from the URL. A line beginning `#` is
ignored.

**[DL-W1] The line form is a convenience over the spec.** A file that begins
`{` MUST be read as a spec exactly as this page spells it, and a line MUST
mean the spec it abbreviates; the line form MUST NOT become a second
grammar.

What the folder shows is the whole interface:

| the file is | meaning |
|---|---|
| `x.txt` | waiting: nothing has looked, so no runner is watching or it has not swept yet |
| `x.txt.accepted` | taken; inside, the job ids, where each lands, and progress on every sweep |
| `x.txt.done` | every job delivered; inside, the path, the size, and the digest if one was verified |
| `x.txt.failed` | a job ended without delivering; inside, why |
| `x.txt.refused` | a request the runner will not act on; inside, which line and why |

**[DL-W2] A refused request is answered in place and its own text is
kept.** The answer MUST be appended as `#` lines, so renaming
`x.txt.refused` back to `x.txt` after fixing it is how to try again. A file
over 64 KiB MUST be renamed and left unread.

**[DL-W3] Only `http` and `https` are fetched for a dropped request.** The
folder is a wider door than a record, because anyone who can write to the
share can use it, so it accepts less. A dropped request MUST name an `http`
or `https` source; a `file:` source would copy the runner's own disk onto
the share. The destination MUST be inside the store, contained, outside the
store's own layout and outside the drop folder itself. A dropped request
MUST NOT carry a header or a credential name: a credential is resolved on
the fetching machine, and a request from a share could point it at any
host. Which hosts the runner opens a connection to at all is the `Reach`
interface's answer (DL-E8). Every refusal MUST be a line in the file, never
a crash and never silence.

**[DL-W4] A request is `.done` once every job it named has delivered.** Once
one of its jobs has ended without delivering, it MUST become `.failed`.
Until then it is `.accepted`, and its last lines are each job's state. The
runner takes delivery on the person's behalf, because nobody else will.

A hot folder is the oldest integration there is: printers, media encoders
and mail transfer agents take work this way, because a file is the one
thing every person and every program can write.

### The delegate channel

A `Delegator` hands a job to something that will still be working after this
process exits. The `nas` delegate hands it to another runner, over a store
both machines can open. This section collects the rules already declared
above that bind each party, so a second implementer can write either half.
It declares no rule of its own.

**The channel is a shared record.** The requester opens the remote store as
a `job` file store (a UNC path, a mapped drive, a mounted share), submits a
record into it, and reads that record back. The delegate is a runner
sweeping the same directory through its own filesystem. Neither side opens
a connection to the other, and no message format exists beyond the record
the job module already defines. `go/nas/nas.go` imports `os`, `path`,
`path/filepath` and the two modules, and reaches the far side only through
`job.Store`.

Two functions in the same package are configuration. `Find`
(`go/nas/find.go`) sends SSDP and mDNS multicast to look for a host that
might hold a store, and `Check` writes and re-reads one probe byte to prove
the share is writable. Both run when a person is choosing a store, carry no
job and no record, and a configured delegate calls neither. A delegate
learns of new jobs from its own sweep alone.

**What the requester writes.** A record of kind `download`, submitted with
a fresh id in the remote store, holding a spec that differs from the local
one in two places. `sink.final` is rewritten to
`<dir>/<basename of the local final>`, relative and slash-separated, so the
far side resolves it against its own view of the root (DL-R20). `spec.request`
carries the local record's id, which is how a handoff whose answer was lost
is found again by reading the remote records' own specs. The local record
keeps `delegation = {system, external_id}`, where `external_id` is the
remote record's id, and its `progress` becomes a cache of what the remote
record last said.

**What the delegate obeys.** It is a runner over a store several machines
can write, so every rule for that position applies unchanged:

- A relative destination resolves against the delegate's own root (DL-R20);
  an absolute one is refused outright by a runner in this position (DL-R30)
  and by any machine whose convention it is not written in (DL-R21, DL-S5
  to DL-S7).
- A destination that resolves outside the root is refused (DL-S8), and one
  that names the store's own layout is refused (DL-R22, DL-S9), with
  `work/<id>` the one path the job may write (DL-R23, DL-S10, DL-S11),
  compared by the rules that do not vary with the host (DL-S12 to DL-S14).
- A credential named in a record is resolved on the fetching machine and
  refused for a host it is not bound to (DL-R29); a host this machine's
  policy will not open is *not now* (DL-E8). Both are the delegate's own
  answers.
- Bytes its own store has already proven are used before any source is
  opened (DL-R28). On this channel that matters most, because the
  delegate's earlier deliveries are files the requester cannot see and the
  record cannot name.
- Intent is honoured (DL-R25 to DL-R27); a failure is recorded with its
  class (DL-E1 to DL-E3, DL-E14); the digest decides (DL-R15, DL-R16); and
  it runs the same transfer rules as any other runner, DL-R1 to DL-R14.

**What the requester obeys.** It verifies what the delegate delivered
(DL-R17), because the bytes crossed a network twice. It returns a job whose
handle no longer resolves to `pending` with its sources and checkpoint
intact (DL-R18, DL-R19). It publishes the dispatch rule's answer in place
of the head of the chain, because a delegate is reached only through a
runner (DL-R33).

**What stays with the requester.** A resume offset stays local: the proven
bytes are on the requester's disk, the delegate has its own store and its
own partial, and an offset for a file the far side does not hold produces
the right length and the wrong contents. The `nas` delegate therefore
declines `resume` as a capability, so a locally interrupted job stays
local. The checkpoint stays local for the same reason.

**How this compares with the job service.** The job module's `RemoteStore`
and `Serve` carry the same `Store` semantics over a socket, with a
generated `Request`/`Response` envelope, a `Verdict` enum, and one
newline-terminated JSON exchange per connection. This channel has a file for
framing, no envelope and no operation field: the operation is implied by
which part of the record changed, and a refusal is a local error in the
requester's own process. What the two share is the record schema, the
lease, the epoch, and polling as the way either side learns something
changed.

**A record written from two hosts.** Both sides write this record: the
requester submits it, sets intent on it without a lease to pause or cancel,
and claims it to acknowledge or abandon; the delegate claims it,
checkpoints it and ends it. The lease and its epoch make one writer
authoritative at a time and are the whole of the mutual exclusion here. The
file lock underneath them is unshared, because a byte-range lock taken over
SMB and a `flock` taken on the server's own volume do not meet. Measured in
`abstraction-cas/README.md`: writers on two hosts on one file lost or
refused 147–149 of 2150 updates across three runs, while six writers in
three languages on one host lost none of 1800. The job module's own page
states the consequence, that a record written from two hosts needs a
protocol that store does not have, and this channel is that case. How often
it bites here, where the two sides write at different moments in a job's
life, is unmeasured. Since 0.8.0 the Go `job` file store refuses a root on a
share when it opens (`abstraction-cas` `CONTRACT.md` CAS-L2), so `nas.New`
and `Check` refuse such a store with that reason, and a runtime configured
with one stops at start and pins no provider. The channel waits for a
requester that reaches the far runner through a service.

Obligations the implementation places on a delegate that no id states yet
are listed under [Not built](#not-built).

## Outcomes

A failed transfer ends one of two ways. This is **retry classification**,
as old as SMTP: RFC 5321 §4.2.1's `4yz` transient against `5yz` permanent,
BITS' `BG_JOB_STATE_TRANSIENT_ERROR` against `BG_JOB_STATE_ERROR`, Go's
`net.Error.Temporary()`, a soft bounce against a hard one. *Not now* and
*no* are those two words.

| ending | what it means | what the record does |
|---|---|---|
| **not now** | a dropped connection, a full disk, a NAS that rebooted | keeps its error, the lease lapses, the next runner resumes from the last proven byte (DL-E1) |
| **no** | the request as written can never work | `failed`, and nothing tries it again (DL-E2) |

**[DL-E1] *Not now* keeps the job adoptable.** A *not now* failure MUST keep
its error on the record and let the lease lapse, and the next runner MUST
resume from the last proven byte.

**[DL-E2] *No* ends the job `failed`.** A *no* failure MUST end the record
`failed`, and nothing MUST try it again. A `404` classed as *not now* is
re-fetched on every sweep forever and nothing waiting on the record can
stop waiting; a dropped connection classed as *no* throws away the case
this project exists for.

**[DL-E3] The class is attached where the error is defined.** An
implementation MUST attach the class to each error where that error is
defined and MUST NOT keep a list. A list is a second place to update, and
two lists in two languages disagreed by a row for as long as both existed.
Go marks a sentinel by constructing it with `forever`; a further
implementation uses whatever its language already has for "this error is
one of these": an enum tag, a marker trait, a field.

**[DL-E4] What is *no*.** The following MUST be *no*: the source refused;
nothing registered can serve the job's sources; the destination escapes
the store root; the destination names the store's own layout; the job
module says the record is invalid.

**[DL-E5] Everything else is *not now*.** Every other failure MUST be *not
now*, including a destination written in the other platform's convention:
that path is unusable here and usable on the machine whose convention it is.

Over HTTP, **the source refused** is exactly these statuses:

```
400 401 402 403 404 405 406 410 414 451
```

**[DL-E6] The refusal statuses are listed, never ranged.** A 4xx outside
that list MUST be *not now*. This departs from RFC 9110 §15 (see
[Divergences](#divergences); the measurement is in
[HISTORY.md](HISTORY.md#dl-e6)). `401` is on the list because a gated
repository answers it until a person adds a token, and adding a token is a
new request; `400` is there because a signature that has expired is not
fixed by asking again. [`wire-refusal-status`](testdata/scenarios/wire-refusal-status.txt)
and [`wire-notnow-status`](testdata/scenarios/wire-notnow-status.txt)
compare each implementation's classification of a status against the
others'.

**[DL-E7] `416` and `412` restart cleanly.** They say the resume offset or
the file version is wrong, and the runner MUST restart the transfer from
zero. `408`, `425` and `429` say try later; `409` and `423` are somebody
else's lock; all five are *not now*.

**[DL-E8] A host this machine will not reach is *not now*.** Before a source
is handed to a fetcher, at the same last moment a credential is resolved,
the runner MUST ask its `Reach` interface about the host the source would
open a connection to, and MUST record a refusal with the reason the policy
gave, so an application can show it in its own words. The job stays
adoptable: the refusal is this machine's policy, and the same record runs
unchanged on a machine that may reach the host, or here once a person turns
it back on. What implements `Reach` is the machine's choice: `Discover`
wires the hosts a person switched off, kept in `download/refused.json` under
the user settings directory; a bare runner reaches everything.
The download service asks the same list of the account it runs as at every
hop: by host name, subdomains included, before every connection, redirect
and resumed range, and by each address the name resolves to, checked on the
address the connection dials. Through a proxy the proxy resolves names, and
the list applies by name only. Its admission MUST refuse a request whose
every source host the list refuses `unavailable`, with the reason this rule
records, before anything is journaled; a request with one reachable source
is admitted and the refused sources are skipped. A refused attempt reports
cause `refused` and names the reason. The ruling is design B of
`decisions/RULINGS-2026-11-22-owner-designs.md`, rule 6.

**[DL-E14] The class reaches the caller.** A record MUST carry the reason an
attempt ended and its class, and the error an application receives when it
waits on that job MUST carry the class too. Rebuilding the error from the
record's sentence alone destroys the class one call before the caller sees
it, and the caller can branch on *not now* against *no* only through the
API. A reader that cannot read the class sees an unclassed failure and MUST
treat it as *not now*, the safe half.

**[DL-E16] The class crosses a process, a provider and a language in one
shape.** It MUST ride in `extensions["abstraction.download/failure@1"]`,
defined once in [download.thrift](download.thrift) as `error` and
`permanent`, with `permanent` written only when it is true, so a *not now*
is the shorter document. A valid payload establishes that a failure is
present even when its `error` text and the record's diagnostic are empty;
diagnostic text MUST NOT determine the presence or class of a failure. The
key is spelled as a content name because it is one: it appears in `content`
for exactly as long as the payload does, and a driver's `--models` roster
names it like any other name its implementation writes.

- **The key is the version.** An incompatible change to the payload MUST be
  a new key; this one is never edited, which is what lets the payload refuse
  a field it has never heard of.
- **An unreadable payload is an absent payload.** For an unknown field, a
  missing `error`, or a `permanent` that is not a boolean, the reader MUST
  fall back to the record's sentence, and the job is *not now*. A reader
  that kept the half it understood would be inventing the other half.
- **The payload and its declaration belong to the library.** An application
  asks a failed job what it means and is given a classed error; it MUST NOT
  add, remove or compare `extensions` or `content`. `content` is derived by
  the job module from what the record carries on every write, and extension
  keys are appended to it sorted, by the job page's rules.
- **Both modes, one meaning.** A store this process owns and a store it
  reaches over a socket carry the same record and MUST recover the same
  class. Service availability may change coordination and isolation and
  MUST NOT change what a failure means.

**[DL-E17] A typed cause rides under its own key.**
`extensions["abstraction.download/failure@2"]` carries the failure@1 shape
plus `cause`, a member of abstraction.job `FailureCause` other than
`result_lost`. `credential` is a named credential the service
could not apply, and the error carries `credential:<outcome>:<name>` with the
applier's outcome. An empty cause is unreported. Only a runner whose owner
opts in MUST write failure@2, always beside failure@1 in the same write, and
failure@1 MUST NOT carry `cause`. The class is failure@1's `permanent`, and a
cause MUST NOT decide it. A reader that knows only failure@1 keeps the class,
and records written without the opt-in are byte-identical to before. A
download service's execution profile MAY end failures beyond this module's
refusals when its own contract names them: `download-http-request-v1` ends
`digest_mismatch` and `oversize` as permanent, so its record is `failed` and
says permanent under both keys. An ending is a property of the bytes: a
digest a delegate's delivery fails ends the job exactly as one this process
fetched itself does, under the same profile.

**[DL-E18] A record written before the key keeps its meaning.** Such a
record carries a sentence and no class. It MUST load, its `error` MUST stay
intact, and it is *not now*, the answer every reader gave before the key
existed. A reader MUST NOT infer a class from the sentence, and MUST NOT
infer one from `state`: `failed` is where a *no* leaves a record (DL-E2), and
a record can also be `failed` because a person cancelled a doomed job by
hand.

**[DL-E15] An authority that is not a plain host name is *not now*.** A
plain host name is a non-empty run of `a-z 0-9 . - _ :` after folding, with
one trailing `.` dropped. A locator MUST be refused, as *not now*, when its
authority carries userinfo (empty or not), a byte above `0x7F`, a percent
escape, a backslash, a space or a control character, and when an `http` or
`https` locator names no host at all. The job stays adoptable: the same
record with the host spelled plainly runs here unchanged.

Where a locator's host decides something (whether a credential may be
attached, DL-R29; whether a host is reachable, DL-E8), the host MUST be the
one the transport will open, read by the parser the transport itself uses:
`net/url` in Go, the one implementation. It is folded to lower case
per RFC 3986 §3.2.2 and §6.2.2.1 and compared with the port and the brackets
of an IPv6 literal removed. A host is merged where a locator is split
(DL-S24), for the opposite reason: merging two spellings of one host can
only make a refusal list match more often and bind a credential to the
origin it was already going to, and splitting one host into two is what
lets a refusal be walked around. No IDNA table is carried, and an
internationalised name is refused: transports disagree on IDNA (Go's
applies it and dials the A-label), and the only table that could reconcile
them is a copy of Unicode that would go stale inside a shipped binary
([HISTORY.md](HISTORY.md#dl-e15) keeps the full account). An adopter
names the host in punycode; `xn--n3h.example` is accepted and was measured
identical to its dialled form. One authority is still read by hand,
`\\server\share`: it is no URI, and no URI parser accepts one.

### Exit status

A process boundary is the narrowest one this module crosses. The record
carries a class in a field; a delegate's failure crosses as
`Failure{error, permanent}`, because `errors.Is` does not survive JSON and a
sentence is not a class; and the same class reaches a shell as one byte, by
the numbers below. A command-line conformance corpus is written against
that byte.

**[DL-E9] The status is the class, never the cause.** A command MUST exit
with the number its class names. These are the three answers of
[`abstraction-job/SPEC.md` § 6](https://github.com/openabstractions/abstraction-job/blob/main/SPEC.md),
plus the two a command line needs.

| status | name | § 6 answer | what the caller should do |
|---|---|---|---|
| `0` | done | — | take the result |
| `1` | not now | unavailable | run the same command again, later |
| `2` | misuse | — | fix the command line; **nothing was attempted** |
| `3` | no | forbidden | change the request or stop; running it again is pointless |
| `4` | unknown | unknown | find out before deciding; the job may have happened |
| `130` | interrupted | — | `128 + SIGINT`, POSIX |

The cause goes to stderr, in the implementation's own wording, and a
caller that parses stderr to recover the cause is doing what this
vocabulary exists to stop. `curl` publishes about ninety-nine codes naming
causes and no class, so a caller must keep a table mapping them onto
retryable and permanent, a second place to update that drifts (DL-E3).
`0` and `2` are conceded: every shell reads `0` as success, and `2` is a
usage error in `wget`, in bash's builtins, and in this module's own
commands. `3` is `dlc`'s own, arrived at independently of the Go tool. `1`
is *not now* because the generic slot is what an unfamiliar failure gets
treated as, and a *not now* costs one more sweep where a *no* throws the
download away (DL-E6). `4` is *unknown* and MUST NOT be folded into `1`:
`ErrOutcomeUnknown` means a delegate may still hold the job, so retrying is
unsafe and giving up is wrong.

**[DL-E10] The numbers are this module's, and `sysexits.h` maps onto
them.** BSD's taxonomy is the same: `EX_TEMPFAIL` (75) is *not now*,
`EX_UNAVAILABLE` (69) and `EX_NOPERM` (77) are shades of *no*, `EX_USAGE`
(64) is misuse. A command MUST use the numbers in DL-E9's table; see
[Divergences](#divergences).

**[DL-E11] The status is about this invocation.** A command MUST exit with
the class of its own run, whatever the jobs it observed ended as. A sweep
is one invocation over many records: when a record in the store has failed
permanently, the sweep succeeded, printed the problem and exits `0`. A
scheduled task reporting failure because one of a hundred records is a
`404` teaches an operator to ignore it.

**[DL-E12] A contract refusal stays a value.** `specread` MUST print
`final_refusal` on stdout and exit `0`: it judges a record, identically on
every host. The exit status judges one run of one program.

**[DL-E13] What a status byte cannot carry is said.** A full disk and a
rebooted NAS are both `1`: the class fits, the cause does not. A resumable
transfer that moved 39 of 40 GB and stopped is `1`, the same as one that
moved nothing; progress is in the record (`openabstractions jobs show`).
*Started, still running* has no code: `over_curl` has a fifth answer,
`working`, and `modelget get --background` exits `0` having delivered
nothing, where `0` means the job is in the store. A command that may exit
before the job is done MUST say so in its own help, and its `0` is a
different `0` from this vocabulary's.

## Bounds

| what | bound | rule |
|---|---|---|
| lease TTL before a killed runtime's job is adopted | 30 s plus one sweep | DL-R35 |
| spec nesting a tree-building reader accepts | 1000 levels | DL-S25 |
| nesting inside a job record | 64 levels (JOB-J8) | DL-S25 |
| a dropped request file that is read | at most 64 KiB | DL-W2 |
| a spec needs | 4 levels | DL-S25 |
| resume and range offsets | 64-bit (`int64`) | DL-R1 |

Implementations and what each claims:

| implementation | shape | schemes | resume | survives process exit | status |
|---|---|---|---|---|---|
| `HTTP` | Fetcher | `http`, `https` | yes, `Range` | **no** | working; the runtime runs it |
| `File` | Fetcher | `file`, `smb` | yes, seek | **no** | in the Go package; the runtime does not run it |
| `bits` | Delegator | `http`, `https`, `smb` | yes | **yes** | in the Go package, 12 tests against real BITS; the runtime does not run it |
| `nas` | Delegator | `http`, `https` | **no**: declines `resume`; the far side resumes its own partials | **yes** | the runtime runs it over a store on its own host's volume; a share root is refused (CAS-L2, `docs/REMOVED.md` entry 14), and the Synology verification predates that refusal |

A caller asks for bytes, and the runner offers the job to whatever is
registered and capable (DL-R24, DL-R33); registration comes from
configuration. A NAS outranks BITS because it is always on; BITS outranks
in-process because it survives this process exiting. No argument anywhere
names a hop; see [`addon-synology`](https://github.com/openabstractions/addon-synology).

## Divergences

- **DL-R1 to DL-R14, the range arithmetic.** A checkpoint's proven ranges
  are half-open; an HTTP byte range is inclusive at both ends (RFC 9110
  §14.1.2). `[40, 48)` goes out as `bytes=40-47`, and `bytes 40-47/64`
  comes back as `[40, 48)`. The divergence and its cost are declared in
  [`abstraction-job/CONTRACT.md`](https://github.com/openabstractions/abstraction-job/blob/main/CONTRACT.md).
- **DL-E6.** RFC 9110 §15 requires a client meeting an unrecognised status
  code to treat it as the `x00` of its class, so an unknown 4xx would behave
  as `400`, which is on the refusal list. This module treats it as *not
  now*. An unfamiliar 4xx in front of a large file is far more often an
  intermediary than an origin refusing the artifact (`499` is nginx closing
  on its own client, `440` an IIS session timing out), and a *no* throws the
  download away where a *not now* costs one more sweep.
- **DL-E10.** `sysexits.h` numbers 64 to 78 are refused: they collide with
  nothing and communicate nothing, `2` for misuse already shipped in two of
  this module's commands, and sysexits' own manual page has disclaimed
  universal use for thirty years. The distinctions are taken; the numbers
  are this module's.
- **DL-E15.** RFC 3986 §3.2.1 permits userinfo, and WHATWG URL repairs an
  authority where this module refuses it. Measured over 56 locators against
  three parsers and the address each transport dialled, 22 rows had at
  least two answers: `https://hf.co\@evil.com/x` is `hf.co` to a
  hand-written scan and `evil.com` to `WinHttpCrackUrl`, so a credential
  bound to `hf.co` was attached to a request whose socket opened elsewhere.
  WHATWG URL's repairs are what make it unsafe here: `urllib.parse` deletes
  tabs and newlines from an authority before reading it, so
  `hf.co<TAB>.evil.com` becomes a plain host that appears nowhere in the
  locator. The full account is in [HISTORY.md](HISTORY.md#dl-e15).
- **DL-S24.** RFC 3986 §6 defines URI equivalence as a ladder of
  normalisations and WHATWG URL defines a serialisation; this module climbs
  neither for a locator until an adopter shows the case that needs it.
- **DL-S1.** A spec ignores an unknown key; the job record
  ([abstraction-job's contract](https://github.com/openabstractions/abstraction-job/blob/main/CONTRACT.md))
  refuses one. The record is a contract every language shares, and a spec
  is opaque payload the module above extends.
- **DL-R22, DL-R23: authority is designation.** Local IPC is normally
  secured by what the platform says about the other end of a socket
  (`SO_PEERCRED`, macOS `LOCAL_PEERTOKEN` with a code-signing requirement,
  `GetNamedPipeClientProcessId`), and this module asks for all of them
  through `abstraction-identity`. Their answer MAY refuse a caller and MUST
  NOT authorise one, because what a platform says about the other end of a
  local socket can name the wrong process. What carries rights is a
  designation, the job id: holding one lets a process work that job and
  nothing else, and the store gives it exactly one path it may write, its
  own `work/<id>`. This is designation in Dennis and Van Horn's 1966 sense;
  the failure it avoids is the confused deputy (Hardy, 1988), the same shape
  `job.Reserved` guards against in a filesystem. The macOS measurement
  behind it is in [HISTORY.md](HISTORY.md#authority-is-designation); the
  Linux and Windows equivalents were not probed, and the divergence applies
  on all three.

## Not built

- **The remote form.** An application reaching this service on another
  machine's runtime has no binding; the delegate channel between runtimes is
  no application-facing service. When it is built, a remote receiver MUST
  refuse every request that needs a person with the outcome `forbidden`
  (the submission refusal of DL-K1) and the reason "no person reachable on
  this runtime", until remote questions exist. A forwarded
  caller is the forwarding runtime's certificate subject; the agent behind
  it is an unverified label.

- **DL-R37's runtime activation and placement.** The explicit pass exists
  behind the existing Job journal owner, with no scheduled pass in 0.8.0.
  Activation is a separate 0.9.0 change after installed/data-loss acceptance.
  The runtime still places no `tmp/` directory and has no low-space trigger
  (`research/storage-classes/DECISION.md` §4, §8). Caller-named destinations,
  migrated work and delegated/NAS storage remain outside this collector.
- **The destination at the wire.** Prose says destination; the spec
  document, the Go type `Sink` and the refusal texts keep `sink` until the
  release [Reading this page](#reading-this-page)'s table names.
  `request.thrift` carries no destination field today: the service owns
  result allocation (DL-R36).
- **DL-E9 in every command.** `openabstractions download` exits with the
  runtime's job codes (`serve/jobs.go`): `1` no runtime resolved or a
  transport failure, `3` refused, `4` unavailable, `5` ended failed or
  cancelled, `6` unknown, `7` still waiting. DL-E9 has `1` for *not now*
  and `4` for *unknown*, and DL-E10 requires its numbers. This is a code
  defect held open by the runtime's `RUN-E2` (`serve/CONTRACT.md`): CQ-6
  writes one exit-code table for the binary, and this table and the
  command's codes converge on it (TODO, W0-03 repair rows). `modelget`
  speaks DL-E9's table as of 2026-09-08. The example `dlc`
  (`abstraction-download-over-curl/example/dlc.cpp`) still exits `1` for
  every non-delivered answer; its `3` for `list` and `watch` is already
  right. `jobd` and `jobctl` spoke the table until they were removed in
  0.1.8, and `dl`, removed with them, never distinguished `1` from `3`.
- **Delegate obligations no id states yet.** The implementation places these
  on a delegate, and a second implementer meets them here:
  - The delegate is a `job` file store implementation at the same root,
    holding byte 0 of `<id>.json.lock` for every write and staging through a
    temporary file it renames. A delegate that locks differently passes its
    own tests and loses updates against this one;
    `abstraction-cas/README.md` § *What a fourth implementation must do* is
    the agreement.
  - `transferred` is the delegate's last state. The requester writes
    `complete` when it has taken delivery; a delegate that acknowledges its
    own job removes the state that says finished and not yet collected.
  - `spec.request` must survive on the remote record. It is absent from
    [The fields](#the-fields), and a reader that dropped it would make a
    lost handoff unrecoverable.
  - `requires` stays off the remote record, so the delegate's own placement
    constraints are its machine's and not the submitter's (DL-R24).
  - Abandoning removes files inside the delegate's store: the job's own
    partial, and the final when no other unfinished record names it. No rule
    above says a requester may delete there.
  - `wanted/` is unreserved. `job.Reserved` covers `jobs/` and `work/`, and
    this module reserves `supervisor.json` and `supervisor.sock`; the drop
    folder is in neither list, so DL-R22 does not reach it.
  - A failure crosses with its class. The remote record carries it where
    DL-E16 says, the delegate's status report carries it beside the
    sentence, and the requester writes both onto the local record. A
    delegate that cannot tell the two endings apart reports *not now* and
    says so where it decides; reading a class out of the words is what
    DL-E14 forbids.
