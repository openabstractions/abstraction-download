# Contract

Every rule this layer states, each carrying a tag, in the order they were
decided. A conformance scenario cites the tag it tests on its `# expect` line,
and a citation that resolves to no rule here is a defect in one of the two.

[README.md](README.md) is the door — what this layer is, how to obtain it, one
example that runs. No rule on that page carries a tag.

## What the Runner guarantees

**It resumes from what was proven, not from what is on disk** [DL-R1]. The resume
point is the *smaller* of the checkpoint's `verified_prefix` and what the file
actually holds. Those differ after a crash — the record is written periodically,
so the file can run ahead of it, and a partial can also be truncated or missing.
The unproven tail is discarded, and the hash is rebuilt over the prefix that is
kept [DL-R2]. That last part is the cost of resuming honestly: a sequential read
of what you already have, at disk speed, instead of re-downloading it at network
speed.

**The file is the floor, and a partial is never deleted to make a record
true** [DL-R3]. A checkpoint claiming more than the file holds is cut down to
what is there — per range, when the checkpoint has holes — and the transfer
carries on from it [DL-R4]. Go and C++ deleted the partial and refused until
2026-09-06, on the argument that
a file that short had been edited by something outside this library and none of
it could be believed. The argument does not survive being asked what it buys: a
file of the *right* length that a second writer replaced is accepted without a
murmur, because length is the only witness there is and length says nothing about
content. Refusing the short case detects no class of corruption; it declines the
one case where the damage announced itself. What answers "we cannot trust these
bytes" is the digest, over the whole artifact, before anything is delivered.
Deleting a verified prefix to avoid a check that happens anyway is 40 GB fetched
twice, which is the complaint this layer exists to answer. See
[`resume-short-file`](testdata/scenarios/resume-short-file.txt).

**It never appends the answer to a Range request the server ignored** [DL-R5].
Ask for bytes from 40,000, get back `200` and the whole file from zero, append
it, and you have a file of plausible length and impossible content. `curl -C -`
will do exactly that. The body is a complete artifact, so the prefix is thrown
away and it is taken from zero [DL-R6]: the bytes already moved are wasted, the
download still arrives, and Chromium settles it the same way in
`components/download/internal/common/download_utils.cc`.

~~This page said "refuses" until 2026-09-06.~~ Go and C++ had both stopped
obeying that sentence and no scenario existed to notice; Python still obeyed it,
and lost a download that was going to succeed on every retry, because nothing
about the record changed between them. Proven in
`abstraction-download/go/runner_test.go` `TestRestartsWhenServerIgnoresRange`,
`abstraction-download/python/test_abstraction_download.py`
`test_restarts_at_zero_when_a_server_ignores_range`, and across all three in
[`wire-ignored-range`](testdata/scenarios/wire-ignored-range.txt).

**Mind the arithmetic at this boundary.** A checkpoint's proven ranges are
half-open; an HTTP byte range is inclusive at both ends (RFC 9110 §14.1.2). So
`[40, 48)` goes out as `bytes=40-47`, and `bytes 40-47/64` comes back as
`[40, 48)` — eight bytes, spelled two ways. The divergence is declared, with
what it costs, in [`abstraction-job/CONTRACT.md`](https://github.com/openabstractions/abstraction-job/blob/main/CONTRACT.md).

**A 206 is an answer to this request only if it names the offset the next byte
goes at** [DL-R7]. One rule, and it settles four questions that had four answers. A
`Content-Range` beginning somewhere else describes bytes that belong at an
offset nobody asked about — written where they were asked for, they put the
artifact's own content in the wrong place, which no length check and no
transport error can see. A `multipart/byteranges` body is worse than misplaced:
its boundary line and per-part headers are content this layer would author into
the artifact itself. RFC 9110 permits a server to answer a single range that
way and a coalescing proxy does it, but we never send a multi-range request, so
it is never an answer to ours [DL-R8]. And a 206 answering a request that carried
no `Range` at all — which CDNs send — is acceptable exactly when it starts at
zero [DL-R9], because zero is where an unranged request writes.

An answer that fails this is not an error to record; it is a response to throw
away [DL-R10]. The prefix goes with it and the artifact is asked for whole, for the same
reason a server that ignores `Range` is handled that way: refusing instead loses
a download that the very next request would have completed, and asking again for
the same range would get the same unusable answer on every sweep forever — which
is exactly what `416` did until 2026-09-06.

Proven across all three in
[`wire-wrong-offset`](testdata/scenarios/wire-wrong-offset.txt),
[`wire-multipart`](testdata/scenarios/wire-multipart.txt),
[`wire-marked-multipart`](testdata/scenarios/wire-marked-multipart.txt) — the
same MIME body under a `Content-Range` that is correct, so the media type is
the only thing left standing — and
[`wire-unasked-206`](testdata/scenarios/wire-unasked-206.txt), where Go refused
a download the other two completed.

**Every request asks for `identity`, and a body that comes back under another
content coding is refused** [DL-R11]. A digest is over the artifact, and a coding is not
the artifact's: silently decoding changes what "the bytes" are. A request that
sends no `Accept-Encoding` has accepted every coding, so a gzipped answer is
legal — and the answer then depends on which language's transport is underneath.
Go's `net/http` decoded it and completed, Python hashed the envelope, C++ failed
a third way, all three reading one legal response. Setting the header explicitly
is also what turns off Go's transparent decompression, so this is the rule and
its enforcement in one line. It is *not now*, not a refusal [DL-R12]: a mirror,
or the same server tomorrow, may serve the identity that was asked for. See
[`wire-encoded-body`](testdata/scenarios/wire-encoded-body.txt).

**Bytes a run proved are written down before the failure is** [DL-R13]. The periodic
checkpoint fires on a byte count or an interval, so a transfer that stops before
the first one has proven nothing as far as the store is concerned — and the next
owner re-fetches bytes sitting on the disk in front of it. Go and C++ both did
that; Python did not, and Python was right. On a 64-byte scenario the difference
is 32 bytes; on a 40 GB model over a link that drops every ten minutes it is the
whole product, and "no 40 GB fetched twice" is what this layer is for. It is
written upwards only: `done` is the total of every proven range and a stop
offers a prefix, so a checkpoint that already knows more keeps what it
knows [DL-R14].
See [`wire-truncated-body`](testdata/scenarios/wire-truncated-body.txt),
[`wire-short-range`](testdata/scenarios/wire-short-range.txt) and
[`wire-undeclared-length`](testdata/scenarios/wire-undeclared-length.txt).

**It refuses bytes that do not match their digest** [DL-R15], deletes the partial
rather than leaving known-bad bytes for the next runner to resume onto [DL-R16],
and records why in the job so a human can read it without finding the log of a
process that no longer exists.

**It verifies what a delegate delivered** [DL-R17]. BITS "guarantees that the version of
the file it transfers is consistent based on the file size and time stamp, not
content" — so a delegate reporting success is not evidence the file is right.
After `Finalize`, this layer hashes the delivered file itself and refuses it on a
mismatch.

**It survives the delegate disappearing** [DL-R18]. BITS reaps jobs after 90
days, its queue database gets discarded wholesale when corrupt, and machines get
rebuilt. A handle that no longer resolves returns the job to `pending` with its
sources and checkpoint intact [DL-R19], so an ordinary in-process run can finish
it.

**It does not fetch bytes its own store has already proven** [DL-R28]. Before
any source the record names is opened, the runner looks in its store for a
finished record with the same digest whose file is still there at the size it
proved, and copies from that file first. A digest is an identity, so whose
destination that file was does not matter: a caller's earlier one, or a
delegate's own `files/`, which the submitter cannot see and the record cannot
name. On 2026-09-06 the NAS fetched 386 MB from Hugging Face twice over a file
it already held, digest verified both times. See
[`already-here`](testdata/scenarios/already-here.txt).

**Two paths, one file: it asks the filesystem, never the spelling** [DL-R31].
Whether two destinations name one file is a question about the volume holding
them. An implementation MUST answer it by asking that volume, and MUST NOT
decide it from the bytes of the path or from the name of the host operating
system.

Every platform furnishes this and we take theirs: `os.SameFile` in Go,
`os.path.samefile` / `os.path.samestat` in Python, `std::filesystem::equivalent`
in C++17, `java.nio.file.Files.isSameFile` in Java — whose documentation says
outright that it may open both files. All four compare device and inode, or
volume serial and file id.

The reason is measurable on any volume, and
[`go/samepath_test.go`](go/samepath_test.go) is the reproducer: it names the
pairs the three implementations were seen disagreeing about — `café` against
`CAFÉ`, `K` (U+212A) against `k`, NFC against NFD — and states no expected
answer, because `TestSamePathAgreesWithTheVolume` creates one name and looks for
the other on the volume under the test. NTFS folds `café` against `CAFÉ` and
holds `K` apart from `k`; APFS folds both; ext4 folds neither; one volume can be
mounted differently from the next, so a case-sensitive APFS volume, a
`mount -o casefold` ext4 and an SMB share each falsify any rule read off the
host's name. Two records for one file
are two leases, two partials and two writers racing into it. One record for two
files hands the caller a job fetching a **different** file and says nothing.

A destination usually does not exist yet, which none of those four can answer.
**Its parent does**, so an implementation settles the parent the same way and
settles the final component by asking that directory to demonstrate its own
rule — creating a name it owns and looking for the other spelling of it. It MUST
NOT create either destination to find out, and MUST leave nothing behind.

Two things are outside this rule. **Containment** — whether a resolved path
stays under the store root — is lexical and stays lexical, for the reason given
under [DL-S8]; both sides are folded alike and neither may exist. And **a
destination key**, the string a record id is derived from before any record
exists, cannot be a question to a volume at all: it is derivable from the path
alone, so an implementation that keys on one MUST first scan by this rule, and
the key is then a race window rather than the decision.

**It works on a record another machine wrote** [DL-R20]. Sink paths are relative
to the store root unless you say otherwise, and each machine resolves them against its
own view of it. A record written by Windows into `\\nas\models\store` is the same
record a container reading `/store` acts on — which is the entire NAS story, with
no NAS-specific code anywhere. An absolute path is left alone by the machine
whose convention it is written in and **refused by any other** [DL-R21], rather
than turning into a file literally named `D:\models\x.gguf` in whatever directory the
process happened to be in. A relative sink is the portable one; see
[The spec, exactly](#the-spec-exactly).

**A supervisor over a store several machines can write refuses an absolute
sink** [DL-R30]. An absolute path is never joined onto the root, so containment
never sees it, and a path in this machine's own convention is not foreign —
`/etc/cron.d/x` on the NAS passes every lexical check and is written with the
supervisor's authority, where a record somebody else wrote chose. A relative
sink is the portable, contained form and the only one such a supervisor
accepts; an absolute sink is legitimate only for a caller writing to its own
machine, which `dl -o` is and an adopted record never is. The refusal is *not
now*: the record is valid where it was written. See
[`deputy-absolute-sink`](testdata/scenarios/deputy-absolute-sink.txt).

**A sink cannot name the store** [DL-R22]. Staying inside the store root is not
enough on its own — `jobs/<id>.json` and `work/<another id>` are both inside it,
and both would have been written. The store's own layout is unspellable as a
destination, and a job may write exactly one path the store owns: its own
`work/<id>` [DL-R23].

**It honours required capabilities** [DL-R24]. `requires` is a placement
constraint in the sense Kubernetes' `nodeSelector` and Nomad's `constraint` are,
and the capability names are declared features in WebDriver's and POSIX's sense.
A job that asks for a fetcher which
survives process exit is not quietly served by one that does not — the in-process
HTTP fetcher does not claim `survives_process_exit`, because it dies with its
caller. Bindings differ enormously; pretending otherwise lies to the caller on
the tier most people actually run.

**A capability nothing on the machine would perform is refused at submission,
not written down and failed later** [DL-R31]. Accepting it leaves a record in
the store forever for a guarantee nobody could have kept, and the store is what
an application lists to show a person their downloads. The refusal says which of
three things it is, because they are three different problems and only two are
the caller's: a word the roster does not offer is a caller's typo and is *no*;
nothing registered promising the capability is *no*; a delegate promising it
with nothing watching the store is ***not now***, because starting a supervisor
serves the identical request. An unknown capability name is never accepted and
ignored — a capability only ever grants, so an unknown one matches nothing and
the job would be refused later for the wrong reason.

**A refusal names the condition that was not met, never a condition that was**
[DL-R32]. Choosing a fetcher asks two questions — does anything serve this
scheme, and does anything serving it promise these capabilities — and reporting
the scheme for both told an application its URL scheme was wrong while the
fetcher for that scheme was registered and serving.

**One rule decides who performs a job, and what is published is that rule's
answer** [DL-R33]. A delegate is reached only through a supervisor, so on a
machine with nothing watching the store the delegation chain's head is a tier
the work will never touch. Naming it is worse than saying nothing: it is the one
string an adopter puts in a status bar, and it tells a person their download
survives closing the app when it does not. What can be published before a job
exists is a *prediction*, made by the same rule that dispatches and answered for
a job with no requirements and a sink anybody could resolve. What is true of one
job is read from that job's own record — its delegation, then its lease — and
never predicted from the machine's configuration.

**It honours what somebody asked for** [DL-R25]. The job layer's `intent` is
checked before any byte moves and again at every checkpoint, and the runner
converges on it: `cancel` writes `cancelled` [DL-R26], `pause` lets the lease go
so the record reads `pending` and nobody is shown an owner that has
stopped [DL-R27]. Checking on adoption is
not an optimisation — an owner that dies between a pause being asked for and the
pause being carried out leaves the record `running` under a lapsed lease, and
[a sweep offers that one back](https://github.com/openabstractions/abstraction-job/blob/main/CONTRACT.md) precisely so the next owner can
finish what the last one started. Either half alone is a defect: honouring
without sweeping makes such a record unreachable forever, sweeping without
honouring restarts a download seconds after a person stopped it.

### Two endings

A failed transfer ends one of two ways, and the difference is the whole of it.
This is **retry classification** and it is as old as SMTP: RFC 5321 §4.2.1's
`4yz` transient against `5yz` permanent, BITS'
`BG_JOB_STATE_TRANSIENT_ERROR` against `BG_JOB_STATE_ERROR`, Go's
`net.Error.Temporary()`, a soft bounce against a hard one. *Not now* and *no*
are those two words.

| ending | what it means | what the record does |
|---|---|---|
| **not now** | a dropped connection, a full disk, a NAS that rebooted | keeps its error, lease lapses, the next runner resumes from the last proven byte [DL-E1] |
| **no** | the request as written can never work | `failed`, and nothing tries it again [DL-E2] |

Getting it backwards costs in both directions: a `404` classed as *not now* is
re-fetched on every sweep forever and nothing waiting on the record can stop
waiting, and a dropped connection classed as *no* throws away the case this
project exists for.

**An implementation attaches the class to each error where that error is
defined, and never keeps a list** [DL-E3]. A list is a second place to update: this layer
kept one in Go and one in Python, they disagreed by a row for as long as both
existed, and neither language could see it because each only ever read its own.
Go marks a sentinel by constructing it with `forever`; Python derives the class
from `Permanent`; a fourth implementation picks whatever its language already
has for "this error is one of these" — an enum tag, a marker trait, a field.

**The class reaches the caller, not just the record** [DL-E14]. A record carries
the reason an attempt ended *and* its class, and the error an application
receives when it waits on that job carries the class too. Prose is not a class:
rebuilding the error from the record's sentence alone destroys it one call
before the caller sees it, and then the whole of *not now* against *no* is
unobservable through the API — which is the only place a caller can branch on
it. The class rides beside the sentence, and a reader that does not know how to
read it sees an unclassed failure and treats it as *not now*, which is the safe
half.

**The class crosses a process, a provider and a language in one shape, and what
it means does not depend on a service being up** [DL-E16]. It rides in
`extensions["abstraction.download/failure@1"]`, and that payload is defined
once, in [download.thrift](download.thrift), as `error` and `permanent` —
`permanent` written only when it is true, so a *not now* is the shorter
document. A valid payload establishes failure presence even when its `error`
text and the record's diagnostic are empty; diagnostic text does not determine
the presence or class of a failure. The key is spelled as a content name because it is one: it appears in
`content` for exactly as long as the payload does, and a driver's `--models`
roster names it like any other name its implementation writes. Four
consequences, and none of them optional:

- **The key is the version.** An incompatible change to the payload is a new
  key, never an edit to this one, which is what lets the payload refuse a field
  it has never heard of instead of granting it.
- **A payload a reader cannot make sense of is a payload that is not there.** An
  unknown field, a missing `error`, a `permanent` that is not a boolean: the
  reader falls back to the record's sentence and the job is *not now*. An
  unreadable class and an absent class are one answer, because a reader that
  kept the half it understood would be inventing the other half.
- **The payload and its declaration are the library's, never the
  application's.** An application asks a failed job what it means and is given
  a classed error; it does not add, remove or compare `extensions` or `content`.
  `content` is derived by the job layer from what the record carries on every
  write, and extension keys are appended to it sorted — the job page's rules,
  not this one's — so the name appears there because the payload is there and
  for no other reason.
- **Both modes, one meaning.** A store this process owns and a store it reaches
  over a socket carry the same record, so they recover the same class. Service
  availability may change coordination and isolation; it may not change what a
  failure means.

**An older record keeps its meaning, and says less than it looks like it does.**
A record written before this key existed carries a sentence and no class. It
loads, its `error` is intact, and it is *not now* — which is not a downgrade but
the only answer available, and the answer every reader gave before the key
existed. A reader must not infer a class from the sentence, and must not infer
one from `state`: `failed` is where a *no* leaves a record [DL-E2], and a record
can also be `failed` because a person cancelled a doomed job by hand.

*No* is: the source refused, nothing registered can serve the job's sources, the
sink escapes the store root, the sink names the store's own layout, and the job
layer says the record is invalid [DL-E4]. Everything else, including a sink
written in the other platform's convention, is *not now* [DL-E5] — that path is
unusable here and perfectly usable on the machine whose convention it is.

**A credential is bound to hosts on the fetching machine, and a source whose
host is not among them is *not now*** [DL-R29]. A record names a credential —
`attrs.credential=hf` — never a secret, and it also names the host the request
goes to. Anyone who can write the store can write both, so a machine that
resolved the name without asking where it was going would send the owner's
token to a server the record chose. The binding lives beside the secret, on the
machine — `ABSTRACTION_CRED_HF_HOSTS=huggingface.co`, each entry covering its
subdomains — never in the record and never in the store. A credential with no
binding reaches nobody. The refusal happens before a connection is opened, and
the job is left adoptable: a machine that binds the name to that host runs the
same record unchanged. See
[`deputy-credential`](testdata/scenarios/deputy-credential.txt) and
[`credential-bound`](testdata/scenarios/credential-bound.txt).

**A host this machine will not reach is *not now*** [DL-E8]. Before a source is
handed to a fetcher — the same last moment a credential is resolved — the runner
asks its `Reach` interface about the host the source would open a connection to, and
a refusal is recorded with the reason the policy gave, so an application can
show it in its own words. The job is left adoptable: the refusal is this
machine's policy, not the job's fault, and the same record runs unchanged on a
machine that may reach the host, or here once a person turns it back on. What
implements `Reach` is the machine's choice: `Discover` wires the hosts switched off in the
window; a bare runner reaches everything.

Over HTTP, **the source refused** is exactly these:

```
400 401 402 403 404 405 406 410 414 451
```

Listed, not ranged, and an unrecognised 4xx is *not now* [DL-E6].

**That breaks a MUST, and we mean to.** RFC 9110 §15 requires a client meeting
an unrecognised status code to treat it as equivalent to the `x00` of its class,
so an unknown 4xx must behave as `400` — which is on the list above as
permanent. Obeying it would end a job forever on any 4xx nobody had heard of.
We diverge because the cost is asymmetric and the population is not what the
rule assumes: an unfamiliar 4xx in front of a large file is far more often an
intermediary than an origin refusing the artifact — `499` is nginx closing on
its own client and `440` is an IIS session timing out, and neither is a
statement about the file. A *no* throws the download away; a *not now* costs one
more sweep. The measurement is below, and it is worse than a drifting list:
of three implementations, none had implemented the published rule either, so
this is a departure from RFC 9110 rather than a correction of a naive reading of
it.

`416` and `412`
are the resume offset or the file version being wrong and both restart
cleanly [DL-E7]; `408`, `425` and `429` say try later; `409` and `423` are
somebody else's lock. `401` is
on the list because a gated repository answers it until a person adds a token,
and adding a token is a new request rather than a retry — the same reasoning puts
`400` there, since a signature that has expired is not fixed by asking again.

Go and Python disagreed on this list by four rows until 2026-09-06 and nothing
could see it, because until
[`wire-refusal-status`](testdata/scenarios/wire-refusal-status.txt) and
[`wire-notnow-status`](testdata/scenarios/wire-notnow-status.txt) no test in
this repository compared one implementation's classification of a status against
another's. The first thing those two found was worse than a drifting list: Go's
`Fetch` had never consulted its own, so every status but `200` ended the job and
a `503` was permanent; C++ asked `400 <= status < 500` with two holes cut in it,
so somebody else's lock (`409`, `423`) was permanent too. **Listed, not ranged**
was written here, implemented nowhere, and enforced by nothing.

**The wire is a scenario surface like any other.**
[`scripts/behaviour-conformance.sh`](https://github.com/openabstractions/abstractions/blob/main/scripts/behaviour-conformance.sh) starts
[`testdata/fixture.py`](testdata/fixture.py) and a scenario names the answer it
wants in the URL — a range honoured, a range ignored, a `416`, a `206` starting
somewhere else, a `Content-Range` that lies about the body, a body that
disagrees with its own `Content-Length`, a redirect, a version that changed
under a resume. A new wire case costs a fixture answer and a scenario, and no
driver in any language changes.

**And a scenario says what SHOULD happen, not only that three implementations
did the same thing.** A `# expect <step>: <phrase>` line is checked against that
step's transcript in every implementation, so the contract is a fourth party to
the comparison and a rule all three break the same way goes red instead of
green — which it did not for `terminal`, where all three allow an update to a
record this project's own page calls history. The assertion is partial on
purpose: a scenario pins the fields a written rule settles and stays silent about
the rest, and the harness counts the scenarios that assert nothing rather than
letting silence read as conformance.

**And every rule on this page carries a tag a scenario cites**, so the harness
can print the rules nothing exercises rather than leaving them to be discovered
by whoever happens to write that scenario. The convention is one paragraph in
[`abstraction-job/CONTRACT.md`](https://github.com/openabstractions/abstraction-job/blob/main/CONTRACT.md#every-invariant-carries-a-name); the count is
printed by every run.

**A resume says which version it is continuing** [DL-V1]. A bare `Range` names no file,
so a source that replaced the artifact answers honestly with a valid range of
something else, and the splice has exactly the right length and the wrong
contents. The checkpoint therefore carries the strong validator the proven bytes
came from and every ranged request sends it as `If-Range` [DL-V2]: the source
answers `206` for the same file and `200` — the whole new one — for a different
one, which is a case already handled. Only strong validators are kept [DL-V3],
because a weak `ETag` asserts semantic equivalence rather than byte equality,
which is the one distinction a resume depends on. `Last-Modified` is used when
there is no usable `ETag` [DL-V4], and **the accepted spellings are the three RFC
9110 requires a recipient to accept** [DL-V5]: IMF-fixdate — `Sun, 06 Nov 1994 08:49:37 GMT`, the only one a
sender may use — and the two obsolete forms, `Sunday, 06-Nov-94 08:49:37 GMT`
and `Sun Nov  6 08:49:37 1994`. Nothing parses a time out of them. The value is
echoed back verbatim and an origin server evaluates `If-Range` by exact match, so
the only question is which spellings are recognised, and recognising them is a
shape check.

~~This said "only as an IMF-fixdate" until 2026-09-06.~~ Each implementation had
asked its own standard library what an HTTP date was and been given a different
answer — Go took a three-letter timezone the grammar does not allow, Python took
RFC 2822 with numeric zones, C++ took IMF-fixdate alone — so a rule that read as
one sentence was three, and a server sending the obsolete form got its validator
recorded by two implementations and dropped by the third. A dropped validator is
a resume that cannot name its version. See
[`wire-version-changed`](testdata/scenarios/wire-version-changed.txt),
[`wire-version-modified`](testdata/scenarios/wire-version-modified.txt) and
[`wire-obsolete-date`](testdata/scenarios/wire-obsolete-date.txt).

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

**Proved** ([`docs/results/SUPERVISOR1.txt`](https://github.com/openabstractions/abstractions/blob/main/docs/results/SUPERVISOR1.txt)): a
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

### `wanted/` — asking without a program

Every other door into this layer is a call. A person with a share and no
toolchain has a folder, so the supervisor watches one: a text file put in
`wanted/` inside the store is a request, and the folder answers it by renaming
the file.

```
https://huggingface.co/HuggingFaceTB/SmolLM2-135M-Instruct-GGUF/resolve/main/smollm2-135m-instruct-q8_0.gguf
https://example.com/big.iso sha256:2fc237e6…feb756 isos/
```

One download per line: the URL, then in either order an optional `sha256:<hex>`
and an optional destination. The destination is relative to the store; absent,
it is `files/<the name in the URL>`; ending in `/`, it is a directory and the
name comes from the URL. A line beginning `#` is ignored, and a file that begins
`{` is a spec exactly as this page spells it — **the line form is a convenience
over the spec, never a second grammar** [DL-W1].

What the folder shows is the whole interface:

| the file is | meaning |
|---|---|
| `x.txt` | waiting — nothing has looked, so no supervisor is running or it has not swept yet |
| `x.txt.accepted` | taken; inside, the job ids, where each lands, and progress on every sweep |
| `x.txt.done` | every job delivered; inside, the path, the size, and the digest if one was verified |
| `x.txt.failed` | a job ended without delivering; inside, why |
| `x.txt.refused` | not a request the supervisor will act on; inside, which line and why |

**A refused request is answered in place and its own text is kept** [DL-W2]:
the answer is appended as `#` lines, so renaming `x.txt.refused` back to
`x.txt` after fixing it is how to try again. A file over 64 KiB is renamed and
left unread.

It is a wider door than a record, because anyone who can write to the share can
use it, so it accepts less. **Only `http` and `https` are fetched for a dropped
request** [DL-W3] — a `file:` source would copy the supervisor's own disk onto
the share. The destination is inside the store, contained, not the store's own
layout and not the drop folder itself. No header and no credential name: a
credential is resolved on the fetching machine, and a request from a share could
point it at any host. Which hosts the supervisor will open a connection to at
all is the `Reach` interface's answer, not the folder's. Every refusal is a line in
the file, never a crash and never silence.

**Once every job a request named has delivered, the request is `.done`**
[DL-W4]; once one has ended without delivering, `.failed`. Until then it is
`.accepted` and its last lines are each job's state. The supervisor takes
delivery on the person's behalf, because nobody else will.

A hot folder is the oldest integration there is — printers, media encoders and
mail transfer agents all take work this way — and it is chosen here for the
same reason they chose it: a file is the one thing every person and every
program can write.

### Authority is designation, not identity

The bus is where a process reaches this supervisor: a local transport whose name
the supervisor invents at startup and publishes in its heartbeat, never a path
in the store, because two spellings of one path would be two names. Every
request on it arrives with its caller bound and checked by the `identity` layer
first, and a caller the machine cannot name is answered with the refusal and
nothing else — no wakeup, no owner, no tier.

**It carries no authority in either direction**: two requests, `look` meaning
"look now" and `who` meaning "who is there, and who do you take me for", one
JSON line each way, no job id, no payload, nothing granted. Being named is a
floor a stranger fails, never a grant. That is not modesty about the feature. It
is the design consequence of a measurement — **what a platform says about the
other end of a local socket can name the wrong process**, so nothing here is
decided by who a caller is.

A machine where the identity layer cannot name a caller at all gets no bus. The
supervisor says why, announces no endpoint, and is reached through the store
alone, sweeping on its timer; nothing that worked without a bus stops working.

Those platform facilities — `SO_PEERCRED`, macOS `LOCAL_PEERTOKEN` plus a
code-signing requirement, `GetNamedPipeClientProcessId` — are how local IPC here
is normally secured, and this layer asks for all of them through `identity`. The
deliberate divergence is in what their answer is allowed to do: it may refuse a
caller, and it may never authorise one. Measured on macOS 15.7.4,
2026-09-05,
[`probe-evidence.txt`](https://github.com/openabstractions/abstraction-identity/blob/main/probe-evidence.txt):
a helper forked *before* the connection existed, then `exec`ed `/bin/cat` while
holding the peer socket. The kernel then described that peer as pid 52137,
`exec=/bin/cat`, `package=com.apple.cat`, code signed by Apple Software Signing,
with `startedAfterConnect=false` so that a "started before it connected" check
passed too. `anchor apple` **accepts** what was an unsigned caller a moment
earlier. Every identity signal the platform offers bound to the wrong process.
Only uid survived.

What does carry rights is a **designation**: the job id. Holding one is what
lets a process work that job, and it authorises that job and nothing else — the
store gives it exactly one path it may write, its own `work/<id>`, and refuses
every other name in the store's own layout. This is designation in the
sense Dennis and Van Horn gave it in 1966; the failure it avoids is the confused
deputy (Hardy, 1988), which is the same shape `job.Reserved` guards against in a
filesystem.

**Verified for:** macOS peer-credential and code-signing checks. **NOT
examined:** the Linux and Windows equivalents, which were not probed, and the
divergence is applied on all three.

### The delegate channel

A `Delegator` hands a job to something that will still be working after this
process exits. The `nas` delegate hands it to another supervisor, over a store
both machines can open. This section is what a stranger needs in order to write
either half of that.

**This section declares no rule.** It collects the ones already declared above
and says which of them bind which party, so a tag here is written `DL-R20`
rather than in the square brackets used above — the brackets are how a rule is
declared on this page, and the harness that finds a tag declared twice cannot
tell a declaration from a citation.

**The channel is a shared record and nothing else.** The requester opens the
remote store as a `job` file store — a UNC path, a mapped drive, a mounted
share — submits a record into it, and reads that record back. The delegate is a
supervisor sweeping the same directory through its own filesystem. Neither side
opens a connection to the other, and no message format exists beyond the record
the `job` layer already defines. Verified in `abstraction-download/go/nas/nas.go`, which
imports `os`, `path`, `path/filepath` and the two layers, and reaches the far
side only through `job.Store`.

Two things in the same package are **not** on this channel and are configuration
rather than transport. `Find` (`abstraction-download/go/nas/find.go`) sends SSDP and mDNS
multicast to look for a host that might hold a store, and `Check` writes and
re-reads one probe byte to prove the share is writable; both run when a person
is choosing a store, carry no job and no record, and a configured delegate never
calls either. The supervisor's bus is a local transport by construction and does
not cross a share, so a delegate learns of new work from its own sweep and from
nothing else.

**What the requester writes.** A record of kind `download`, submitted with a
fresh id in the remote store, holding a spec that differs from the local one in
two places: `sink.final` is rewritten to `<dir>/<basename of the local final>`,
relative and slash-separated so the far side resolves it against its own view of
the root `DL-R20`; and `spec.request` carries the *local* record's id, which is
how a handoff whose answer was lost is found again by reading the remote
records' own specs. The local record keeps `delegation = {system, external_id}`,
where `external_id` is the remote record's id, and its `progress` becomes a
cache of what the remote record last said.

**What the delegate must obey.** It is a supervisor over a store several
machines can write, so every rule already stated for that position applies
unchanged, and the reason each exists is that the record's author and the
writer's authority are different parties:

- A relative sink resolves against the delegate's own root `DL-R20`; an absolute
  one is refused outright by a supervisor in this position `DL-R30`, and by any
  machine whose convention it is not written in `DL-R21` `DL-S5` `DL-S6`
  `DL-S7`.
- A sink that resolves outside the root is refused `DL-S8`, and one that names
  the store's own layout is refused `DL-R22` `DL-S9`, with `work/<id>` the one
  path the job may write `DL-R23` `DL-S10` `DL-S11`, compared by the rules that
  do not vary with the host `DL-S12` `DL-S13` `DL-S14`.
- A credential named in a record is resolved on the fetching machine and is
  refused for a host it is not bound to `DL-R29`; a host this machine's policy
  will not open is *not now* `DL-E8`. Both are the delegate's own answers, not
  the requester's.
- Bytes its own store has already proven are used before any source is opened
  `DL-R28` — on this channel that matters more than anywhere else, because the
  delegate's earlier deliveries are files the requester cannot see and the
  record cannot name.
- Intent is honoured `DL-R25` `DL-R26` `DL-R27`; a failure is recorded with its
  class `DL-E1` `DL-E2` `DL-E3` `DL-E14`; the digest decides `DL-R15` `DL-R16`;
  and it runs the same transfer rules as any other runner, `DL-R1` through
  `DL-R14`.

**What the requester must obey.** It verifies what the delegate delivered rather
than believing the record `DL-R17`, because the bytes crossed a network twice;
it returns a job whose handle no longer resolves to `pending` with its sources
and checkpoint intact `DL-R18` `DL-R19`; and it publishes the dispatch rule's
answer rather than the head of the chain, because a delegate is reached only
through a supervisor `DL-R33`.

**What the requester does not hand over.** A resume offset is not carried: the
proven bytes are on the requester's disk, the delegate has its own store and its
own partial, and an offset for a file the far side does not hold produces the
right length and the wrong contents. The `nas` delegate therefore declines
`resume` as a capability, so a locally interrupted job stays local. The
checkpoint is not carried for the same reason.

**How this compares with the service binding.** `job`'s `RemoteStore` and
`Serve` carry the same `Store` semantics over a socket, with a generated
`Request`/`Response` envelope, a `Verdict` enum, and one newline-terminated JSON
exchange per connection. This channel has none of those: there is no envelope,
no framing beyond a file, no operation field — the operation is implied by which
part of the record changed — and no serialised refusal vocabulary, because a
refusal here is a local error in the requester's own process. What the two share
is the record schema, the lease, the epoch, and polling as the way either side
learns something changed.

**Not yet a rule.** These are obligations the implementation places on a
delegate that no tag above states, listed so that a second implementer meets
them on this page rather than in a defect:

- The delegate is a `job` file store binding at the same root, holding byte 0 of
  `<id>.json.lock` for every write and staging through a temporary file it
  renames. A delegate that locks differently passes its own tests and loses
  updates against ours; `abstraction-cas/README.md` § *What a fourth implementation must do*
  is the agreement.
- `transferred` is the delegate's last state. The requester writes `complete`
  when it has taken delivery, and a delegate that acknowledges its own work
  removes the state that says "finished, not yet collected".
- `spec.request` must survive on the remote record. It is not in
  [The fields](#the-fields), and a reader that dropped it would make a lost
  handoff unrecoverable.
- `requires` is not carried into the remote record, so the delegate's own
  placement constraints are its machine's and not the submitter's `DL-R24`.
- Abandoning removes files inside the delegate's store — the job's own partial,
  and the final when no other unfinished record names it. Nothing above says a
  requester may delete there.
- `wanted/` is not reserved. `job.Reserved` covers `jobs/`, `work/` and
  `services.json`, and this layer adds `supervisor.json` and `supervisor.sock`;
  the drop folder is in neither list, so `DL-R22` does not reach it.
- A failure crosses with its class. The remote record carries it where
  [DL-E16] says, the delegate's status report carries it beside the sentence,
  and the requester writes both onto the local record. A delegate that cannot
  tell the two endings apart reports *not now* and says so where it decides —
  it does not read a class out of the words, which is what `DL-E14` forbids.

**A record written from two hosts.** Both sides write this record: the requester
submits it, sets intent on it without a lease to pause or cancel, and claims it
to acknowledge or abandon; the delegate claims it, checkpoints it and ends it.
The lease and its epoch are what make one writer authoritative at a time, and
they are the whole of the mutual exclusion here — the file lock underneath them
is not shared, because a byte-range lock taken over SMB and a `flock` taken on
the server's own volume do not meet. Measured in `abstraction-cas/README.md`: writers on two
hosts on one file lost or refused 147–149 of 2150 updates across three runs,
while six writers in three languages on one host lost none of 1800. `job`'s own
page states the consequence — a record written from two hosts needs a protocol
that store does not have — and this channel is that case. What is not measured
is how often it bites here, where the two sides write at different moments in a
job's life rather than in a loop.

## What ships today

| implementation | shape | schemes | resume | survives process exit | status |
|---|---|---|---|---|---|
| `HTTP` | Fetcher | `http`, `https` | yes, `Range` | **no** | working |
| `File` | Fetcher | `file`, `smb` | yes, seek | **no** | working |
| `bits` | Delegator | `http`, `https`, `smb` | yes | **yes** | working, 12 tests against real BITS |
| `nas` | Delegator | `http`, `https` | yes, over there | **yes** | working, verified against a Synology |

**Nothing above chooses between them.** A caller asks for bytes; the Runner
offers the job to whatever is registered and capable, and registration comes from
configuration. A NAS outranks BITS because it is always on; BITS outranks
in-process because it survives this process exiting. There is no argument
anywhere that names a tier — see [`addon-synology`](https://github.com/openabstractions/addon-synology).

`https://github.com/openabstractions/research/blob/main/transfer/SUMMARY.txt` said adopt BITS rather than write it, and that
held up: persistent jobs with a GUID any process can open, documented ownership
transfer, auto-resume on logon and network recovery.

The `nas` binding needed no new wire format and no network protocol. It writes a
job record into a store the far side can also see, and polls it by reading a file
on a share. That it required nothing new is the strongest evidence the job layer
was cut in the right place.

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

## The spec, exactly

`job` will not look inside a `Spec`, so nothing underneath this layer can notice
two implementations reading one differently. They did: a digest written bare
reached a reader that built `"sha256:" + hex` and compared strings, the error
read `got sha256:1fc70f… want 1fc70f…` — the same digest twice — and a correct
1.5 GB download was deleted and fetched again.

So each implementation ships a **`specread`** that prints the meaning it arrived
at, and [`scripts/spec-conformance.sh`](https://github.com/openabstractions/abstractions/blob/main/scripts/spec-conformance.sh) requires
the printouts to be identical. Everything below is the contract. Write a reader
against this page and it passes; nothing here is recoverable from our source.

### The fields

| field | meaning | absent |
|---|---|---|
| `artifact.digest` | `sha256:` + 64 lowercase hex | unknown — **not** "matches anything" |
| `artifact.size` | bytes, decimal integer | `0`, meaning unknown |
| `sources[]` | where the bytes may be had, ordered | nothing to try |
| `sources[].scheme` | `https`, `file`, `smb`, … | — |
| `sources[].locator` | the address, in that scheme's own spelling | — |
| `sources[].priority` | integer, **lower is tried first**, may be negative | `0` |
| `sources[].attrs` | string→string, describes the source **to us** | empty |
| `sources[].headers` | string→string, sent **to the server** verbatim | empty |
| `sink.final` | where the bytes end up | — |
| `sink.partial` | where they accumulate first | the layer invents one |

`attrs` is an annotation bag in the sense OCI and Kubernetes use the word:
non-behavioural metadata, keyed by whoever wrote it, that this layer carries and
does not act on.

**`attrs` and `headers` are two bags and the split is the contract.** It was one
bag, and a key nobody remembered to exclude went out as an HTTP header. No unit
test in one language can see another language classifying the same key
differently, which is why the classification is printed and compared.

**Unknown keys are ignored, at every level** [DL-S1]. This is the opposite of the job
record, which [refuses a field it does not know](https://github.com/openabstractions/abstraction-job/blob/main/CONTRACT.md), and the
difference is deliberate: the record is a contract three languages share, while a
spec is opaque payload that the layer above extends. A reader that refuses
`group_id` cannot carry a record ComfyUI wrote.

**A digest is meant as its hex** [DL-S2]. `sha256:` and `sha256-` (how Ollama
names its blobs) are stripped, case is folded down, surrounding space is trimmed.
Anything that is not exactly 64 hex characters afterwards reads as
**empty** [DL-S3] — an unreadable digest must never compare equal to another
unreadable one.

**A locator is compared byte for byte, and that is ours, not theirs** [DL-S24].
`RFC 3986 §6` defines URI equivalence and gives a ladder of normalisations —
case of scheme and host, percent-encoding, the default port, an empty path —
and WHATWG URL defines a parser that produces a serialisation. **We climb none of
it.** Two locators are the same source when their bytes are the same.

That is deliberate and it is the *safe* direction, unlike the path case
[DL-R31], because the two errors are not symmetric. Splitting one source into
two costs at worst a second attempt at the same bytes, and [DL-R28] catches it
by digest before the network is touched. Merging two locators that are not the
same resource fetches the wrong file. A normalisation ladder can only merge, so
we do not climb it until an adopter shows us the case that needs it.

**The host that decides is the host the transport will open, read by the parser
the transport itself uses** [DL-E15]. Where a locator's host decides something —
whether a credential may be attached [DL-E7], whether a host is reachable
[DL-E8] — the answer comes from `net/url` in Go, `urllib.parse` in Python and
`WinHttpCrackUrl` in C++, which is the same call whose result is handed to
`WinHttpConnect`. Folded to lower case per `RFC 3986 §3.2.2` and §6.2.2.1, and
compared with the port and the brackets of an IPv6 literal removed.

**An authority that is not a plain host name is *not now*** [DL-E15]. A plain
host name is a non-empty run of `a-z 0-9 . - _ :` after folding, with one
trailing `.` dropped. Userinfo is refused whether or not it is empty; so is a
byte above `0x7F`, a percent escape, a backslash, a space, a control character,
and an `http` or `https` locator naming no host at all. The job is left
adoptable: the same record with the host spelled plainly runs here unchanged.

**This diverges from `RFC 3986 §3.2.1`, which permits userinfo, and from WHATWG
URL, which repairs an authority rather than refusing it — and the divergence is
the whole rule.** Measured over 56 locators against all three parsers *and*
against the address each transport actually dialled, **22 rows had at least two
answers**. `https://hf.co\@evil.com/x` is `hf.co` to a hand-written scan and
`evil.com` to `WinHttpCrackUrl`, so a credential bound to `hf.co` was attached to
a request whose socket opened elsewhere; the reverse spelling
`https://evil.com\@hf.co/x` fails the other way and merely does not work.
`https://ｈｕｇｇｉｎｇｆａｃｅ.ｃｏ/x` and `https://huggingface。co/x` are
distinct strings to every parser and `huggingface.co` to Go's dialler, so a
refusal list naming the site did not refuse it; `https://huggingface.co./x`
resolves to the same four addresses as `https://huggingface.co/x` and did not
refuse either. WHATWG URL's repairs are what make it unsafe here: `urllib.parse`
deletes tabs and newlines from an authority before reading it, so
`hf.co<TAB>.evil.com` becomes a perfectly plain host that appears nowhere in the
locator.

**A host is merged where a locator is split, and that asymmetry is deliberate**
— it is the opposite of [DL-S24] one paragraph above, for the opposite reason.
Merging two locators fetches the wrong file. Merging two spellings of one host
can only make a refusal list match more often and bind a credential to the same
origin it was already going to; splitting one host into two is what lets a
refusal be walked around. So the ladder is climbed for a host and not for a
locator.

**No IDNA table is carried, and that is why an internationalised name is refused
rather than guessed at.** Go's transport applies IDNA and dials the A-label,
WinHTTP applies its own, and `urllib` raises `UnicodeEncodeError` before it
reaches a socket — three answers, none of them ours, and the only table that
could reconcile them is a copy of Unicode that would go stale inside a shipped
binary. An adopter names the host in punycode; `xn--n3h.example` is accepted and
was measured identical to its dialled form.

**C++ has no URL parser in its standard library and this is where that costs
something.** On Windows there is no gap: `WinHttpCrackUrl` is the transport's
parser and it answers. On every other platform `https_available()` is false, no
fetcher opens a socket, and the authority is read by `RFC 3986 §3.2`'s delimiters
in `offline_authority` — held to the other two implementations by the shared
corpus rather than by a transport, which is the weaker guarantee and is the
reason this rule is stated as a refusal instead of a normalisation. The day a
fetcher lands on that platform, its parser answers instead.

**One authority the platform will not parse is still read by hand: `\\server\share`.**
It is not a URI, no URI parser accepts one, and it is the only place this layer
still spells out delimiters for a host.

**A path is absolute if it is absolute under *either* convention** [DL-S4] — a
leading `/` or `\`, or a `X:` drive letter. Absolute paths are left byte for byte
as given; relative ones are printed with forward slashes.

**An absolute sink is honoured only by a machine whose convention it is written
in** [DL-S5]. Windows spells one with a drive letter or a leading `\`; POSIX
spells one with a leading `/`; a machine refuses the other. This corrects a claim that was
wrong for four months — that a foreign absolute path "fails with no such file
rather than quietly creating a directory". That is true of a path being *read*
and false of a *sink*: `open("D:\models\x.gguf", O_CREAT)` on Linux succeeds and
makes a file of that literal name in the working directory, with a `.part`
beside it, and the runner creates the parent first so `/mnt/models` on Windows
lands on whatever drive the process was on. **A record carrying such a path is
still valid** [DL-S6] — it is correct on the machine that wrote it — so this is
refused by the machine about to do the writing and never at submission [DL-S7].
**A record meant to be adopted by another machine names a relative sink**; that
is what resolving against the store root is for, and it is the only portable
form.

**A relative sink path that resolves outside the store root is refused** [DL-S8], and the
answer does not depend on the root: containment is measured from it, so one `..`
climbs out of it wherever it sits. Resolve first and ask where the answer landed:
scanning the input for `..` instead fires on the perfectly contained `a/../b`,
and a backslash in a relative path is a separator here — `..\..\Startup\evil.bat`
is a climb, not a filename, because a backslash cannot legally be in one.

**A relative sink path that names the store's own layout is refused** [DL-S9].
Staying inside the root was never enough: a final of `jobs/<id>.json` overwrites a job
record and a final of `work/<other>` overwrites another job's partial, and both
are contained by every measure above. The reserved set is the file binding's
layout, and it is asked of the `job` layer rather than spelled here — a
`download` that hardcoded `jobs` and `work` is the coupling the opaque spec
exists to prevent.

Measured from the store root, with `.` and `..` resolved first:

| reserved | who owns it |
|---|---|
| `jobs`, and anything under it | the record, its claim tokens, its temporaries |
| `work`, and anything under it except `work/<this job's id>` | another job's scratch |
| `services.json` at the root | the discovery registry |
| `supervisor.json`, `supervisor.json.tmp`, `supervisor.sock` at the root | this layer's heartbeat, and one name it reserves that nothing binds |

`supervisor.sock` is that name. The bus is not a file in the store, so nothing
opens it any more; it stays reserved because a name two implementations of three
refuse is a divergence, and a store written under an older one may hold it.

`work/<id>` and everything below it is **not** reserved against job
`<id>` [DL-S10] — that is where its own partial goes, and a blanket ban on
`work/` would refuse the default partial this layer invents. Asked on behalf of
no job (an empty id, which is what a caller has before an id exists), the whole
of `work/` is reserved [DL-S11].

**Comparison folds case and trims a trailing dot or space from every segment, on
every platform** [DL-S12]. Deliberately unlike the containment comparison above,
which folds case when the host is Windows — a guess about the host, and known to
match no filesystem exactly [DL-R31]; it survives only because containment folds
both sides alike and is lexical by construction. Containment compares two paths on the
machine doing the resolving; this compares a record's path against names the
contract fixed, and a record refused by Windows and accepted by Linux would mean
the refusal depends on who looked. `Jobs/x.json` and `jobs./x.json` both open
`jobs/` on NTFS. A segment that is *nothing but* dots and spaces is left
alone [DL-S13]: it is a real name on POSIX and not one Windows will open.

Absolute paths are outside this rule entirely [DL-S14] — they are never joined
onto the root, so they name the store's contents only by a coincidence no lexical
rule could see.

Like containment, this is **lexical and claims nothing more**. A symlink or a
junction inside the root that points at `jobs/` defeats it, for the reason given
above, and nothing here resolves a path at the moment of the write.

### What `specread` prints

`specread <spec.json>` writes to **stdout**, in this order, one `key=value` per
line, and exits `0`:

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
- Then three lines per source, `source0` … `sourceN`, indexed from `0` **in the
  order the sources would be tried**: stable sort on ascending `priority`, so
  equal priorities keep the order the spec gave them [DL-S15]. That order is behaviour —
  a local copy at `-100` is what turns a download into a copy.
- `sourceN=` is `scheme`, one `|`, `locator`.
- `digest` is the normalised form, `sha256:<hex>`, or empty.
- `final` and `partial` are the portable form of `sink.final` and `sink.partial`.
- `*_refusal` is empty when the path is acceptable. Otherwise it is the refusal
  text, and the path in it is quoted **as the spec spells it**, before the
  portable form is taken.
- A map prints as `k=v` pairs joined by `,`, keys sorted ascending by byte value,
  empty map as the empty string. Values are raw: nothing is quoted or escaped, so
  a value containing `,` or `=` is out of contract.

**LF, always, including after the last line.** No CR anywhere. The record pins
this too, and for the same reason: a CRLF build disagrees with every other
implementation about every line at once, and the diff shows nothing. On Windows,
Python needs `sys.stdout.reconfigure(newline="\n")` and C++ needs the stream
opened in binary mode; Go is already right.

`specread --partial <final> <id>` prints exactly one line and exits `0`:

```
partial=work/1757000000000-deadbeef
```

The partial name is the one field a caller may leave out, so the layer invents
it — and a successor in another language finds a predecessor's bytes only by
inventing the same name. A `final` that is relative under both conventions gives
`work/<id>` [DL-S16], in the store's own work directory, because the record must
not name the machine that submitted it. Anything else gives
`<final>.part` [DL-S17], beside the artifact, because delivery across volumes is
a copy and a copy into the final name leaves a truncated file under the name an
application reads as an installed model. An empty `final` counts as
relative [DL-S18].

`specread --portable <path>` prints exactly one line and exits `0`:

```
portable=C:/Users/r/.cache/huggingface/hub/models--o--r/snapshots/41ba88db/x.gguf
```

**A path in a record has one separator, `/`, and it is the same one all the way
through** [DL-S20]. Every backslash becomes a forward slash, whatever wrote the
path and whether it is relative or absolute — except a path rooted at a single
`/`, which is returned byte for byte [DL-S21], because a backslash is a legal
character in a POSIX file name and rewriting one would name a different file. A
UNC root comes out as `//server/share` [DL-S22] and is still an absolute Windows
path to `--foreign`.

Nothing about the machine is lost, because nothing about the machine was ever in
the separator: a drive letter and a UNC root say Windows on their own, and
Windows accepts either separator in every path it is handed. What the separator
did carry was noise — which implementation happened to write the record — and two
spellings of one destination do not compare equal, so a store holding both
fetches the artifact twice.

The rewrite is **stable**: a path already in this form comes back
unchanged [DL-S23]. Comparing two paths is a different question and a wider
normalisation — `.` and `..` resolved, case folded where the filesystem folds it
— and it is applied to both sides at the moment of the comparison rather than
assumed of what is stored, because records written before this rule are on disk
and keep resolving.

`specread --reserved <owner-id> <path>` prints exactly one line and exits `0`:

```
reserved=download: sink path is reserved by the store: jobs/1757000000001-cafebabe.json
```

Empty when the path is free space. The owner id is an argument because the
answer depends on it: `work/<owner>` is the one reserved path that job may
write. This is not a fixture for the same reason — a spec file names no job.

`specread --foreign <path>` prints exactly one line and exits `0`:

```
foreign=download: sink path names another platform's filesystem: /mnt/models/x.gguf
```

Empty when this machine may write the path. **The only line here whose answer
depends on the host**, so what the harness compares is that every implementation
*running on the same host* gives the same answer — and that at least one of a
Windows-shaped and a POSIX-shaped absolute path is refused, or the check is
passing because nothing is ever refused.

### Refusing, and where a refusal goes

Two kinds, and they do not share a channel.

**A contract refusal is a value.** It is a judgement about the record —
`final_refusal`, `partial_refusal` — printed on stdout, with exit status `0`,
and **its wording is part of the comparison**. Three languages must refuse the
same records for the same stated reason, or a record one of them will not touch
is quietly acted on by another. This is the whole catalogue:

| when | exactly |
|---|---|
| a relative sink path resolves outside the store root | `download: sink path escapes the store root: <path as spelled in the spec>` |
| a relative sink path names the store's own layout | `download: sink path is reserved by the store: <path as spelled in the spec>` |
| an absolute sink path is written in the other platform's convention | `download: sink path names another platform's filesystem: <path as spelled in the spec>` |

The first two are judgements about the **record** and are the same on every host,
so they are checked at submission and again by the machine that writes. The third
is a judgement about **this machine** and is checked only by the one about to
write: a record it refuses is a record that was correct where it was made.

**A tool failure is not part of the contract.** The file is missing, the bytes
are not JSON, the arguments are wrong: that goes to **stderr**, with a non-zero
exit status, and the wording is nobody's business but the implementation's. The
harness reads stdout only and requires exit `0`, so it can never mistake one for
the other.

### What a status byte can carry

A process boundary is the narrowest one this layer crosses, and it was the only
one with no vocabulary. The record carries a class in a field; a delegate's
failure crosses as `Failure{error, permanent}` precisely because *"errors.Is does
not survive JSON and a sentence is not a class"*; and then the same class reaches
a shell as one byte, where until 2026-09-08 it did not survive at all. `dl` and
`dlc` agreed on `0` and `2` by coincidence, disagreed `1` against `3` for
"cannot do that", and neither could tell *not now* from *no* [DL-E1] [DL-E2].
A command-line conformance corpus cannot be written against that.

**The status is the class, never the cause** [DL-E9]. These are the three
answers of [`abstraction-job/SPEC.md` § 6](https://github.com/openabstractions/abstraction-job/blob/main/SPEC.md), plus the two a command line needs.

| status | name | § 6 answer | what the caller should do |
|---|---|---|---|
| `0` | done | — | take the result |
| `1` | not now | unavailable | run the same command again, later |
| `2` | misuse | — | fix the command line; **nothing was attempted** |
| `3` | no | forbidden | change the request or stop; running it again is pointless |
| `4` | unknown | unknown | find out before deciding — the work may have happened |
| `130` | interrupted | — | `128 + SIGINT`, POSIX |

**Why the class and not the cause.** `curl` publishes about ninety-nine codes
naming causes — `6` cannot resolve host, `7` cannot connect, `28` timeout — and
no class at all, so a caller asking the only question that changes its behaviour
must keep a table mapping curl's numbers onto retryable and permanent. That table
is a second place to update and it drifts, which is the defect [DL-E3] is about;
`wget`'s nine are half causes and half classes; `rsync`'s are causes with two
famous exceptions, `23` and `24`, which exist because "partial transfer" needed a
class and a cause-numbered space had nowhere to put one. **We publish the class.
The cause goes to stderr, where its wording is nobody's business but the
implementation's** — and a caller that parses stderr to recover the cause is
doing the thing this vocabulary exists to stop.

**Why these numbers.** `0` and `2` are conceded rather than chosen: every shell
reads `0` as success, and `2` is a usage error in `wget`, in bash's builtins,
and in both of our commands already. `3` is `dlc`'s own: written independently
of the Go tool, it arrived at "understood, and this engine cannot keep it" as a
number nobody had asked it for, and two implementations converging on a code for
a permanent refusal is better evidence than either arguing for it. `1` is *not
now* deliberately, because the generic slot is what an unfamiliar failure gets
treated as, and this page has already priced that asymmetry once — *a no throws
the download away; a not now costs one more sweep* — when it made an
unrecognised 4xx *not now* [DL-E6]. `4` is *unknown* and must not be folded into
`1`: § 6.2 records that the job layer has no sentinel for it and the download
layer does, and `ErrOutcomeUnknown` means a delegate may still hold the work, so
retrying is unsafe and giving up is wrong. *Find out* is a third instruction and
it needs a third number.

**We diverge from `sysexits.h`, and record it** [DL-E10]. BSD got the taxonomy
right first: `EX_TEMPFAIL` (75) is *not now*, `EX_UNAVAILABLE` (69) and
`EX_NOPERM` (77) are shades of *no*, `EX_USAGE` (64) is misuse. We take the
distinctions and refuse the numbers. 64–78 collides with nothing and communicates
nothing; `2` for misuse is already shipped in two implementations of this layer;
and sysexits' own manual page has disclaimed universal use for thirty years. The
mapping above is the whole of what an adopter who knows sysexits has to read.

**The status is about this invocation, not about the work it observed**
[DL-E11]. `jobd once` is a sweep. When a record in the store has failed
permanently, the sweep succeeded and the job did not: it prints the problem and
exits `0`. A scheduled task reporting failure because one of a hundred records is
a `404` teaches an operator to ignore it, which costs more than the `404` did.

**A contract refusal is still a value, not a status** [DL-E12]. `specread` prints
`final_refusal` on stdout and exits `0`, and that rule is untouched: it is a
judgement about a *record*, identical on every host and comparable across
languages. This page is about the fate of *one run of one program*.

**Three things a status byte cannot carry, said out loud rather than faked**
[DL-E13]. A full disk and a rebooted NAS are both `1` — the class fits, the cause
does not. A resumable transfer that moved 39 of 40 GB and stopped is `1`, the
same as one that moved nothing; `rsync` needed two codes for that shade and
scripts still get it wrong, and what actually holds progress is the record, so
the answer is `jobd status` and not a number. And *started, still running* has no
code: `over_curl` has a fifth answer, `working`, and `modelget get --background`
exits `0` having delivered nothing, where `0` means *the job is in the store*.
**A command that may exit before the work is done says so in its own help, and
its `0` is not this vocabulary's `0`** — an argument for that flag being rare,
not for a sixth number.

**UNPROVEN.** `jobd`, `modelget` and `jobctl` speak this as of 2026-09-08.
`dl` still exits `1` for every failure and `dlc` for every non-delivered answer,
so neither yet distinguishes `1` from `3`; `dlc`'s `3` for `list` and `watch` is
already right. `jobctl` cannot reach `3` or `4` at all: the job layer publishes
its § 6.1 mapping as a table on a page rather than attaching the class to each
sentinel the way this layer does [DL-E3], so its own driver cannot classify
`ErrNotFound` without keeping the copy that rule forbids.

### Whether a document is readable at all is also the contract

The wording of a tool failure is nobody's business but the implementation's. Its
**verdict** is everybody's. A spec one implementation reads and another refuses
is not a strictness preference: the first writes the record and the second cannot
open it, and the work stops at whichever host happens to pick it up. **Strictness
is a cross-language decision or it is a bug** [DL-S19], and that holds even when the
strict reader is the safer one — refusing a repeated key in one implementation of
three does not close a parser differential, it converts it into an availability
split, and by then the record is already written.

`specread --echo <spec.json>` prints exactly one line and exits `0`:

```
echo={"artifact":{"digest":"sha256:…","size":1},"sources":[…],"sink":{…}}
```

The spec as this implementation would **carry** it inside a record, compact. Go
holds a spec as raw bytes; Python and C++ hold it parsed and re-emit it, so a
number can be respelled and an escape policy applied on the way through one
implementation and not another, and every reader downstream sees the changed
bytes rather than the submitter's. Whitespace is the record writer's choice and
is compacted away here; escapes and number spellings are not, and are compared.

`scripts/verdict-conformance.sh` feeds every file in
`abstraction-download/testdata/verdicts/` to every registered implementation in both modes
and compares the verdict — accepted, refused, or neither — rather than the
output. It takes `SPECREAD_CPP` and `SPECREAD_EXTRA` exactly as
`spec-conformance.sh` does. **That corpus only grows**: any input that has ever
produced a disagreement stays in it, so a divergence closed today cannot quietly
reopen without the harness saying so.

**`specread` plus a growing verdict corpus is protobuf's
`conformance_test_runner`**, which does exactly this: hand one payload to every
implementation, compare what each says it means, and never delete a case.
CommonMark's spec tests, `sqllogictest` and Wycheproof are the same instrument
aimed at a grammar, a query language and a crypto library. The part that is ours
is only which document is fed in.

### Adding a fourth implementation

One line, and it need not live in this repository:

```bash
SPECREAD_EXTRA='node=node /path/to/specread.js' bash scripts/spec-conformance.sh
```

One `name=command` per line for several. The harness builds Go, finds Python,
takes C++ from `SPECREAD_CPP`, and compares everything registered against
everything else.

## Tested

```bash
cd go && go test ./...
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
