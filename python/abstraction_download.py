"""The download abstraction, in Python.

This is the second implementation, and the second implementation is the point.

An interface with one implementation is a design. It is only an abstraction once
something written independently can pick up work the first one started, and the
only way to find out is to make them do it. That is not a theoretical exercise:
the job layer shipped a cross-language bug where Go's RFC3339Nano trimmed
trailing zeros and Python did not, so two implementations wrote the same instant
as different strings. Nothing but a test that ran both could have found it.

So this deliberately does NOT read the Go source while running. It reads the same
records, from the same directory, and either the contract is written down
properly or it does not work.

    from abstraction_job import FileStore
    import abstraction_download as dl

    store = FileStore(dl.store_root())
    job_id = dl.submit(store, dl.Spec(
        artifact=dl.Artifact(digest="sha256:...", size=328597408),
        sources=[dl.Source(scheme="https", locator="https://...")],
        sink=dl.Sink(final="models/x.gguf"),
    ))
    dl.Runner(store).run(job_id)

Standard library only, like the job layer, and for the same reason: an
abstraction that needs three packages installed before anybody can try it is one
nobody tries.
"""

from __future__ import annotations

import hashlib
import json
import ntpath
import os
import posixpath
import re
import socket
import stat
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from datetime import datetime, timezone

from abstraction_job import Record, Scratch, Store, RUNNING, TRANSFERRED, COMPLETE
# The heartbeat is written by whichever implementation is supervising, so its
# timestamps are the job layer's format and are parsed by the job layer's reader
# rather than by a second one that could drift from it.
from abstraction_job import _parse_time

# KIND is the job.Record.kind this module understands. A process that finds a job
# of an unknown kind leaves it alone rather than guessing at its spec.
KIND = "download"

# Capabilities, matching the Go names exactly. These cross the boundary as
# strings in Record.requires, so a typo here is a job no Python worker will ever
# claim and no error anybody will ever see.
CAP_RESUME = "resume"
CAP_SURVIVES_PROCESS_EXIT = "survives_process_exit"
CAP_VERIFIES = "verifies_content"
CAP_DELEGATES = "delegates"

# CREDENTIAL_ATTR names a credential by NAME in a source's attrs. The value never
# appears in a record: it is resolved from the environment at the moment of the
# request. See credentials, below.
CREDENTIAL_ATTR = "credential"


class DownloadError(Exception):
    pass


class DigestMismatch(DownloadError):
    """The bytes are not what was asked for. The partial is deleted rather than
    left for a successor to resume onto a prefix already known to be wrong."""


class ShortTransfer(DownloadError):
    pass


class RangeIgnored(DownloadError):
    """The server answered a ranged request with the whole file.

    Appending that to what we already have produces a file of plausible length
    and impossible content. `curl -C -` does exactly this; this refuses.
    """


class NoSource(DownloadError):
    pass


# ---------------------------------------------------------------- the spec ---


@dataclass
class Artifact:
    """What the job is for: an identity, and how big it is.

    digest is an INPUT, not a result. Every transfer tool in the survey either
    verifies nothing, or verifies only size and timestamp. If the caller knows
    what the bytes should be, this layer must be able to refuse anything else.
    """

    digest: str = ""
    size: int = 0


@dataclass
class Source:
    """Somewhere the bytes can be had. Deliberately NOT a URL — an SMB path, a
    peer and a local file are all sources, and none are expressible as an http
    URL without lying."""

    scheme: str = ""
    locator: str = ""
    attrs: Dict[str, str] = field(default_factory=dict)
    priority: int = 0


@dataclass
class Sink:
    """Where the bytes land. 40 GB does not fit in a return value, and the
    process doing the writing may not be ours at all."""

    partial: str = ""
    final: str = ""

    def resolve(self, root: str) -> Tuple[str, str]:
        """Turn these into paths on THIS machine.

        A relative path means "under the store root". That is what lets a record
        written by Windows into \\\\nas\\share\\store be acted on by a container
        that mounts the same directory as /store.
        """
        return _resolve_under(root, self.partial), _resolve_under(root, self.final)


def local_root(store) -> str:
    """The store's own directory, or "" when its binding is not a filesystem.

    Asking is the point. ``store.root`` used to be an attribute, so this layer
    -- which is not supposed to know what a file is -- could read a directory off
    any store without admitting it needed one, and would have handed a service
    binding's absence straight to os.path.join.
    """
    return store.root() if isinstance(store, Scratch) else ""


def local_sink(store, sink: "Sink") -> Tuple[str, str]:
    """Resolve a sink's relative paths into paths on THIS machine.

    A relative path in a record means "under the store's own area", so a record
    written by a PC and adopted by a NAS names one directory rather than one
    machine's view of it. Absolute paths are left exactly as written.
    """
    return sink.resolve(local_root(store))


@dataclass
class Checkpoint:
    """How many leading bytes of the partial are KNOWN to be the artifact's real
    bytes. A new owner may resume only from here. There is no field meaning
    'trust the part I did not check', so curl's mistake is not expressible."""

    verified_prefix: int = 0


@dataclass
class Spec:
    artifact: Artifact = field(default_factory=Artifact)
    sources: List[Source] = field(default_factory=list)
    sink: Sink = field(default_factory=Sink)

    def validate(self) -> None:
        if not self.sources:
            raise DownloadError("download: at least one source is required")
        for i, s in enumerate(self.sources):
            if not s.scheme.strip():
                raise DownloadError(f"download: source {i}: scheme is required")
            if not s.locator.strip():
                raise DownloadError(f"download: source {i}: locator is required")
        if not self.sink.final.strip():
            raise DownloadError("download: sink final path is required")
        if self.artifact.digest and not self.artifact.digest.startswith("sha256:"):
            raise DownloadError(
                f"download: digest {self.artifact.digest!r} is not sha256:<hex>"
            )

    def to_dict(self) -> dict:
        d: dict = {"artifact": {}, "sources": [], "sink": {}}
        if self.artifact.digest:
            d["artifact"]["digest"] = self.artifact.digest
        if self.artifact.size:
            d["artifact"]["size"] = self.artifact.size
        for s in self.sources:
            one: dict = {"scheme": s.scheme, "locator": s.locator}
            if s.attrs:
                one["attrs"] = dict(s.attrs)
            if s.priority:
                one["priority"] = s.priority
            d["sources"].append(one)
        d["sink"] = {"partial": self.sink.partial, "final": self.sink.final}
        return d

    @staticmethod
    def from_dict(d: dict) -> "Spec":
        a = d.get("artifact") or {}
        sk = d.get("sink") or {}
        return Spec(
            artifact=Artifact(digest=a.get("digest", ""), size=int(a.get("size", 0) or 0)),
            sources=[
                Source(
                    scheme=s.get("scheme", ""),
                    locator=s.get("locator", ""),
                    attrs=dict(s.get("attrs") or {}),
                    priority=int(s.get("priority", 0) or 0),
                )
                for s in (d.get("sources") or [])
            ],
            sink=Sink(partial=sk.get("partial", ""), final=sk.get("final", "")),
        )


# --------------------------------------------------------------- portability ---

_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")


def _relative_everywhere(p: str) -> bool:
    """Is p relative under BOTH conventions?

    os.path.isabs alone answers for the OS running it: on Linux
    "D:\\models\\x.gguf" is "relative", and joining it onto the store root would
    silently produce a directory literally named "D:\\models" on the NAS. A path
    absolute anywhere is treated as absolute everywhere, so a mistake surfaces as
    a plain "no such file" rather than as a strange one.
    """
    if not p:
        return False
    if p.startswith("/") or p.startswith("\\"):
        return False  # POSIX absolute, or a UNC path
    if _WINDOWS_DRIVE.match(p):
        return False
    return not ntpath.isabs(p) and not posixpath.isabs(p)


def _resolve_under(root: str, p: str) -> str:
    if not p or not _relative_everywhere(p):
        return p
    # Forgive a backslash in a RELATIVE path: records written before separators
    # were normalised are on disk already, and a backslash cannot legally appear
    # in a Windows filename anyway, so reading it as a separator is the only
    # interpretation ever right.
    return os.path.join(root, *[seg for seg in p.replace("\\", "/").split("/") if seg])


def portable(p: str) -> str:
    """Put a relative path into the one form every machine reads the same way.

    os.path.join on Windows produces "models\\x.gguf", and on Linux that is not a
    directory and a file — it is ONE file whose name contains a backslash. The
    job would "succeed" and put the weights somewhere nobody would ever look.
    """
    if not _relative_everywhere(p):
        return p
    return p.replace("\\", "/")


# -------------------------------------------------------------- submitting ---


def submit(store: Store, spec: Spec, requires: Optional[List[str]] = None) -> str:
    """Create a download job."""
    spec.validate()
    spec.sink.partial = portable(spec.sink.partial)
    spec.sink.final = portable(spec.sink.final)

    rec = Record(id="", kind=KIND, spec=spec.to_dict(), requires=list(requires or []))
    rec.progress.total = spec.artifact.size
    job_id = store.submit(rec)

    # The partial goes under the store's own work directory, relative and
    # slash-separated, so a successor on another machine can find what a
    # predecessor left without either agreeing on a convention.
    if not spec.sink.partial:
        spec.sink.partial = "work/" + job_id
        held = store.claim(job_id, "submit", 30)
        def set_partial(r: Record) -> None:
            r.spec = spec.to_dict()
        store.update(job_id, held.lease.epoch, set_partial)
        store.release(job_id, held.lease.epoch)
    return job_id


def spec_of(rec: Record) -> Spec:
    if rec.kind != KIND:
        raise DownloadError(f"download: job {rec.id} is kind {rec.kind!r}, not {KIND!r}")
    return Spec.from_dict(rec.spec or {})


def checkpoint_of(rec: Record) -> Checkpoint:
    cp = rec.checkpoint or {}
    return Checkpoint(verified_prefix=int(cp.get("verified_prefix", 0) or 0))


# ------------------------------------------- resuming, keyed on destination ---
#
# submit answers "start this work and give me its id". That is the right question
# for a program that keeps the id -- a UI, a service, a queue. It is the wrong
# question for a program run again from a shell, which has no id to keep: it has
# a URL and a path, and what it means by "resume" is "the file at this path is
# half here, carry on with it". Such a caller submits a fresh record on every
# invocation, with a verified prefix of zero, and never sends a Range request.
#
# Both existing adopters wrote this on top of the library rather than getting it
# from it: the ComfyUI node's _resume_or_submit, and the same loop in C++ in the
# Lemonade fork.

#: Nothing in the store was working on this destination, so a new record was
#: created. The only disposition meaning no bytes survive from an earlier try.
SUBMITTED = "submitted"
#: An unfinished record for this destination was adopted. Continuation.resume_from
#: says how many of its bytes survive.
RESUMED = "resumed"
#: The artifact is already at the destination and no bytes need to move. The
#: returned job may still be waiting for take_delivery.
DELIVERED = "delivered"
#: An unfinished record was adopted and another owner holds its lease right now.
#: The lease was not taken. Watch the job; if the owner is dead its lease lapses
#: within the runner's lease TTL and the ordinary reclamation path picks it up.
BUSY = "busy"
#: An unfinished record was adopted and somebody asked it to stop. Clear the
#: intent through the job layer before expecting bytes to move.
PAUSED = "paused"


@dataclass
class Continuation:
    """What resume_or_submit decided, in a form a caller can print.

    Nothing here is a branch a caller must take: the returned job id is correct
    whatever this says. It exists because every decision below is one a person
    running a command is entitled to see rather than have made silently.
    """

    #: Which of SUBMITTED, RESUMED, DELIVERED, BUSY, PAUSED applied.
    disposition: str = SUBMITTED
    #: The byte the next attempt will actually start at, measured against the
    #: filesystem and not against the record. A record claiming a verified prefix
    #: of 900 whose partial holds 100 bytes -- or none -- yields 100, or 0.
    resume_from: int = 0
    #: The locator the adopted job will fetch from, which is not necessarily the
    #: one just passed in. See source_changed.
    source: str = ""
    #: An existing record was adopted whose first source differs from the
    #: caller's.
    #:
    #: The destination is the identity, so this is the same work: a URL changes
    #: when a mirror is configured (HF_ENDPOINT does exactly this), when a signed
    #: link is reissued, or when a redirect resolves differently, and none of
    #: those are a different file. The adopted record keeps its OWN sources, so
    #: the partial on disk stays consistent with what wrote it -- a caller that
    #: meant a genuinely different artifact should cancel this job and submit to
    #: a different path rather than have its bytes appended to another prefix.
    source_changed: bool = False
    #: One line of display text saying the same thing. Never parse it.
    note: str = ""


def resume_or_submit(
    store: Store,
    spec: Spec,
    requires: Optional[List[str]] = None,
    owner_name: Optional[str] = None,
) -> Tuple[str, Continuation]:
    """The job for spec's destination, continuing the existing one if there is
    one. Returns ``(job_id, Continuation)``.

    Call it instead of submit whenever the caller identifies work by where the
    bytes land. Unlike the Go binding, which hands the job to a supervisor or
    starts it in the background, this returns and leaves running to the caller:
    ``Runner(store).run(job_id)``, exactly as after submit. Python has no
    Service, so there is nowhere here for that decision to live.

    The rules, in the order they are applied:

      - A destination is required. Identity IS the destination, so there is no
        meaning to this call without one.

      - Two spellings of one destination are one record. The path is made
        absolute, normalised, and its parent directory resolved through
        symlinks; on Windows it is also case-folded. What that cannot see: a
        file reached through two mounts of one filesystem, a hardlink, a drive
        letter mapped to a UNC share, an 8.3 short name, and -- on a
        case-insensitive macOS or Linux volume -- two spellings differing only
        in case. Each of those yields two records for one file, which is the
        failure this call exists to remove, so a caller that can hand over one
        spelling should.

      - An unfinished record for that destination is continued, whatever its
        source. See Continuation.source_changed.

      - COMPLETE means the file is there, and that is checked rather than
        believed: a complete record whose file has been moved or deleted is
        history, and a new job is created. FAILED and CANCELLED are history too
        -- running a command again is a new request, not an appeal against the
        last one -- and neither carries bytes forward, because nothing proved
        them.

      - A lease held by another owner is left alone: nothing is claimed here,
        and the record comes back with disposition BUSY. The work is offered to
        this process only once that lease lapses of its own accord, which is
        what recovers a download whose owner was killed rather than stopped.

      - The resume point is the smaller of the checkpoint and the size of the
        partial file, so a vanished or truncated partial cannot make a runner
        ask a server to continue from bytes that are not there.

      - Two callers racing for one destination produce one record, because the
        record id is derived from the destination and the store refuses to
        create an id twice. The loser of the race loads the winner's record and
        continues it. No lock is introduced here; the store's own exclusion is
        the whole mechanism.
    """
    spec.validate()
    dest = _destination_of(store, spec.sink)
    if not dest:
        raise DownloadError("download: resume_or_submit needs a destination")
    me = owner_name or owner()

    live, delivered = _records_for(store, dest)
    if live is not None:
        return live.id, _continuation(store, live, spec, me)
    if delivered is not None:
        return delivered.id, Continuation(
            disposition=DELIVERED,
            source=_first_locator(delivered),
            note="already downloaded",
        )

    job_id, created = _claim_destination(store, dest, spec, requires)
    if not created:
        # Somebody won the race between the scan above and the create. Their
        # record is the one record for this destination, so continue it.
        return job_id, _continuation(store, store.load(job_id), spec, me)
    return job_id, Continuation(
        disposition=SUBMITTED,
        source=spec.sources[0].locator,
        note="starting a new download",
    )


def resume_or_get(
    store: Store,
    source: str,
    destination: str,
    requires: Optional[List[str]] = None,
    owner_name: Optional[str] = None,
) -> Tuple[str, Continuation]:
    """resume_or_submit for a caller holding a URL and a path, which is the shape
    a command-line tool has. A destination naming a directory takes its filename
    from the source."""
    return resume_or_submit(
        store, _spec_for(source, destination), requires=requires, owner_name=owner_name
    )


def _spec_for(source: str, destination: str) -> Spec:
    """A URL and a path as a spec, with no digest, because a caller holding only
    those two does not have one."""
    if not source or not source.strip():
        raise DownloadError("download: no source")
    dest = destination or "."
    if dest.endswith("/") or dest.endswith("\\") or dest == "." or os.path.isdir(dest):
        dest = os.path.join(dest, _name_from(source))
    # Absolute, because the process that finally moves these bytes may have a
    # different working directory, a different user, or be on another machine.
    return Spec(
        sources=[Source(scheme=_scheme_from(source), locator=source)],
        sink=Sink(final=os.path.abspath(dest)),
    )


def _name_from(locator: str) -> str:
    locator = locator.split("?", 1)[0]
    name = posixpath.basename(locator)
    if not name or name in ("/", "."):
        return "download.bin"
    return name


def _scheme_from(locator: str) -> str:
    i = locator.find("://")
    return locator[:i] if i > 0 else "https"


def _continuation(store, rec: Record, want: Spec, me: str) -> Continuation:
    """What an existing unfinished record means for this call."""
    c = Continuation(disposition=RESUMED, source=_first_locator(rec))
    c.source_changed = bool(want.sources) and bool(c.source) and (
        c.source != want.sources[0].locator
    )

    if rec.state == TRANSFERRED and _destination_exists(store, rec):
        c.disposition = DELIVERED
        c.note = "already downloaded, waiting to be taken delivery of"
        return c

    c.resume_from = _resume_from(store, rec)
    if rec.paused():
        c.disposition = PAUSED
        c.note = "an existing download to this path is paused"
    elif not store.claimable(rec) and rec.lease.owner != me:
        c.disposition = BUSY
        c.note = f"{rec.lease.owner} is already downloading to this path"
    elif c.resume_from > 0:
        c.note = f"continuing an existing download from byte {c.resume_from}"
    else:
        c.note = "continuing an existing download from the beginning"
    if c.source_changed:
        c.note += "; it fetches from " + c.source
    return c


def _resume_from(store, rec: Record) -> int:
    """What survives on disk, never what the record claims.

    A checkpoint is written periodically, so a partial file is normally AHEAD of
    it -- but it can also be behind it, or gone: a temp cleaner, a half-finished
    copy onto a full disk, a user tidying up. Believing the record in that case
    makes a runner send "Range: bytes=900-" for a file holding a hundred bytes,
    and the answer is appended to those hundred. The file then has the right
    length and the wrong contents, which is the one outcome worth more than a
    re-download.
    """
    try:
        spec = spec_of(rec)
    except DownloadError:
        return 0
    proven = checkpoint_of(rec).verified_prefix
    if proven <= 0:
        return 0
    partial, _ = local_sink(store, spec.sink)
    try:
        st = os.stat(partial)
    except OSError:
        return 0
    if stat.S_ISDIR(st.st_mode):
        return 0
    return min(st.st_size, proven)


def _destination_exists(store, rec: Record) -> bool:
    """Whether the record's final path holds a file."""
    try:
        spec = spec_of(rec)
    except DownloadError:
        return False
    _, final = local_sink(store, spec.sink)
    return os.path.isfile(final)


def _records_for(store, dest: str) -> Tuple[Optional[Record], Optional[Record]]:
    """This destination's records: work to continue, and a COMPLETE one.

    Two, because the two are treated differently: an unfinished record is work to
    continue, and a COMPLETE one is only evidence that the file might already be
    there. The scan is over every record rather than over ids this module would
    have chosen, so a job submitted by an older version, by the CLI, or by the Go
    implementation is still found.
    """
    live = delivered = None
    try:
        records = store.list()
    except Exception:
        return None, None
    for rec in records:
        if rec.kind != KIND:
            continue
        try:
            spec = spec_of(rec)
        except Exception:
            continue
        if _destination_of(store, spec.sink) != dest:
            continue
        if not rec.terminal():
            live = rec
        elif rec.state == COMPLETE and _destination_exists(store, rec):
            delivered = rec
    return live, delivered


def _claim_destination(
    store, dest: str, spec: Spec, requires: Optional[List[str]]
) -> Tuple[str, bool]:
    """Create the one record for this destination, or return the id of the record
    somebody else created first.

    The exclusion is the store's, not this module's: submit refuses an id that
    already exists -- O_EXCL in the file binding, a dict check in the memory one
    -- so deriving the id from the destination turns two concurrent creates into
    one winner and one loser that can read what the winner wrote.

    The generation suffix is what keeps that compatible with "download it again":
    a destination whose first record ended failed, cancelled, or complete with no
    file needs a second record, and it cannot have the same id as the first.
    """
    base = _destination_id(dest)
    for generation in range(1, 513):
        job_id = base if generation == 1 else f"{base}-{generation}"
        try:
            return _submit_as(store, job_id, spec, requires), True
        except Exception as taken:
            refusal = taken
        # Either somebody just created it, in which case it is the record for
        # this destination and the caller continues it, or it is spent history
        # from an earlier run and the next generation is free.
        try:
            rec = store.load(job_id)
        except Exception:
            raise refusal
        if not rec.terminal():
            return job_id, False
    raise DownloadError(
        f"download: {dest} has too many spent records to start another"
    )


def _submit_as(
    store: Store, job_id: str, spec: Spec, requires: Optional[List[str]] = None
) -> str:
    """submit with the id chosen by the caller rather than by the store.

    The id is the store's only exclusion primitive, so a caller that can compute
    an id from the work itself gets create-or-find without a lock.
    """
    spec.validate()
    # A copy: the caller's spec is theirs, and a partial path filled in here must
    # not appear in it.
    spec = Spec.from_dict(spec.to_dict())
    spec.sink.partial = portable(spec.sink.partial)
    spec.sink.final = portable(spec.sink.final)
    if not spec.sink.partial:
        # Relative, deliberately. The store knows where its work directory is on
        # whichever machine is asking.
        spec.sink.partial = "work/" + job_id
    rec = Record(id=job_id, kind=KIND, spec=spec.to_dict(), requires=list(requires or []))
    rec.progress.total = spec.artifact.size
    return store.submit(rec)


def _destination_id(dest: str) -> str:
    """The record id for a destination: stable, identical in Go and Python, and
    shaped so that it cannot collide with the timestamped ids the job layer
    invents.

    It sorts after those ids rather than among them, so a listing ordered by id
    puts records made this way at the end. Anything wanting creation order has
    Record.created_at.
    """
    return "dest-" + hashlib.sha256(dest.encode("utf-8")).hexdigest()[:16]


def _destination_of(store, sink: Sink) -> str:
    """A sink reduced to the one string that identifies its file.

    A relative sink means "under the store's own area", so it is resolved the
    same way the runner will resolve it -- otherwise a record written as
    "out/x.bin" and a call naming the absolute path of that same file would be
    two records.
    """
    _, final = local_sink(store, sink)
    return _canonical_path(final)


def _canonical_path(p: str) -> str:
    """This module's answer to "are these two strings the same file", and its
    limits are listed on resume_or_submit.

    The parent directory is resolved through symlinks rather than the file
    itself, because the file is usually not there yet -- that is the whole
    situation. A directory that cannot be resolved is used as written, which is
    right for a destination whose directory has still to be created, and is what
    the Go implementation does; resolving it partially instead would give the two
    implementations different ids for one path.
    """
    if not p or not p.strip():
        return ""
    if os.sep != "/":
        p = p.replace("/", os.sep)
    p = os.path.abspath(p)
    directory, base = os.path.split(p)
    try:
        directory = os.path.realpath(directory, strict=True)
    except OSError:
        pass
    p = os.path.join(directory, base)
    if os.name == "nt":
        # Windows filenames are case-insensitive, so folding here merges two
        # spellings of one file. Not done elsewhere: a case-sensitive Linux
        # volume holds "X.bin" and "x.bin" as two files, and folding them
        # together would point two downloads at one record.
        p = p.lower()
    return p


def _first_locator(rec: Record) -> str:
    try:
        spec = spec_of(rec)
    except DownloadError:
        return ""
    return spec.sources[0].locator if spec.sources else ""


# ------------------------------------------------------------- credentials ---


def credentials(name: str) -> Dict[str, str]:
    """Resolve a credential NAME into headers.

    The record holds only the name. The secret comes from the environment at the
    moment of the request, because a record is deliberately readable by every
    other process — that is what makes progress observable — and so it is the
    last place a secret may live.
    """
    if not name:
        return {}
    env = "ABSTRACTION_CRED_" + name.upper()
    token = os.environ.get(env, "")
    if not token and name.lower() == "hf":
        token = os.environ.get("HF_TOKEN", "")
    if not token:
        # Refuse rather than fetch anonymously. A public 404 in place of a gated
        # file is a confusing failure; "you did not set this" is not.
        raise DownloadError(
            f"download: source needs credential {name!r} but ${env} is not set"
        )
    return {"Authorization": "Bearer " + token}


# ------------------------------------------------------------------ runner ---


def store_root() -> str:
    """Where jobs live on this machine, matching the Go implementation."""
    for name in ("ABSTRACTION_STORE", "MODELGET_STORE"):
        v = os.environ.get(name)
        if v:
            return v
    home = os.path.expanduser("~")
    legacy = os.path.join(home, ".modelget")
    if os.path.isdir(legacy):
        return legacy
    return os.path.join(home, ".abstraction")


@dataclass
class Supervisor:
    """A process that watches this store and finishes work nobody is doing.

    Presence is the configuration. An application does not choose a downloader:
    it asks whether anything is watching, and if something is, it submits the
    work and stops caring who moves the bytes.
    """

    owner: str = ""
    host: str = ""
    pid: int = 0
    seen: Optional[datetime] = None
    every: str = ""
    tier: str = ""


def supervisor_of(store) -> Tuple[Supervisor, bool]:
    """The live supervisor for this store, if there is one.

    "Live" means the heartbeat is younger than three of its own intervals --
    enough to survive a slow sweep or a jittery clock, short enough that a killed
    supervisor stops attracting work within a minute or two rather than forever.
    A supervisor that was killed leaves a stale file behind, and an application
    that trusted it would submit work into a store nobody is watching, which
    looks exactly like a download that started and never progressed.

    A store whose binding is not a filesystem has no heartbeat to read, and
    answers no rather than pretending.
    """
    root = local_root(store)
    if not root:
        return Supervisor(), False
    try:
        with open(os.path.join(root, "supervisor.json"), "rb") as fh:
            d = json.load(fh)
    except (OSError, ValueError):
        return Supervisor(), False

    s = Supervisor(
        owner=d.get("owner", ""),
        host=d.get("host", ""),
        pid=int(d.get("pid", 0) or 0),
        every=d.get("every", ""),
        tier=d.get("tier", "") or "",
    )
    try:
        s.seen = _parse_time(d["seen"])
    except Exception:
        return s, False

    every = _parse_go_duration(s.every)
    if every <= 0:
        every = 30.0
    age = (datetime.now(timezone.utc) - s.seen).total_seconds()
    return s, age <= 3 * every


def _parse_go_duration(text: str) -> float:
    """Go writes "30s", "1m30s", "2h45m0s". Parsed here because the heartbeat is
    written by whichever implementation happens to be supervising, and this one
    must read what that one wrote."""
    if not text:
        return 0.0
    units = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}
    total = 0.0
    for value, unit in re.findall(r"([0-9]*\.?[0-9]+)(ns|us|ms|s|m|h)", text):
        total += float(value) * units[unit]
    return total


def owner(program: Optional[str] = None) -> str:
    """program@host:pid.

    The program name is read from the executable rather than supplied, for the
    same reason the Go side does it: asking an application to state its own name
    is asking for a claim.
    """
    if program is None:
        import sys

        program = os.path.basename(sys.argv[0] or "python")
        if program.endswith(".py"):
            program = program[:-3]
    return f"{program}@{socket.gethostname()}:{os.getpid()}"


class Runner:
    """Claim a job, get the bytes, prove them, deliver them.

    Everything that must be identical across implementations lives here rather
    than in the transport: hashing, resume, progress persistence, lease renewal,
    the final rename. That is what lets a transfer begun by Go be finished by
    Python, which is the entire premise.
    """

    def __init__(
        self,
        store: Store,
        owner_name: Optional[str] = None,
        lease_ttl: float = 30.0,
        persist_every: int = 8 << 20,
        persist_interval: float = 5.0,
    ):
        self.store = store
        self.owner = owner_name or owner()
        self.lease_ttl = lease_ttl
        # Bytes OR time. A byte threshold alone is silently wrong on a slow link:
        # a real 313 MB download killed after 12 seconds had checkpointed nothing
        # because it had not reached 8 MiB, and a slow connection is exactly when
        # resuming matters most.
        self.persist_every = persist_every
        self.persist_interval = persist_interval

    def run(self, job_id: str) -> None:
        rec = self.store.claim(job_id, self.owner, self.lease_ttl)
        epoch = rec.lease.epoch
        try:
            self._run(rec, epoch)
        except Exception as e:
            # Record why, so a human reading the job later does not have to find
            # the log of a process that no longer exists.
            def note(r: Record) -> None:
                r.error = str(e)

            try:
                self.store.update(job_id, epoch, note)
            except Exception:
                pass
            raise
        # Let go: the bytes are proven, and the only thing left is for whoever
        # wanted them to say so, which means claiming this job.
        try:
            self.store.release(job_id, epoch)
        except Exception:
            pass

    def _run(self, rec: Record, epoch: int) -> None:
        spec = spec_of(rec)
        cp = checkpoint_of(rec)
        partial, final = local_sink(self.store, spec.sink)

        os.makedirs(os.path.dirname(partial) or ".", exist_ok=True)

        # Resume point: the SMALLER of what the checkpoint proved and what the
        # file actually holds. They differ after a crash, and trusting either
        # alone is how a resumed download ends up the right length and the wrong
        # bytes.
        on_disk = os.path.getsize(partial) if os.path.exists(partial) else 0
        start = min(cp.verified_prefix, on_disk)
        _truncate(partial, start)

        h = hashlib.sha256()
        if start:
            _hash_prefix(partial, start, h)

        total = self._fetch(rec, spec, epoch, partial, h, start)

        if spec.artifact.size and total != spec.artifact.size:
            raise ShortTransfer(
                f"download: got {total} bytes, expected {spec.artifact.size}"
            )

        if spec.artifact.digest:
            got = "sha256:" + h.hexdigest()
            if got.lower() != spec.artifact.digest.lower():
                # Do not keep bytes that failed: leaving them means the next
                # runner resumes onto a prefix already known to be wrong.
                try:
                    os.remove(partial)
                except OSError:
                    pass

                def reset(r: Record) -> None:
                    r.progress.done = 0
                    r.checkpoint = {"verified_prefix": 0}

                self.store.update(rec.id, epoch, reset)
                raise DigestMismatch(
                    f"download: got {got}, want {spec.artifact.digest}"
                )

        _deliver(partial, final)

        # Transferred, not complete: the bytes are here and proven, but whoever
        # wanted them has not said so yet.
        def done(r: Record) -> None:
            r.progress.done = total
            r.state = TRANSFERRED
            r.error = ""
            r.checkpoint = {"verified_prefix": total}

        self.store.update(rec.id, epoch, done)

    def take_delivery(self, job_id: str) -> None:
        """The requester saying "I have it". Without this a finished job waits in
        the store forever for somebody who never comes."""
        rec = self.store.load(job_id)
        if rec.state == COMPLETE:
            return
        if rec.state != TRANSFERRED:
            raise DownloadError(f"download: {job_id} is {rec.state}, not {TRANSFERRED}")
        try:
            held = self.store.claim(job_id, self.owner, self.lease_ttl)
        except Exception:
            return  # somebody else is mid-delivery; the bytes are still proven

        def mark(r: Record) -> None:
            r.state = COMPLETE

        self.store.update(job_id, held.lease.epoch, mark)
        self.store.release(job_id, held.lease.epoch)

    def adopt(self) -> int:
        """Finish work nobody is doing. The primary reclamation path, not a
        fallback: a SIGKILLed process never hands anything over."""
        n = 0
        for o in self.store.orphans():
            if o.kind != KIND or o.delegated():
                continue
            try:
                self.run(o.id)
                n += 1
            except Exception:
                continue  # one bad job must not stop the rest being rescued
        return n

    # -- transport ---------------------------------------------------------

    def _fetch(
        self, rec: Record, spec: Spec, epoch: int, partial: str, h, start: int
    ) -> int:
        ordered = sorted(spec.sources, key=lambda s: s.priority)
        last: Optional[Exception] = None
        for src in ordered:
            try:
                return self._fetch_one(rec, src, epoch, partial, h, start)
            except Exception as e:  # try the next source; mirrors exist for this
                last = e
        raise last or NoSource("download: no source could be used")

    def _fetch_one(
        self, rec: Record, src: Source, epoch: int, partial: str, h, start: int
    ) -> int:
        if src.scheme in ("http", "https"):
            return self._fetch_http(rec, src, epoch, partial, h, start)
        if src.scheme in ("file", "smb"):
            return self._fetch_file(rec, src, epoch, partial, h, start)
        raise NoSource(f"download: no transport for scheme {src.scheme!r}")

    def _fetch_http(
        self, rec: Record, src: Source, epoch: int, partial: str, h, start: int
    ) -> int:
        req = urllib.request.Request(src.locator)
        for k, v in credentials(src.attrs.get(CREDENTIAL_ATTR, "")).items():
            req.add_header(k, v)
        if start:
            req.add_header("Range", f"bytes={start}-")

        with urllib.request.urlopen(req) as resp:
            # A 200 answering a ranged request means the server sent the whole
            # file. Appending it produces a file of plausible length and
            # impossible content.
            if start and resp.status != 206:
                raise RangeIgnored(
                    f"download: asked for bytes from {start}, server answered "
                    f"{resp.status} with the whole file"
                )

            # Worked out BEFORE the copy. Content-Length on a 200 is the whole
            # artifact; on a 206 it is what remains, so the offset has to be
            # added back.
            declared = 0
            try:
                length = int(resp.headers.get("Content-Length") or 0)
                if length > 0:
                    declared = start + length
            except ValueError:
                declared = 0

            total = self._drain(rec, resp, epoch, partial, h, start)

            # The transport's own promise, checked even when the spec makes
            # none. A spec without a size or a digest is the normal case for a
            # bare URL out of a model list, and without this there was NOTHING
            # to catch a truncated transfer: the partial was delivered under the
            # final name and presented to the application as a finished
            # download. That is precisely the failure this layer exists to
            # refuse, reproduced by the layer itself.
            #
            # Go gets this for free -- its HTTP client errors on a body shorter
            # than Content-Length -- and urllib does not, which is why this
            # cannot be left to the language underneath.
            if declared and total != declared:
                raise ShortTransfer(
                    f"download: got {total} bytes, the server said {declared}"
                )
            return total

    def _fetch_file(
        self, rec: Record, src: Source, epoch: int, partial: str, h, start: int
    ) -> int:
        with open(src.locator, "rb") as f:
            f.seek(start)
            return self._drain(rec, f, epoch, partial, h, start)

    def _drain(self, rec: Record, reader, epoch: int, partial: str, h, start: int) -> int:
        total = start
        since_persist = 0
        last_persist = time.monotonic()
        with open(partial, "r+b" if os.path.exists(partial) else "w+b") as out:
            out.seek(start)
            try:
                while True:
                    chunk = reader.read(256 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
                    h.update(chunk)
                    total += len(chunk)
                    since_persist += len(chunk)

                    now = time.monotonic()
                    if (
                        since_persist >= self.persist_every
                        or now - last_persist >= self.persist_interval
                    ):
                        out.flush()
                        os.fsync(out.fileno())
                        self._persist(rec.id, epoch, total)
                        since_persist = 0
                        last_persist = now
            finally:
                # However this ends. Every byte counted here was written AND
                # hashed, so it is proven whether the transfer went on to
                # succeed, fail, or be killed -- and recording it is the whole
                # difference between resuming and starting over.
                #
                # Periodic checkpointing alone is not enough, and the gap is
                # worst exactly where it is least expected: a transfer that dies
                # before the FIRST checkpoint has proven nothing, so the resume
                # point is min(0, what is on disk) = 0, and a partial file
                # sitting right there is fetched again from zero. On a fast link
                # that is every download under 8 MiB; on a slow one it is the
                # first five seconds of a 40 GB model.
                out.flush()
                os.fsync(out.fileno())
                if total > start:
                    try:
                        self._persist(rec.id, epoch, total)
                    except Exception:
                        # A store that will not take the checkpoint must not
                        # replace the real error with its own.
                        pass
        return total

    def _persist(self, job_id: str, epoch: int, done: int) -> None:
        """Write down what has been proven, and renew the lease on the same beat.

        Renewal rides this callback deliberately: without it a transfer that is
        progressing slowly lets its own lease expire and is adopted as an orphan
        while still running.
        """

        def mutate(r: Record) -> None:
            r.progress.done = done
            r.state = RUNNING
            r.checkpoint = {"verified_prefix": done}

        try:
            self.store.renew(job_id, epoch, self.lease_ttl)
            self.store.update(job_id, epoch, mutate)
        except Exception:
            # A failed checkpoint is not a failed download. The worst case is
            # re-fetching from the last one that worked.
            pass


# ------------------------------------------------------------------ helpers ---


def _truncate(path: str, size: int) -> None:
    if not os.path.exists(path):
        if size == 0:
            return
        raise DownloadError(f"download: cannot resume from {size}: {path} is missing")
    with open(path, "r+b") as f:
        f.truncate(size)


def _hash_prefix(path: str, n: int, h) -> None:
    """Rebuild the rolling hash over the prefix being kept.

    This is the cost of resuming honestly: a sequential read of what we already
    have, at disk speed, instead of re-downloading it at network speed. It is
    also the only way the final digest check covers bytes an earlier owner wrote.
    """
    left = n
    with open(path, "rb") as f:
        while left > 0:
            chunk = f.read(min(1 << 20, left))
            if not chunk:
                raise DownloadError("download: partial is shorter than claimed")
            h.update(chunk)
            left -= len(chunk)


def _deliver(partial: str, final: str) -> None:
    os.makedirs(os.path.dirname(final) or ".", exist_ok=True)
    try:
        os.replace(partial, final)
    except OSError:
        # Across filesystems rename fails; copy then remove.
        with open(partial, "rb") as src, open(final, "wb") as dst:
            while True:
                b = src.read(1 << 20)
                if not b:
                    break
                dst.write(b)
        os.remove(partial)
