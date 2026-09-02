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
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from abstraction_job import FileStore, Record, RUNNING, TRANSFERRED, COMPLETE

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


def submit(store: FileStore, spec: Spec, requires: Optional[List[str]] = None) -> str:
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
        store: FileStore,
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
        partial, final = spec.sink.resolve(self.store.root)

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
            return self._drain(rec, resp, epoch, partial, h, start)

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
            out.flush()
            os.fsync(out.fileno())
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
