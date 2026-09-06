"""replay applies a scripted sequence of operations to a job store and prints
what an observer would have seen after each one.

The Python half of scripts/behaviour-conformance.sh. Same scenario file, same
transcript, no knowledge of any other implementation. See the Go replay for why
the transcript carries what it carries.
"""

import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone

import abstraction_download as dl
from abstraction_job import (
    CANCELLED,
    COMPLETE,
    FAILED,
    FileStore,
    Invalid,
    JobError,
    KeepAwake,
    KNOWN_FEATURES,
    LeaseExpired,
    LeaseHeld,
    NEVER_CRITICAL,
    NotFound,
    PENDING,
    RUNNING,
    StaleEpoch,
    Terminal,
    TRANSFERRED,
    UnknownSchema,
    watch,
)
from abstraction_watch import Closed

STATES = (PENDING, RUNNING, TRANSFERRED, COMPLETE, FAILED, CANCELLED)


def fixture():
    """Where download/testdata/fixture.py is listening, or "" when the harness
    started no server. A driver that cannot reach one must not claim the wire
    capability: a scenario nobody ran is counted as unproven, and a scenario
    silently skipped reads as a pass."""
    return os.environ.get("ABSTRACTION_FIXTURE", "")


def capabilities():
    return "store transfer wire wanted" if fixture() else "store transfer"


def models():
    """Every content-set name this implementation can read, and whether it may
    be marked critical. A record naming anything absent here in ``critical`` is
    refused, so the roster is the whole of what this reader negotiates over and
    it belongs where a harness can diff it against the other two."""
    return "\n".join(
        f"{name} {'never-critical' if name in NEVER_CRITICAL else 'critical-ok'}"
        for name in sorted(KNOWN_FEATURES)
    )


def now():
    return datetime.now(timezone.utc)

REFUSALS = [
    (NotFound, "not-found"),
    (LeaseHeld, "lease-held"),
    (StaleEpoch, "stale-epoch"),
    (LeaseExpired, "lease-expired"),
    (Terminal, "terminal"),
    (UnknownSchema, "unknown-model"),
    (Invalid, "invalid"),
]


def outcome(exc):
    """Name a refusal in a vocabulary all three implementations share. The
    wording of an error is not a contract and must never become one; which class
    of refusal happened is exactly what a caller branches on."""
    if exc is None:
        return "ok"
    for kind, name in REFUSALS:
        if isinstance(exc, kind):
            return name
    return "refused"


def artifact(size):
    """Content is a function of the offset alone, so every implementation makes
    the same file and the same digest without being told what it is."""
    return bytes(i % 251 for i in range(size))


def final_for(alias, kind):
    """Name the sink. ``foreign`` is an absolute path in the OTHER platform's
    convention, and the driver spells it rather than the scenario because which
    spelling is foreign depends on the host: a scenario that named ``/mnt/...``
    would assert one thing on Windows and its opposite on Linux, and the
    behaviour under test is the same on both."""
    if kind == "foreign":
        if os.name == "nt":
            return "/mnt/store/models/" + alias + ".bin"
        return "C:\\store\\models\\" + alias + ".bin"
    if kind == "abs":
        # THIS machine's own absolute convention, so foreign_path does not catch
        # it: the exact hole a shared-store runner must close.
        if os.name == "nt":
            return "C:\\abstraction-deputy\\" + alias + ".bin"
        return "/etc/cron.d/" + alias
    return "models/" + alias + ".bin"


class Replay:
    def __init__(self, work):
        os.makedirs(os.path.join(work, "store"), exist_ok=True)
        self.store = FileStore(os.path.join(work, "store"))
        self.work = work
        self.ids = {}
        self.epochs = {}
        self.subs = {}
        self.holds = {}
        self.refused = {}

    def do(self, f):
        op = f[0]
        if op == "submit":
            return self.submit(f[1], dict(a.split("=", 1) for a in f[2:] if "=" in a))
        if op == "claim":
            return self.claim(f[1], f[2], int(f[3]))
        if op == "renew":
            return self.renew(f[1], f[2], int(f[3]))
        if op == "progress":
            return self.progress(f[1], f[2], int(f[3]), " ".join(f[4:]))
        if op == "hold":
            return self.hold(f[1])
        if op == "release":
            return self.release(f[1], f[2])
        if op == "finish":
            return self.finish(f[1], f[2], f[3])
        if op == "intent":
            return self.intent(f[1], f[2])
        if op == "recall":
            return self.recall(f[1], f[2], int(f[3]), " ".join(f[4:]))
        if op == "state":
            return self.show(f[1], None)
        if op == "orphans":
            return self.orphans()
        if op == "run":
            return self.run(f[1], f[2])
        if op == "credential":
            return self.credential(f[1], f[2] if len(f) > 2 else "-")
        if op == "runshared":
            return self.run(f[1], f[2], shared=True)
        if op == "refuse":
            self.refused[f[1]] = " ".join(f[2:])
            return "ok"
        if op == "allow":
            self.refused.pop(f[1], None)
            return "ok"
        if op == "stage":
            return self.stage(f[1], int(f[2]), f[3] if len(f) > 3 else "")
        if op == "plant":
            return self.plant(f[1], f[2], f[3])
        if op == "watch":
            return self.watch(f[1], int(f[2]) if len(f) > 2 else 0)
        if op == "next":
            return self.next(f[1])
        if op == "close":
            return self.close(f[1])
        if op == "sleep":
            time.sleep(int(f[1]) / 1000.0)
            return "ok"
        if op == "drop":
            return self.drop(f[1], dict(a.split("=", 1) for a in f[2:] if "=" in a))
        if op == "sweep":
            return self.sweep()
        return "unknown-op"

    def drop(self, name, a):
        """Put one request in the store's drop folder, spelled the way a person
        would spell it: the locator, then a digest and a destination if the
        scenario gave one. ``text`` replaces the whole line, for a request that
        is not one."""
        size = int(a.get("size", "0"))
        body = artifact(size)
        line = a.get("text", "")
        if not line:
            kind = a.get("src", "")
            if kind == "file":
                line = "file:///" + os.path.join(self.work, "artifact.bin").replace("\\", "/")
            elif kind.startswith("http:"):
                line = f"{fixture()}/{kind[len('http:'):]}/{size}"
            else:
                line = kind
            if a.get("digest") == "good":
                line += " sha256:" + hashlib.sha256(body).hexdigest()
            elif a.get("digest") == "bad":
                line += " sha256:" + "0" * 64
            if a.get("dest"):
                line += " " + a["dest"]
        d = os.path.join(self.work, "store", "wanted")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, name), "w", encoding="utf-8", newline="\n") as f:
            f.write(line + "\n")
        return "ok"

    def sweep(self):
        """One pass of the drop folder, then what a person sees in it: each
        request and the answer its name carries. An accepted request's first
        job becomes the alias, so the scenario can run and inspect it."""
        w = dl.Wanted(self.store, lambda s: dl.submit(self.store, s))
        w.answer()
        w.take_in()
        d = w.dir()
        seen = []
        for entry in sorted(os.listdir(d)):
            name, _, state = entry.partition(".")
            seen.append(f"{name}={state}")
            if state != "accepted":
                continue
            with open(os.path.join(d, entry), encoding="utf-8") as f:
                for line in f:
                    if line.startswith("# job ") and " -> " in line:
                        self.ids[name] = line[len("# job "):].split(" -> ", 1)[0]
                        break
        return "ok " + " ".join(seen)

    def fields(self, rec):
        held = "yes" if rec.lease.held(now()) else "no"
        recall = rec.lease.recall.reason if rec.lease.recalled() else "none"
        cp = json.dumps(rec.checkpoint, separators=(",", ":")) if rec.checkpoint else "none"
        hold = self.holds.get(rec.id)
        awake = "yes" if hold is not None and hold.held() else "no"
        return (
            f"state={rec.state} epoch={rec.lease.epoch} held={held} recall={recall} want={rec.wants()} "
            f"done={rec.progress.done} err={'set' if rec.error else 'none'} cp={cp} "
            f"content={','.join(rec.content)} crit={','.join(rec.critical)} awake={awake}"
        )

    def hold(self, alias):
        """Keeps the machine awake for the lease the record carries right now,
        the way a runner does for the lease it just claimed."""
        try:
            rec = self.store.load(self.ids[alias])
        except JobError as e:
            return outcome(e)
        self.holds[rec.id] = KeepAwake(self.store, rec)
        return self.show(alias, None)

    def show(self, alias, exc):
        """The verdict, then what the record looks like from outside. Printed
        even when the operation was refused: what a refusal leaves behind is the
        half of it a caller has to live with."""
        try:
            rec = self.store.load(self.ids[alias])
        except JobError as e:
            return outcome(e)
        return outcome(exc) + " " + self.fields(rec)

    def credential(self, name, hosts):
        """Hold the canary token under ``name``, bound to ``hosts`` -- what a
        machine that holds a secret looks like to the runner."""
        key = "ABSTRACTION_CRED_" + name.upper()
        os.environ[key] = "hf_thisMustNeverAppearOnDisk_EXAMPLE"
        os.environ[key + "_HOSTS"] = "" if hosts == "-" else hosts
        return "ok"

    def submit(self, alias, a):
        size = int(a.get("size", "0"))
        body = artifact(size)
        src = os.path.join(self.work, "artifact.bin")
        with open(src, "wb") as f:
            f.write(body)

        spec = dl.Spec(sink=dl.Sink(final=final_for(alias, a.get("sink", ""))))
        spec.artifact.size = size
        if a.get("digest") == "good":
            spec.artifact.digest = "sha256:" + hashlib.sha256(body).hexdigest()
        elif a.get("digest") == "bad":
            spec.artifact.digest = "sha256:" + "0" * 64
        kind = a.get("src", "")
        if kind == "file":
            spec.sources = [dl.Source(scheme="file", locator=src)]
        elif kind == "missing":
            spec.sources = [dl.Source(scheme="file", locator=os.path.join(self.work, "absent.bin"))]
        elif kind == "nofetcher":
            spec.sources = [dl.Source(scheme="gopher", locator="gopher://example.invalid/x")]
        elif kind.startswith("http:"):
            # The behaviour is a path segment, so a new wire case needs a
            # fixture answer and a scenario and no driver in any language
            # changes.
            spec.sources = [dl.Source(
                scheme="http",
                locator=f"{fixture()}/{kind[len('http:'):]}/{size}",
            )]

        if a.get("cred"):
            for s in spec.sources:
                s.attrs = {dl.CREDENTIAL_ATTR: a["cred"]}
        try:
            self.ids[alias] = dl.submit(self.store, spec)
        except JobError as e:
            return outcome(e)
        except Exception:
            return "refused"
        return self.show(alias, None)

    def stage(self, alias, n, kind):
        """Put bytes in the partial before a run, so a resume has a prefix to
        continue from and sends the Range request the wire scenarios are about.

        ``stale`` writes bytes from a DIFFERENT artifact. That is the case a
        bare Range cannot see: the server replaced the file, the range it
        answers is honest and belongs to a version the prefix never came from,
        and the splice has the right length and the wrong contents."""
        body = bytes((i + 7) % 251 for i in range(n)) if kind == "stale" else artifact(n)
        path = os.path.join(self.work, "store", dl.partial_for("models/" + alias + ".bin", self.ids[alias]))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(body)
        return self.show(alias, None)

    def plant(self, alias, where, name):
        """Forge what a newer writer would have written: a content-set name this
        implementation has never heard of, in ``content`` alone or in
        ``critical`` too.

        It edits the file rather than going through the store because no
        conforming writer can produce this record -- an implementation validates
        its own declaration on the way out and refuses a name it could not read
        back -- so the refusal path exists and nothing in the language could
        reach it. The edit is textual on purpose: the encoding is fixed at
        two-space indent, so inserting one element at the head of an array is
        the same three lines in three languages."""
        path = os.path.join(self.work, "store", "jobs", self.ids[alias] + ".json")
        with open(path, "r", encoding="utf-8", newline="") as f:
            lines = f.read().split("\n")
        for key in ("content", "critical") if where == "critical" else ("content",):
            out = []
            for line in lines:
                out.append(line)
                if line == f'  "{key}": [':
                    out.append(f'    "{name}",')
            lines = out
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("\n".join(lines))
        return self.show(alias, None)

    def claim(self, alias, owner, ttl_ms):
        try:
            rec = self.store.claim(self.ids[alias], owner, ttl_ms / 1000.0)
            self.epochs[owner] = rec.lease.epoch
            return self.show(alias, None)
        except JobError as e:
            return self.show(alias, e)

    def renew(self, alias, owner, ttl_ms):
        """A verb no driver has is a verb no scenario can reach, and a
        divergence a scenario cannot reach is one no harness will ever report.
        This was the only store operation the language could not say."""
        return self.attempt(
            alias,
            lambda: self.store.renew(self.ids[alias], self.epochs.get(owner, 0), ttl_ms / 1000.0),
        )

    def progress(self, alias, owner, done, checkpoint):
        def mutate(r):
            r.progress.done = done
            r.progress.updated_at = now()
            if checkpoint:
                r.checkpoint = json.loads(checkpoint)

        return self.attempt(alias, lambda: self.store.update(self.ids[alias], self.epochs.get(owner, 0), mutate))

    def release(self, alias, owner):
        return self.attempt(alias, lambda: self.store.release(self.ids[alias], self.epochs.get(owner, 0)))

    def finish(self, alias, owner, state):
        def mutate(r):
            if state not in STATES:
                raise Invalid(f"state {state!r}")
            r.state = state

        return self.attempt(alias, lambda: self.store.update(self.ids[alias], self.epochs.get(owner, 0), mutate))

    def intent(self, alias, want):
        return self.attempt(alias, lambda: self.store.set_intent(self.ids[alias], want, "replay"))

    def recall(self, alias, owner, grace_ms, reason):
        """Issued against the epoch the named owner holds, which is the epoch an
        issuer would have read off the record: naming an owner with no epoch is
        a recall decided against a holding that never existed."""
        return self.attempt(
            alias,
            lambda: self.store.recall(self.ids[alias], self.epochs.get(owner, 0), reason, "replay", grace_ms / 1000.0),
        )

    def attempt(self, alias, call):
        try:
            call()
            return self.show(alias, None)
        except JobError as e:
            return self.show(alias, e)

    def watch(self, name, budget_ms):
        self.subs[name] = watch(self.store, dl.KIND, budget_ms / 1000.0)
        return "ok"

    def next(self, name):
        """What a listener was handed: the kind of notice, then every job the
        scenario named as state/done. Never the silence -- clocks are not
        compared."""
        if name not in self.subs:
            return "not-found"
        try:
            n = self.subs[name].next()
        except Closed:
            return "closed"
        return ("quiet " if n.quiet else "changed ") + self.present(n.records)

    def present(self, records):
        by_id = {i: a for a, i in self.ids.items()}
        parts = sorted(
            f"{by_id[r.id]}={r.state}/{r.progress.done}" for r in records if r.id in by_id
        )
        return " ".join(parts) if parts else "-"

    def close(self, name):
        if name not in self.subs:
            return "not-found"
        self.subs[name].close()
        return "ok"

    def orphans(self):
        by_id = {i: a for a, i in self.ids.items()}
        names = sorted(by_id[r.id] for r in self.store.orphans() if r.id in by_id)
        return "ok " + (" ".join(names) if names else "-")

    def run(self, alias, owner, shared=False):
        runner = dl.Runner(
            self.store, owner, lease_ttl=5.0, reach=self.refused.get, shared_store=shared
        )
        try:
            runner.run(self.ids[alias])
        except Exception:
            return "transfer-failed " + self.fields(self.store.load(self.ids[alias]))
        return self.show(alias, None)


def main():
    # Before anything is written, not just before the transcript. Windows text
    # mode turns every \n into \r\n, and --capabilities has been answering CRLF
    # since it existed: the harness reads it through a command substitution,
    # which MSYS normalises, so nothing saw it. --models is read from a file and
    # is not forgiven.
    try:
        sys.stdout.reconfigure(newline="\n")
    except AttributeError:
        pass
    if len(sys.argv) == 2 and sys.argv[1] == "--capabilities":
        sys.stdout.write(capabilities() + "\n")
        return
    if len(sys.argv) == 2 and sys.argv[1] == "--models":
        sys.stdout.write(models() + "\n")
        return
    if len(sys.argv) != 3:
        sys.exit("usage: replay.py <workdir> <scenario> | replay.py --capabilities | --models")

    p = Replay(sys.argv[1])
    with open(sys.argv[2], "r", encoding="utf-8") as f:
        script = f.read()

    n = 0
    for raw in script.replace("\r\n", "\n").split("\n"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        n += 1
        sys.stdout.write(f"{n:02d} {line} -> {p.do(line.split())}\n")


if __name__ == "__main__":
    main()
