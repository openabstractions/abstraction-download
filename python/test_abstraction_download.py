"""Tests for the Python download implementation.

These mirror the Go tests deliberately. Two independent implementations passing
the same assertions is what makes this an abstraction rather than a file format
with one reader.
"""

import email.message
import hashlib
import http.server
import os
import sys
import tempfile
import threading
import unittest

# The job layer is a separate repository, not a copy kept here. Put its python/
# directory on PYTHONPATH, or check that repository out beside this one; either
# way these tests run against the same record implementation the Go tests
# interoperate with.
_here = os.path.dirname(os.path.abspath(__file__))
for _candidate in (
    os.path.join(_here, "..", "..", "abstraction-job", "python"),
    os.path.join(_here, "..", "..", "job", "python"),
):
    if os.path.isdir(_candidate):
        sys.path.insert(0, _candidate)
        break

from abstraction_job import FileStore, Record, TRANSFERRED, COMPLETE
import abstraction_download as dl


class RangeServer(http.server.BaseHTTPRequestHandler):
    """Serves content and honours Range, like any sane file host."""

    body = b""
    ignore_range = False

    def do_GET(self):
        start = 0
        rng = self.headers.get("Range")
        if rng and not self.ignore_range:
            start = int(rng.split("=")[1].split("-")[0])
            self.send_response(206)
            self.send_header(
                "Content-Range",
                "bytes %d-%d/%d" % (start, len(self.body) - 1, len(self.body)),
            )
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(len(self.body) - start))
        self.end_headers()
        self.wfile.write(self.body[start:])

    def log_message(self, *a):
        pass


def serve(body, ignore_range=False):
    handler = type("H", (RangeServer,), {"body": body, "ignore_range": ignore_range})
    srv = http.server.HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d/blob.bin" % srv.server_port


def payload(n):
    body = bytes((i * 7 + 11) % 251 for i in range(n))
    return body, "sha256:" + hashlib.sha256(body).hexdigest()


class DownloadTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.store = FileStore(self.dir.name)

    def spec(self, url, digest, size, final="out.bin"):
        return dl.Spec(
            artifact=dl.Artifact(digest=digest, size=size),
            sources=[dl.Source(scheme="http", locator=url)],
            sink=dl.Sink(final=final),
        )

    def stage(self, job_id, on_disk, proven, validators=None):
        """Put a partial on disk and a checkpoint in the record.

        validators say which version those bytes came from, which is what a
        current owner records and what the resume then sends back. Staging
        without them tests a record no owner would write.

        Asserting the staging took is not paranoia: an earlier version of the Go
        equivalent staged with a 1 ms lease, the setup update was refused, the
        error was ignored, and two resume tests passed while testing nothing.
        """
        v = validators or dl.Validators()
        rec = self.store.load(job_id)
        partial, _ = dl.local_sink(self.store, dl.spec_of(rec).sink)
        os.makedirs(os.path.dirname(partial) or ".", exist_ok=True)
        with open(partial, "wb") as f:
            f.write(on_disk)
        held = self.store.claim(job_id, "stager", 30)

        def mutate(r):
            r.progress.done = proven
            r.checkpoint = dl.Checkpoint(
                verified_prefix=proven, validators=v
            ).to_dict()

        self.store.update(job_id, held.lease.epoch, mutate)
        self.store.release(job_id, held.lease.epoch)
        staged = dl.checkpoint_of(self.store.load(job_id))
        self.assertEqual(
            (staged.verified_prefix, staged.validators),
            (proven, v),
            "staging did not take, so this test would prove nothing",
        )
        return partial

    def test_downloads_and_verifies(self):
        body, digest = payload(64 * 1024)
        srv, url = serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        dl.Runner(self.store).run(jid)

        rec = self.store.load(jid)
        self.assertEqual(rec.state, TRANSFERRED)
        _, final = dl.local_sink(self.store, dl.spec_of(rec).sink)
        with open(final, "rb") as f:
            self.assertEqual(f.read(), body)

    def test_refuses_wrong_digest_and_deletes_the_partial(self):
        body, _ = payload(32 * 1024)
        _, wrong = payload(999)
        srv, url = serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, wrong, len(body)))
        with self.assertRaises(dl.DigestMismatch):
            dl.Runner(self.store).run(jid)

        rec = self.store.load(jid)
        partial, _ = dl.local_sink(self.store, dl.spec_of(rec).sink)
        self.assertFalse(
            os.path.exists(partial),
            "bytes known to be wrong were left for a successor to resume onto",
        )
        self.assertEqual(dl.checkpoint_of(rec).verified_prefix, 0)

    def test_restarts_when_the_server_ignores_range(self):
        """Ask for bytes from 20,000, get 200 and the whole file. Appending it
        gives a file of plausible length and impossible content -- curl -C - does
        exactly this.

        The answer is not to fail: the response IS a complete, valid artifact. So
        the prefix is thrown away and it is taken from byte zero, which is what
        the delivered content proves happened.
        """
        body, digest = payload(64 * 1024)
        srv, url = serve(body, ignore_range=True)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        self.stage(jid, body[:20000], 20000)

        dl.Runner(self.store).run(jid)
        _, final = dl.local_sink(self.store, dl.spec_of(self.store.load(jid)).sink)
        with open(final, "rb") as f:
            self.assertEqual(
                f.read(), body, "the 200 was appended to the prefix on disk"
            )

    def test_a_partial_shorter_than_its_record_is_refused(self):
        """The record claims more was proven than the file holds. That is not a
        resume point at a lower offset -- it is a file something other than this
        library has been writing to -- so the attempt fails, the partial is
        thrown away, and the transfer starts again from zero."""
        body, digest = payload(64 * 1024)
        srv, url = serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        partial = self.stage(jid, body[:1000], 50000)  # a lie the file cannot back up

        with self.assertRaises(dl.FileTooShort):
            dl.Runner(self.store).run(jid)
        self.assertFalse(
            os.path.exists(partial),
            "the disagreeing partial was kept; the next runner would resume onto it",
        )
        self.assertEqual(
            dl.checkpoint_of(self.store.load(jid)).verified_prefix,
            0,
            "the checkpoint still claims bytes are proven",
        )

        # And the restart it asked for actually works.
        dl.Runner(self.store).run(jid)
        _, final = dl.local_sink(self.store, dl.spec_of(self.store.load(jid)).sink)
        with open(final, "rb") as f:
            self.assertEqual(f.read(), body)

    def test_resumes_from_what_was_proven_not_what_is_on_disk(self):
        """The dead process wrote more than it checkpointed. Nothing vouches for
        the difference, so it must be discarded."""
        body, digest = payload(64 * 1024)
        srv, url = serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        # 30,000 bytes on disk, 20,000 proven, and the unproven tail is wrong.
        self.stage(jid, body[:20000] + b"\x00" * 10000, 20000)

        dl.Runner(self.store).run(jid)
        _, final = dl.local_sink(self.store, dl.spec_of(self.store.load(jid)).sink)
        with open(final, "rb") as f:
            self.assertEqual(f.read(), body, "the unproven tail was kept")

    def test_submit_writes_no_machine_specific_path(self):
        body, digest = payload(1024)
        srv, url = serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        jid = dl.submit(
            self.store, self.spec(url, digest, len(body), final="models/x.gguf")
        )

        with open(os.path.join(self.dir.name, "jobs", jid + ".json"), "rb") as f:
            raw = f.read()
        self.assertNotIn(
            self.dir.name.encode(), raw, "the record names this machine's store root"
        )
        self.assertIn(b'"final": "models/x.gguf"', raw)

    def test_separators_are_part_of_the_contract(self):
        r"""os.path.join on Windows produces models\x.gguf. On Linux that is one
        file whose name contains a backslash, not a directory and a file."""
        self.assertEqual(dl.portable("models\\x.gguf"), "models/x.gguf")
        self.assertEqual(dl.portable("models/x.gguf"), "models/x.gguf")
        # Absolute under EITHER convention is left alone.
        for p in ("D:\\models\\x.gguf", "/mnt/models/x.gguf", "\\\\nas\\share\\x.gguf"):
            self.assertEqual(dl.portable(p), p)

    def test_same_record_resolves_on_either_machine(self):
        sink = dl.Sink(partial="work/abc", final="models/x.gguf")
        p, f = sink.resolve("/store")
        self.assertEqual(p, os.path.join("/store", "work", "abc"))
        self.assertEqual(f, os.path.join("/store", "models", "x.gguf"))

    def test_credential_is_referenced_never_stored(self):
        os.environ.pop("ABSTRACTION_CRED_HF", None)
        os.environ.pop("HF_TOKEN", None)
        with self.assertRaises(dl.DownloadError):
            dl.credentials("hf")
        os.environ["ABSTRACTION_CRED_HF"] = "secret-token"
        try:
            self.assertEqual(
                dl.credentials("hf"), {"Authorization": "Bearer secret-token"}
            )
        finally:
            os.environ.pop("ABSTRACTION_CRED_HF", None)

    def test_take_delivery_completes_the_job(self):
        body, digest = payload(4096)
        srv, url = serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        r = dl.Runner(self.store)
        r.run(jid)
        self.assertEqual(self.store.load(jid).state, TRANSFERRED)
        r.take_delivery(jid)
        self.assertEqual(self.store.load(jid).state, COMPLETE)

    def test_adopt_finishes_stranded_work(self):
        body, digest = payload(8192)
        srv, url = serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        jid = dl.submit(self.store, self.spec(url, digest, len(body)))

        self.assertEqual(dl.Runner(self.store).adopt(), 1)
        self.assertEqual(self.store.load(jid).state, TRANSFERRED)


class RecordingRangeServer(RangeServer):
    """RangeServer that remembers every Range header it was sent, so a test can
    assert on the wire rather than infer from how many bytes arrived."""

    seen = None

    def do_GET(self):
        self.seen.append(self.headers.get("Range") or "")
        RangeServer.do_GET(self)


def recording_serve(body):
    seen = []
    handler = type("H", (RecordingRangeServer,), {"body": body, "seen": seen})
    srv = http.server.HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d/blob.bin" % srv.server_port, seen


class ResumeTest(unittest.TestCase):
    """resume_or_submit: the job for a destination, continued.

    These mirror the Go tests of the same names. Two implementations agreeing on
    which record a destination has -- down to the id -- is what stops a file
    fetched by one and continued by the other from becoming two downloads.
    """

    OWNER = "test-owner@host:1"

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.store = FileStore(self.dir.name)
        self.out = os.path.join(self.dir.name, "out")

    def spec(self, url, name, digest="", size=0):
        return dl.Spec(
            artifact=dl.Artifact(digest=digest, size=size),
            sources=[dl.Source(scheme="http", locator=url)],
            sink=dl.Sink(final=os.path.join(self.out, name)),
        )

    def call(self, spec, owner_name=None):
        return dl.resume_or_submit(
            self.store, spec, owner_name=owner_name or self.OWNER
        )

    def stage(self, job_id, body, on_disk, proven, validators=None):
        """What a killed download leaves: bytes on disk, a record saying how many
        of them were proven, and which version they came from."""
        v = validators or dl.Validators()
        rec = self.store.load(job_id)
        partial, _ = dl.local_sink(self.store, dl.spec_of(rec).sink)
        if on_disk is not None:
            os.makedirs(os.path.dirname(partial) or ".", exist_ok=True)
            with open(partial, "wb") as f:
                f.write(body[:on_disk])
        held = self.store.claim(job_id, "stager", 30)

        def mutate(r):
            r.progress.done = proven
            r.checkpoint = dl.Checkpoint(
                verified_prefix=proven, validators=v
            ).to_dict()

        self.store.update(job_id, held.lease.epoch, mutate)
        self.store.release(job_id, held.lease.epoch)
        self.assertEqual(
            dl.checkpoint_of(self.store.load(job_id)).verified_prefix,
            proven,
            "staging did not take, so this test would prove nothing",
        )
        return partial

    def finish(self, job_id, state):
        held = self.store.claim(job_id, "test-finisher", 30)

        def mutate(r):
            r.state = state

        self.store.update(job_id, held.lease.epoch, mutate)

    # A one-shot command has no job id to remember. Asked a second time for the
    # same destination, it must be handed the record it made the first time.
    def test_a_second_call_finds_the_first_record(self):
        spec = self.spec("http://example.invalid/a.bin", "a.bin")
        first, c1 = self.call(spec)
        self.assertEqual(c1.disposition, dl.SUBMITTED)
        second, c2 = self.call(spec)
        self.assertEqual(c2.disposition, dl.RESUMED)
        self.assertEqual(first, second, "two ids for one destination")
        self.assertEqual(len(self.store.list()), 1)

    # The whole point: the second attempt asks the server for the rest of the
    # file and not for all of it.
    def test_the_second_attempt_sends_a_range_request(self):
        body, digest = payload(64 * 1024)
        have = 12 * 1024
        srv, url, ranges = recording_serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        spec = self.spec(url, "b.bin", digest, len(body))
        first, _ = self.call(spec)
        self.stage(first, body, have, have)

        second, c = self.call(spec)
        self.assertEqual(second, first, "the second call started a new job")
        self.assertEqual(c.disposition, dl.RESUMED)
        self.assertEqual(c.resume_from, have)

        dl.Runner(self.store).run(second)
        self.assertTrue(ranges, "the server was never asked for anything")
        self.assertEqual(
            ranges[-1],
            "bytes=12288-",
            "the second attempt did not resume",
        )
        _, final = dl.local_sink(self.store, dl.spec_of(self.store.load(second)).sink)
        with open(final, "rb") as f:
            self.assertEqual(f.read(), body)

    # Two shells, one destination, at the same instant. The store's refusal to
    # create one id twice is what makes this one record rather than two racing
    # transfers to the same path.
    def test_concurrent_callers_produce_one_record(self):
        spec = self.spec("http://example.invalid/c.bin", "c.bin")
        callers = 8
        ids = [None] * callers
        errs = [None] * callers
        start = threading.Barrier(callers)

        def caller(i):
            try:
                start.wait()
                ids[i], _ = self.call(spec)
            except Exception as e:  # recorded, not raised out of a thread
                errs[i] = e

        threads = [threading.Thread(target=caller, args=(i,)) for i in range(callers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual([e for e in errs if e is not None], [])
        self.assertEqual(set(ids), {ids[0]}, "one destination produced several ids")
        self.assertEqual(len(self.store.list()), 1)

    # A checkpoint is a claim about a file. When the file disagrees, the file
    # wins: resuming from a byte that is not there produces the right length and
    # the wrong contents, which no later check would catch without a digest.
    def test_a_vanished_partial_is_not_trusted(self):
        body, digest = payload(32 * 1024)
        srv, url, ranges = recording_serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        spec = self.spec(url, "d.bin", digest, len(body))
        first, _ = self.call(spec)
        partial = self.stage(first, body, None, 20 * 1024)
        self.assertFalse(
            os.path.exists(partial), "this test needs the partial to be absent"
        )

        _, c = self.call(spec)
        self.assertEqual(
            c.resume_from, 0, "the checkpoint was believed over the filesystem"
        )
        dl.Runner(self.store).run(first)
        self.assertEqual(
            [r for r in ranges if r],
            [],
            "a Range header was sent for a file with no bytes on disk",
        )
        _, final = dl.local_sink(self.store, dl.spec_of(self.store.load(first)).sink)
        with open(final, "rb") as f:
            self.assertEqual(f.read(), body)

    # The same, one step less obvious: the file is there but shorter than the
    # checkpoint says. That is not a resume point at a lower offset -- something
    # other than this library shortened the file, so no part of the prefix that
    # is still there can be believed. It is reported as no resume point at all,
    # and as the bytes it costs.
    def test_a_short_partial_has_no_resume_point(self):
        body, _ = payload(32 * 1024)
        spec = self.spec("http://example.invalid/e.bin", "e.bin")
        first, _ = self.call(spec)
        self.stage(first, body, 1024, 20 * 1024)

        _, c = self.call(spec)
        self.assertEqual(
            c.resume_from, 0, "a file shorter than its checkpoint was resumed onto"
        )
        self.assertEqual(c.discarded, 1024)

    # The ordinary case, and the other half of the three-way test: the dead owner
    # wrote past its last checkpoint. The unproven tail is counted, not kept.
    def test_an_unproven_tail_is_reported_as_discarded(self):
        body, _ = payload(32 * 1024)
        spec = self.spec("http://example.invalid/e2.bin", "e2.bin")
        first, _ = self.call(spec)
        self.stage(first, body, 5000, 4000)

        _, c = self.call(spec)
        self.assertEqual(c.resume_from, 4000)
        self.assertEqual(c.discarded, 1000)

    # A URL is not the identity; the destination is. Continuing anyway is a
    # choice, so the caller is told which source the job it was handed fetches.
    def test_a_different_source_is_continued_and_reported(self):
        first, _ = self.call(self.spec("http://origin.invalid/f.bin", "f.bin"))
        second, c = self.call(self.spec("http://mirror.invalid/f.bin", "f.bin"))
        self.assertEqual(first, second, "a mirror became a second job")
        self.assertTrue(
            c.source_changed, "the caller was not told the job fetches elsewhere"
        )
        self.assertEqual(c.source, "http://origin.invalid/f.bin")

    # Complete means the file is there. Checked, not believed.
    def test_a_completed_download_is_not_fetched_again(self):
        spec = self.spec("http://example.invalid/g.bin", "g.bin")
        first, _ = self.call(spec)
        _, final = dl.local_sink(self.store, dl.spec_of(self.store.load(first)).sink)
        os.makedirs(os.path.dirname(final), exist_ok=True)
        with open(final, "wb") as f:
            f.write(b"done")
        self.finish(first, COMPLETE)

        second, c = self.call(spec)
        self.assertEqual(c.disposition, dl.DELIVERED)
        self.assertEqual(second, first, "a finished download was fetched again")

        # And when the file is gone, the completed record is history rather than
        # a claim on the path.
        os.remove(final)
        third, c = self.call(spec)
        self.assertNotEqual(
            third, first, "a completed record was reused although its file had gone"
        )
        self.assertEqual(c.disposition, dl.SUBMITTED)

    # Somebody else is downloading to this path. Their lease is theirs.
    def test_another_owners_lease_is_not_taken(self):
        spec = self.spec("http://example.invalid/h.bin", "h.bin")
        first, _ = self.call(spec)
        held = self.store.claim(first, "somebody-else@host:99", 60)

        second, c = self.call(spec)
        self.assertEqual(second, first, "a job somebody else is doing was duplicated")
        self.assertEqual(c.disposition, dl.BUSY)
        after = self.store.load(first)
        self.assertEqual(after.lease.owner, "somebody-else@host:99")
        self.assertEqual(after.lease.epoch, held.lease.epoch, "the lease was taken")

    # Two spellings of one path are one destination.
    def test_two_spellings_of_one_destination_are_one_record(self):
        url = "http://example.invalid/i.bin"
        src = [dl.Source(scheme="http", locator=url)]
        plain = os.path.join(self.out, "i.bin")
        roundabout = self.out + "/./sub/../i.bin"

        first, _ = self.call(dl.Spec(sources=src, sink=dl.Sink(final=plain)))
        second, _ = self.call(dl.Spec(sources=src, sink=dl.Sink(final=roundabout)))
        self.assertEqual(first, second, "two spellings became two jobs")
        self.assertEqual(len(self.store.list()), 1)

    # resume_or_get is the shape a command-line tool has: a URL and a path.
    def test_resume_or_get_is_keyed_on_the_same_path(self):
        dest = os.path.join(self.out, "j.bin")
        first, _ = dl.resume_or_get(self.store, "http://example.invalid/j.bin", dest)
        second, c = dl.resume_or_get(self.store, "http://example.invalid/j.bin", dest)
        self.assertEqual(first, second, "running the command twice made two jobs")
        self.assertEqual(c.disposition, dl.RESUMED)
        self.assertEqual(len(self.store.list()), 1)

    # The id is the cross-language part of this contract. If Go and Python
    # derived it differently, a download begun by one and re-run through the
    # other would be two records for one file -- the very failure being fixed.
    # The Go test of the same name pins the same string.
    def test_the_id_for_a_destination_is_pinned(self):
        self.assertEqual(dl._destination_id("/models/x.gguf"), "dest-01ec6db371a234af")


class VersionedServer(http.server.BaseHTTPRequestHandler):
    """Serves whatever body it currently holds under a validator, and behaves the
    way RFC 7233 says a server should: a Range with an If-Range that does not
    match the current entity is answered with the whole current entity and a 200,
    not with a range of it.

    Every request's headers are kept, so a test can assert on the wire rather
    than infer from how many bytes arrived.
    """

    body = b""
    etag = ""
    last_modified = ""
    seen = None

    def _send(self, status, chunk, extra=()):
        self.send_response(status)
        if self.etag:
            self.send_header("ETag", self.etag)
        if self.last_modified:
            self.send_header("Last-Modified", self.last_modified)
        self.send_header("Accept-Ranges", "bytes")
        for k, v in extra:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(chunk)))
        self.end_headers()
        self.wfile.write(chunk)

    def do_GET(self):
        self.seen.append(self.headers)
        rng = self.headers.get("Range")
        if_range = self.headers.get("If-Range")
        current = [v for v in (self.etag, self.last_modified) if v]
        if rng and (if_range is None or if_range in current):
            start = int(rng.split("=")[1].split("-")[0])
            if start >= len(self.body):
                self._send(
                    416, b"", [("Content-Range", "bytes */%d" % len(self.body))]
                )
                return
            self._send(
                206,
                self.body[start:],
                [
                    (
                        "Content-Range",
                        "bytes %d-%d/%d" % (start, len(self.body) - 1, len(self.body)),
                    )
                ],
            )
            return
        # The client is holding bytes from a version this is no longer serving,
        # or asked for no range at all. Give it the current one, whole.
        self._send(200, self.body)

    def log_message(self, *a):
        pass


def versioned_serve(body, etag="", last_modified=""):
    seen = []
    handler = type(
        "H",
        (VersionedServer,),
        {"body": body, "etag": etag, "last_modified": last_modified, "seen": seen},
    )
    srv = http.server.HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d/blob.bin" % srv.server_port, seen


def scripted_serve(handle):
    """A server whose whole behaviour is one function of (headers) -> (status,
    body, extra headers). For the answers a well-behaved server never gives."""
    seen = []

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append(self.headers)
            status, chunk, extra = handle(self.headers)
            self.send_response(status)
            for k, v in extra:
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(chunk)))
            self.end_headers()
            self.wfile.write(chunk)

        def log_message(self, *a):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, "http://127.0.0.1:%d/blob.bin" % srv.server_port, seen


class ValidatorTest(unittest.TestCase):
    """Whether the bytes a server is offering now are the same artifact as the
    bytes already on disk.

    These mirror the Go tests of the same names. A resume that cannot say which
    version it is continuing invites a valid range of a different file, and the
    result is a file of exactly the right length holding two versions spliced
    together.
    """

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.store = FileStore(self.dir.name)

    def spec(self, url, digest="", size=0):
        return dl.Spec(
            artifact=dl.Artifact(digest=digest, size=size),
            sources=[dl.Source(scheme="http", locator=url)],
            sink=dl.Sink(final="out.bin"),
        )

    def stage(self, job_id, prefix, validators):
        rec = self.store.load(job_id)
        partial, _ = dl.local_sink(self.store, dl.spec_of(rec).sink)
        os.makedirs(os.path.dirname(partial) or ".", exist_ok=True)
        with open(partial, "wb") as f:
            f.write(prefix)
        held = self.store.claim(job_id, "dead-owner", 30)

        def mutate(r):
            r.progress.done = len(prefix)
            r.checkpoint = dl.Checkpoint(
                verified_prefix=len(prefix), validators=validators
            ).to_dict()

        self.store.update(job_id, held.lease.epoch, mutate)
        self.store.release(job_id, held.lease.epoch)
        staged = dl.checkpoint_of(self.store.load(job_id))
        self.assertEqual(
            (staged.verified_prefix, staged.validators),
            (len(prefix), validators),
            "staging did not take, so this test would prove nothing",
        )
        return partial

    def final(self, job_id):
        _, f = dl.local_sink(self.store, dl.spec_of(self.store.load(job_id)).sink)
        with open(f, "rb") as fh:
            return fh.read()

    # The bug this class exists for.
    #
    # The artifact changed between attempts and the new version is the same
    # length as the old, so nothing about the resulting file's SIZE can reveal
    # what happened. The download carries no digest either, which is the ordinary
    # case for a bare URL. The only thing that can catch it is the exchange
    # itself: the resume says which version it is continuing, the server says
    # that is not the version it has, and the client starts again.
    #
    # Asserted on content. A length assertion passes on the corrupt file.
    def test_a_changed_file_is_not_spliced_onto_the_old_one(self):
        v1 = b"A" * (40 * 1024)
        v2 = b"B" * (40 * 1024)
        srv, url, _ = versioned_serve(v2, etag='"v2"')  # already changed
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url))
        self.stage(jid, v1[:1000], dl.Validators(etag='"v1"'))

        dl.Runner(self.store).run(jid)

        got = self.final(jid)
        self.assertEqual(len(got), len(v2), "the delivered file is not even the size")
        self.assertEqual(
            got,
            v2,
            "the delivered file is not the current artifact: the old prefix was "
            "spliced onto the new file",
        )

        # And the record now describes what was actually delivered, so the NEXT
        # resume continues v2 rather than v1.
        cp = dl.checkpoint_of(self.store.load(jid))
        self.assertEqual(cp.verified_prefix, len(v2))
        self.assertEqual(
            cp.validators.etag,
            '"v2"',
            "the checkpoint does not name the version that was delivered",
        )

    # The header that makes the case above possible is actually on the wire,
    # carrying the stored ETag, and it does not stop the resume being a resume
    # when the file has NOT changed.
    def test_resume_sends_if_range(self):
        body, digest = payload(40 * 1024)
        srv, url, seen = versioned_serve(body, etag='"v1"')
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        self.stage(jid, body[: 12 * 1024], dl.Validators(etag='"v1"'))

        dl.Runner(self.store).run(jid)

        self.assertEqual(
            len(seen), 1, "an unchanged file needs no restart, so one request"
        )
        req = seen[0]
        self.assertEqual(req.get("Range"), "bytes=12288-")
        self.assertEqual(
            req.get("If-Range"),
            '"v1"',
            "a resume that does not say which version it is continuing invites a "
            "range of a different file",
        )
        # Byte offsets are counted after decoding here and before it at the
        # server, so a ranged request must not be answered with a compressed body.
        self.assertEqual(req.get("Accept-Encoding"), "identity")
        self.assertEqual(self.final(jid), body)

    # W/"..." means the server is asserting that two responses are equivalent,
    # not that they are byte-identical -- which is exactly the distinction a
    # byte-range resume depends on. Recording one would buy an If-Range that a
    # changed file could still satisfy: false confidence, worse than none.
    def test_a_weak_etag_is_neither_stored_nor_used(self):
        body, digest = payload(8 * 1024)
        last_modified = "Wed, 21 Oct 2015 07:28:00 GMT"
        srv, url, seen = versioned_serve(
            body, etag='W/"weak"', last_modified=last_modified
        )
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        first = dl.submit(self.store, self.spec(url, digest, len(body)))
        dl.Runner(self.store).run(first)

        cp = dl.checkpoint_of(self.store.load(first))
        self.assertEqual(cp.validators.etag, "", "a weak ETag was recorded")
        self.assertEqual(
            cp.validators.last_modified,
            last_modified,
            "Last-Modified was not recorded in place of the unusable ETag",
        )

        # And on the wire: whatever a resume sends, it is never the weak tag.
        second = dl.submit(
            self.store,
            dl.Spec(
                artifact=dl.Artifact(digest=digest, size=len(body)),
                sources=[dl.Source(scheme="http", locator=url)],
                sink=dl.Sink(final="out2.bin"),
            ),
        )
        self.stage(second, body[: 2 * 1024], cp.validators)
        dl.Runner(self.store).run(second)

        for req in seen:
            self.assertNotIn(
                "w/",
                (req.get("If-Range") or "").lower(),
                "If-Range carried a weak validator",
            )
        self.assertEqual(
            seen[-1].get("If-Range"),
            last_modified,
            "the resume did not send the Last-Modified it had stored",
        )

    # The same rule at the unit it lives in, including the forms a real server
    # writes.
    def test_strong_validators(self):
        lm = "Wed, 21 Oct 2015 07:28:00 GMT"
        cases = [
            ("strong etag wins", {"ETag": '"abc"', "Last-Modified": lm},
             dl.Validators(etag='"abc"')),
            ("weak etag is dropped", {"ETag": 'W/"abc"', "Last-Modified": lm},
             dl.Validators(last_modified=lm)),
            ("weak in lower case too", {"ETag": 'w/"abc"'}, dl.Validators()),
            ("unquoted is not an etag", {"ETag": "abc"}, dl.Validators()),
            ("a date that is not a date", {"Last-Modified": "yesterday"},
             dl.Validators()),
            ("nothing at all", {}, dl.Validators()),
        ]
        for name, headers, want in cases:
            with self.subTest(name):
                msg = email.message.Message()
                for k, v in headers.items():
                    msg[k] = v
                self.assertEqual(dl.strong_validators(msg), want)

        self.assertEqual(
            dl.Validators(etag='"a"', last_modified=lm).if_range(),
            '"a"',
            "if_range preferred the Last-Modified over the ETag",
        )
        self.assertEqual(dl.Validators(last_modified=lm).if_range(), lm)
        self.assertEqual(dl.Validators().if_range(), "")
        self.assertTrue(dl.Validators().empty())

    # The status says partial content and the Content-Range says it begins
    # somewhere other than where we asked. Those bytes are the artifact's, but
    # they belong at an offset nobody asked about, so writing them where the
    # request expected them puts real content in the wrong place -- a corruption
    # no length check and no transport error can see.
    def test_a_206_at_the_wrong_offset_restarts(self):
        body, digest = payload(32 * 1024)

        def handle(headers):
            if not headers.get("Range"):
                return 200, body, []
            # Asked for bytes from N; answers with the whole file and calls it a
            # range starting at zero. Servers behind rewriting proxies do this.
            return (
                206,
                body,
                [("Content-Range", "bytes 0-%d/%d" % (len(body) - 1, len(body)))],
            )

        srv, url, seen = scripted_serve(handle)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        self.stage(jid, body[: 4 * 1024], dl.Validators(etag='"v1"'))

        dl.Runner(self.store).run(jid)
        self.assertEqual(
            self.final(jid), body, "a misplaced range was trusted and appended"
        )
        self.assertEqual(
            [bool(r.get("Range")) for r in seen],
            [True, False],
            "want a ranged request and then a whole-file restart",
        )

    # The stored offset is past the end of what the server now holds. Left alone
    # the checkpoint produces the same 416 on every retry, so the job could never
    # finish without somebody deleting the partial by hand.
    def test_a_416_restarts(self):
        body, digest = payload(8 * 1024)
        srv, url, seen = versioned_serve(body, etag='"v1"')
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        # A prefix longer than the artifact the server now serves.
        self.stage(jid, b"\x00" * (16 * 1024), dl.Validators(etag='"v1"'))

        dl.Runner(self.store).run(jid)
        self.assertEqual(self.final(jid), body)
        self.assertEqual(
            [bool(r.get("Range")) for r in seen],
            [True, False],
            "want a ranged request answered 416 and then a whole-file restart",
        )

    # The forms the header arrives in, including the ones that must not be read
    # as a starting byte.
    def test_content_range_start(self):
        ok = {
            "bytes 0-99/100": 0,
            "bytes 1000-40959/*": 1000,
            "  bytes 12-99/100  ": 12,
            "BYTES 7-99/100": 7,
            "bytes 500-999/123456": 500,
        }
        for text, want in ok.items():
            self.assertEqual(dl._content_range_start(text), want, text)
        for text in (
            "",
            None,
            "items 0-99/100",
            "bytes */100",
            "bytes 0-99",
            "bytes x-99/100",
            "bytes -99/100",
        ):
            with self.assertRaises(dl.DownloadError, msg=repr(text)):
                dl._content_range_start(text)

    # The three-way test on its own: ahead of the checkpoint, exactly at it,
    # behind it, and gone.
    def test_resume_at(self):
        def write(name, n):
            p = os.path.join(self.dir.name, name)
            with open(p, "wb") as f:
                f.write(b"\x00" * n)
            return p

        v = dl.Validators(etag='"v1"')

        rp = dl._resume_at(write("ahead.bin", 1500), dl.Checkpoint(1000, v))
        self.assertEqual((rp.from_, rp.discarded, rp.validators), (1000, 500, v))

        rp = dl._resume_at(write("equal.bin", 1000), dl.Checkpoint(1000))
        self.assertEqual((rp.from_, rp.discarded), (1000, 0))

        with self.assertRaises(dl.FileTooShort):
            dl._resume_at(write("short.bin", 900), dl.Checkpoint(1000))

        # Missing is not short: there is no prefix to disbelieve, so this starts
        # over rather than failing.
        gone = os.path.join(self.dir.name, "gone.bin")
        self.assertEqual(dl._resume_at(gone, dl.Checkpoint(1000)).from_, 0)
        # No checkpoint, no questions: a first attempt over whatever is there.
        self.assertEqual(dl._resume_at(write("any.bin", 77), dl.Checkpoint()).from_, 0)

    # The record is the contract. A checkpoint written here must be the one Go
    # reads, field names included, or a job started by one and continued by the
    # other resumes with no validator at all.
    def test_the_checkpoint_names_the_fields_go_reads(self):
        cp = dl.Checkpoint(
            verified_prefix=1000,
            validators=dl.Validators(etag='"v1"', last_modified="x"),
        )
        self.assertEqual(
            cp.to_dict(),
            {
                "verified_prefix": 1000,
                "validators": {"etag": '"v1"', "last_modified": "x"},
            },
        )
        # Absent rather than empty, matching Go's omitempty, so a record written
        # here and one written there are the same bytes.
        self.assertEqual(dl.Checkpoint(5).to_dict(), {"verified_prefix": 5})

        rec = Record(id="x", kind=dl.KIND)
        rec.checkpoint = cp.to_dict()
        self.assertEqual(dl.checkpoint_of(rec), cp)


if __name__ == "__main__":
    unittest.main()
