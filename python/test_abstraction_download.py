"""Tests for the Python download implementation.

These mirror the Go tests deliberately. Two independent implementations passing
the same assertions is what makes this an abstraction rather than a file format
with one reader.
"""

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

from abstraction_job import FileStore, TRANSFERRED, COMPLETE
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

    def stage(self, job_id, on_disk, proven):
        """Put a partial on disk and a checkpoint in the record.

        Asserting the staging took is not paranoia: an earlier version of the Go
        equivalent staged with a 1 ms lease, the setup update was refused, the
        error was ignored, and two resume tests passed while testing nothing.
        """
        rec = self.store.load(job_id)
        partial, _ = dl.local_sink(self.store, dl.spec_of(rec).sink)
        os.makedirs(os.path.dirname(partial) or ".", exist_ok=True)
        with open(partial, "wb") as f:
            f.write(on_disk)
        held = self.store.claim(job_id, "stager", 30)

        def mutate(r):
            r.checkpoint = {"verified_prefix": proven}

        self.store.update(job_id, held.lease.epoch, mutate)
        self.store.release(job_id, held.lease.epoch)
        self.assertEqual(
            dl.checkpoint_of(self.store.load(job_id)).verified_prefix,
            proven,
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

    def test_refuses_a_server_that_ignores_range(self):
        """Ask for bytes from 20,000, get 200 and the whole file, append it, and
        you have a file of plausible length and impossible content. curl -C -
        does exactly this."""
        body, digest = payload(64 * 1024)
        srv, url = serve(body, ignore_range=True)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        self.stage(jid, body[:20000], 20000)

        with self.assertRaises(dl.RangeIgnored):
            dl.Runner(self.store).run(jid)

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

    def stage(self, job_id, body, on_disk, proven):
        """What a killed download leaves: bytes on disk, and a record saying how
        many of them were proven."""
        rec = self.store.load(job_id)
        partial, _ = dl.local_sink(self.store, dl.spec_of(rec).sink)
        if on_disk is not None:
            os.makedirs(os.path.dirname(partial) or ".", exist_ok=True)
            with open(partial, "wb") as f:
                f.write(body[:on_disk])
        held = self.store.claim(job_id, "stager", 30)

        def mutate(r):
            r.checkpoint = {"verified_prefix": proven}

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
    # checkpoint says.
    def test_a_short_partial_resumes_from_the_file(self):
        body, _ = payload(32 * 1024)
        spec = self.spec("http://example.invalid/e.bin", "e.bin")
        first, _ = self.call(spec)
        self.stage(first, body, 1024, 20 * 1024)

        _, c = self.call(spec)
        self.assertEqual(c.resume_from, 1024)

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


if __name__ == "__main__":
    unittest.main()
