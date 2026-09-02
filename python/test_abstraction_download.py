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

# The job layer is a sibling, not a copy. Copying it would let the two drift and
# would defeat the point: this must run against the SAME record implementation
# the Go tests interoperate with.
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "job", "python")
)

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
        partial, _ = dl.spec_of(rec).sink.resolve(self.store.root)
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
        _, final = dl.spec_of(rec).sink.resolve(self.store.root)
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
        partial, _ = dl.spec_of(rec).sink.resolve(self.store.root)
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
        _, final = dl.spec_of(self.store.load(jid)).sink.resolve(self.store.root)
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


if __name__ == "__main__":
    unittest.main()
