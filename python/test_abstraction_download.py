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
import time
import unittest

# The job layer is a sibling, not a copy. Copying it would let the two drift and
# would defeat the point: this must run against the SAME record implementation
# the Go tests interoperate with.
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "job", "python")
)
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "watch", "python")
)

from abstraction_job import FileStore, TRANSFERRED, COMPLETE, FAILED
import abstraction_download as dl


class RangeServer(http.server.BaseHTTPRequestHandler):
    """Serves content and honours Range and If-Range, like any sane file host.

    If-Range is what makes a changed artifact visible to a resuming client: a
    range request carrying a token that no longer matches is answered with the
    whole current file instead of a range of it.
    """

    body = b""
    ignore_range = False
    etag = ""

    def do_GET(self):
        start = 0
        rng = self.headers.get("Range")
        asked_for = self.headers.get("If-Range")
        stale = bool(asked_for) and asked_for != self.etag
        if rng and not self.ignore_range and not stale:
            start = int(rng.split("=")[1].split("-")[0])
            self.send_response(206)
            self.send_header(
                "Content-Range",
                "bytes %d-%d/%d" % (start, len(self.body) - 1, len(self.body)),
            )
        else:
            self.send_response(200)
        if self.etag:
            self.send_header("ETag", self.etag)
        self.send_header("Content-Length", str(len(self.body) - start))
        self.end_headers()
        self.wfile.write(self.body[start:])

    def log_message(self, *a):
        pass


def serve(body, ignore_range=False, etag=""):
    handler = type(
        "H", (RangeServer,), {"body": body, "ignore_range": ignore_range, "etag": etag}
    )
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

        Asserting the staging took is not paranoia: an earlier version of the Go
        equivalent staged with a 1 ms lease, the setup update was refused, the
        error was ignored, and two resume tests passed while testing nothing.
        """
        rec = self.store.load(job_id)
        partial, _ = dl.local_sink(self.store, rec.id, dl.spec_of(rec).sink)
        os.makedirs(os.path.dirname(partial) or ".", exist_ok=True)
        with open(partial, "wb") as f:
            f.write(on_disk)
        held = self.store.claim(job_id, "stager", 30)

        def mutate(r):
            r.checkpoint = {"verified_prefix": proven, "validators": validators or {}}

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
        _, final = dl.local_sink(self.store, rec.id, dl.spec_of(rec).sink)
        with open(final, "rb") as f:
            self.assertEqual(f.read(), body)

    def test_a_long_resume_hash_holds_its_lease(self):
        """Rehashing a proven prefix is local work with nothing to report, and it
        is proportional to the file: 40 GB at disk speed outlasts a thirty-second
        lease many times over. Nothing renewed across it, so the FIRST checkpoint
        after resuming a large partial was refused for a lease that had lapsed
        while this owner sat reading its own file.

        The Go binding has the same shape of bug in two places -- this one, and a
        delegated finalise, which this binding does not have at all.
        """
        body, digest = payload(2 << 20)
        jid = dl.submit(self.store, self.spec("http://127.0.0.1:1/none", digest, len(body)))
        partial = self.stage(jid, body, len(body))

        r = dl.Runner(self.store, lease_ttl=0.3)
        held = self.store.claim(jid, r.owner, r.lease_ttl)
        epoch = held.lease.epoch

        h = hashlib.sha256()
        beat = r._renewing(jid, epoch)
        chunks = 0

        def read_a_megabyte():
            """A disk that takes 200 ms a megabyte, so this read outlives more
            than one lease without any of it being a stall."""
            nonlocal chunks
            chunks += 1
            time.sleep(0.2)
            beat()

        began = time.monotonic()
        dl._hash_prefix(partial, len(body), h, read_a_megabyte)
        took = time.monotonic() - began

        self.assertEqual(h.hexdigest(), hashlib.sha256(body).hexdigest())
        self.assertGreater(took, r.lease_ttl, "the read did not outlive one lease, so this proves nothing")

        # The owner was displaced by nobody, so its next write must land.
        self.store.update(jid, epoch, lambda rec: None)

        # And the test has teeth: the same wall clock without the beats loses it.
        self.store.release(jid, epoch)
        other = self.store.claim(jid, "somebody-else", 0.3)
        time.sleep(took)
        with self.assertRaises(Exception):
            self.store.update(jid, other.lease.epoch, lambda rec: None)

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
        partial, _ = dl.local_sink(self.store, rec.id, dl.spec_of(rec).sink)
        self.assertFalse(
            os.path.exists(partial),
            "bytes known to be wrong were left for a successor to resume onto",
        )
        self.assertEqual(dl.checkpoint_of(rec).verified_prefix, 0)

    def test_restarts_at_zero_when_a_server_ignores_range(self):
        """Ask for bytes from 20,000 and get 200 with the whole file. Appending
        that gives a file of plausible length and impossible content -- curl -C -
        does exactly this -- so the prefix goes and the body is taken from zero.

        Refusing instead is the answer this implementation used to give, and it
        threw away a download that was going to succeed, on every retry, forever.
        Go and C++ both rewind; this is the scenario that says which of the two
        is the contract."""
        body, digest = payload(64 * 1024)
        srv, url = serve(body, ignore_range=True)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(body)))
        self.stage(jid, body[:20000], 20000)

        dl.Runner(self.store).run(jid)

        rec = self.store.load(jid)
        self.assertEqual(rec.state, TRANSFERRED)
        self.assertEqual(dl.checkpoint_of(rec).verified_prefix, len(body))
        _, final = dl.local_sink(self.store, jid, dl.spec_of(rec).sink)
        with open(final, "rb") as f:
            self.assertEqual(f.read(), body, "the discarded prefix was spliced back in")

    def test_a_replaced_artifact_is_not_spliced_onto_the_old_prefix(self):
        """The source replaced the file between two attempts.

        Nothing in a bare Range request says WHICH file it means, so a server
        that honours ranges answers honestly and the answer is a valid range of
        something else. Appended, that is a file of exactly the right length
        holding two versions spliced at an arbitrary offset -- no transport
        error, no short read, and nothing a download without a digest could
        catch. If-Range is what turns it into a 200 and a restart.
        """
        old, _ = payload(64 * 1024)
        new = bytes((i * 13 + 3) % 251 for i in range(48 * 1024))
        digest = "sha256:" + hashlib.sha256(new).hexdigest()
        srv, url = serve(new, etag='"v2"')
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = dl.submit(self.store, self.spec(url, digest, len(new)))
        self.stage(jid, old[:20000], 20000, validators={"etag": '"v1"'})

        dl.Runner(self.store).run(jid)

        rec = self.store.load(jid)
        _, final = dl.local_sink(self.store, jid, dl.spec_of(rec).sink)
        with open(final, "rb") as f:
            self.assertEqual(f.read(), new, "two versions were spliced together")
        self.assertEqual(
            dl.checkpoint_of(rec).validators.etag,
            '"v2"',
            "the checkpoint records how far it got but not which version",
        )

    def test_a_weak_etag_is_never_recorded(self):
        """Weak asserts semantic equivalence, not byte equality: two responses
        may share a weak tag and differ in their bytes, which is exactly the
        distinction a resume depends on. Recording one makes a server answer 206
        for a file whose bytes moved -- worse than having none, because none at
        least leaves the 200 path and its restart available."""
        headers = {"ETag": 'W/"v1"', "Last-Modified": "Sun, 06 Sep 2026 00:00:00 GMT"}
        self.assertEqual(dl.strong_validators(headers).etag, "")
        self.assertEqual(
            dl.strong_validators(headers).last_modified,
            "Sun, 06 Sep 2026 00:00:00 GMT",
        )
        self.assertEqual(dl.strong_validators({"ETag": '"v1"'}).etag, '"v1"')
        # A malformed date is not echoed back to a server that would have to
        # guess what it meant.
        self.assertTrue(
            dl.strong_validators({"Last-Modified": "whenever"}).if_range() == ""
        )

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
        _, final = dl.local_sink(self.store, jid, dl.spec_of(self.store.load(jid)).sink)
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
        r"""One separator per path, "/", whatever wrote it.

        os.path.join on Windows produces models\x.gguf, and on Linux that is one
        file whose name contains a backslash, not a directory and a file.

        Absolute paths were exempt until 2026-09-06, when the monitor showed two
        finished jobs spelled differently -- one "C:/Users/..." because it came
        back through the NAS, one "C:\Users\..." because it was fetched here --
        and the second changed convention halfway along.
        """
        cases = {
            "models\\x.gguf": "models/x.gguf",
            "models/x.gguf": "models/x.gguf",
            "C:\\Users\\r\\snapshots\\41ba88db/x.gguf": "C:/Users/r/snapshots/41ba88db/x.gguf",
            "D:\\models\\x.gguf": "D:/models/x.gguf",
            "\\\\nas\\share\\x.gguf": "//nas/share/x.gguf",
            # A backslash is a legal character in a POSIX file name, so
            # rewriting one would name a different file.
            "/mnt/models/a\\b.gguf": "/mnt/models/a\\b.gguf",
            "": "",
        }
        for given, want in cases.items():
            self.assertEqual(dl.portable(given), want)
            self.assertEqual(dl.portable(want), want, "portable is not stable")
        # And a respelled UNC root is still an absolute Windows path, or the one
        # host that can write it would start refusing its own records.
        unc = dl.portable("\\\\nas\\share\\x.gguf")
        self.assertTrue(dl._windows_shaped(unc))
        self.assertFalse(dl._relative_everywhere(unc))

    def test_same_record_resolves_on_either_machine(self):
        sink = dl.Sink(partial="work/abc", final="models/x.gguf")
        p, f = sink.resolve("/store", "abc")
        self.assertEqual(p, os.path.join("/store", "work", "abc"))
        self.assertEqual(f, os.path.join("/store", "models", "x.gguf"))

    def test_credential_is_referenced_never_stored(self):
        os.environ.pop("ABSTRACTION_CRED_HF", None)
        os.environ.pop("HF_TOKEN", None)
        os.environ.pop("ABSTRACTION_CRED_HF_HOSTS", None)
        with self.assertRaises(dl.DownloadError):
            dl.credentials("hf", "huggingface.co")
        os.environ["ABSTRACTION_CRED_HF"] = "secret-token"
        os.environ["ABSTRACTION_CRED_HF_HOSTS"] = "huggingface.co"
        try:
            self.assertEqual(
                dl.credentials("hf", "huggingface.co"),
                {"Authorization": "Bearer secret-token"},
            )
        finally:
            os.environ.pop("ABSTRACTION_CRED_HF", None)
            os.environ.pop("ABSTRACTION_CRED_HF_HOSTS", None)

    def test_credential_is_bound_to_its_hosts(self):
        """The confused deputy at the credential field: a record naming
        credential 'hf' with a locator the owner never chose must not receive
        the owner's token. The token is bound to its hosts on the machine."""
        os.environ["ABSTRACTION_CRED_HF"] = "hf_thisMustNeverAppearOnDisk_EXAMPLE"
        os.environ["ABSTRACTION_CRED_HF_HOSTS"] = "huggingface.co"
        self.addCleanup(lambda: os.environ.pop("ABSTRACTION_CRED_HF", None))
        self.addCleanup(lambda: os.environ.pop("ABSTRACTION_CRED_HF_HOSTS", None))

        attacker = dl.Source(
            scheme="https",
            locator="https://attacker.example/models/x.gguf",
            attrs={dl.CREDENTIAL_ATTR: "hf"},
        )
        with self.assertRaises(dl.DownloadError):
            dl.headers_for(attacker)
        self.assertFalse(dl.permanent(self._raised(dl.headers_for, attacker)))

        for loc in (
            "https://huggingface.co/org/repo/resolve/main/x.gguf",
            "https://cdn-lfs.huggingface.co/org/repo/x.gguf",
        ):
            src = dl.Source(scheme="https", locator=loc, attrs={dl.CREDENTIAL_ATTR: "hf"})
            got = dl.headers_for(src)
            self.assertTrue(got["Authorization"].startswith("Bearer hf_"))

    def test_shared_store_refuses_an_absolute_sink(self):
        """The confused deputy at the sink field, one spelling past containment:
        an ABSOLUTE sink in this machine's own convention is never joined onto
        the root and is not foreign, so it lands where a foreign record chose.
        A runner over a shared store refuses it and leaves the job adoptable."""
        native = "C:\\abstraction-deputy\\evil.bin" if os.name == "nt" else "/etc/cron.d/evil"
        src = os.path.join(self.dir.name, "src.bin")
        with open(src, "wb") as f:
            f.write(b"x")
        jid = dl.submit(self.store, dl.Spec(
            sources=[dl.Source(scheme="file", locator=src)], sink=dl.Sink(final=native)))
        with self.assertRaises(dl.UnportableSink):
            dl.Runner(self.store, "nas", shared_store=True).run(jid)
        self.assertFalse(os.path.exists(native))
        self.assertNotEqual(self.store.load(jid).state, FAILED)

    def _raised(self, fn, *a):
        try:
            fn(*a)
        except Exception as e:  # noqa: BLE001 - the test wants the instance
            return e
        self.fail("nothing was raised")

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

    # -- containment ------------------------------------------------------
    #
    # The same cases as download/go/containment_test.go, deliberately. A record
    # one implementation refuses and another acts on is worse than either
    # behaviour on its own.

    def test_sink_may_not_escape_the_store_root(self):
        """A PC submits the record and a NAS adopts it, so the sink is a
        destination the SUBMITTER chose and the ADOPTER writes to, with the
        adopter's authority. Joining without checking resolved these to real
        files outside the store -- an ssh authorized_keys, a Startup directory.
        Reproduced against a store root of C:\\store\\jobs."""
        root = "C:\\store\\jobs"

        for p in (
            "../../../Users/victim/.ssh/authorized_keys",
            "..\\..\\..\\Startup\\evil.bat",
            "..",
            "a/../../b",            # clean on its own and still one level out
            "models/../../x.gguf",  # the climb is not at the front
        ):
            with self.assertRaises(dl.EscapesRoot, msg=p):
                dl._resolve_under(root, p)

        for p in (
            "models/x.gguf",
            "work/2f8a-b1",
            "models/org/repo/rev/x.gguf",
            "models/./x.gguf",
            "models/tmp/../x.gguf",  # clean it and it never leaves
        ):
            dl._resolve_under(root, p)  # must not raise

        self.assertEqual(
            dl._resolve_under(root, "models/org/repo/x.gguf"),
            os.path.join(root, "models", "org", "repo", "x.gguf"),
        )

    def test_refusal_names_the_path_from_the_record(self):
        """Spelled out in full, because Go and C++ must print this same string:
        see download/go/containment_test.go and
        download/cpp/test/test_sink_containment.cpp."""
        with self.assertRaises(dl.EscapesRoot) as caught:
            dl._resolve_under("C:\\store\\jobs", "../../../Users/victim/.ssh/authorized_keys")
        self.assertEqual(
            str(caught.exception),
            "download: sink path escapes the store root: "
            "../../../Users/victim/.ssh/authorized_keys",
        )

    def test_containment_is_not_a_prefix_test(self):
        r"""The classic way this fix ships broken: C:\store2 starts with
        C:\store, so a prefix test on the raw strings calls a different
        directory contained."""
        cases = [
            ("C:\\store", "C:\\store2\\x.gguf", False),
            ("C:\\store", "C:\\store\\x.gguf", True),
            ("C:\\store", "C:\\store", True),
            ("C:\\store\\", "C:\\store\\x.gguf", True),  # trailing separator
            ("/store", "/store2/x.gguf", False),
            ("/store", "/store/x.gguf", True),
            ("/", "/x.gguf", True),
            # A UNC root: the share is part of the root, so a sibling share is out.
            ("\\\\nas\\models\\store", "\\\\nas\\models\\store\\x.gguf", True),
            ("\\\\nas\\models\\store", "\\\\nas\\models\\store2\\x.gguf", False),
            ("\\\\nas\\models\\store", "\\\\nas\\other\\store\\x.gguf", False),
            # Nothing to be under: the store's binding is not a filesystem.
            ("", "models/x.gguf", True),
            ("", "../x.gguf", False),
            ("", "..", False),
        ]
        for root, resolved, want in cases:
            self.assertEqual(dl._under(root, resolved), want, (root, resolved))

        # Windows ignores case in a path and POSIX does not, so this is asked of
        # the platform the test is running on rather than assumed either way.
        self.assertEqual(
            dl._under("C:\\Store", "C:\\store\\x.gguf"), os.name == "nt"
        )

    def test_submit_refuses_an_escaping_sink(self):
        src = [dl.Source(scheme="https", locator="https://example.invalid/x.gguf")]
        for sink in (
            dl.Sink(final="../../../Users/victim/.ssh/authorized_keys"),
            dl.Sink(final="..\\..\\..\\Startup\\evil.bat"),
            dl.Sink(final="models/x.gguf", partial="../../../Startup/evil.bat"),
        ):
            with self.assertRaises(dl.EscapesRoot):
                dl.submit(self.store, dl.Spec(sources=src, sink=sink))

        dl.submit(
            self.store,
            dl.Spec(sources=src, sink=dl.Sink(final="models/org/repo/x.gguf")),
        )

    def test_local_sink_refuses_a_record_that_escaped(self):
        """Refused on the side that does the writing, which is the side that
        matters: a record can arrive in a shared store without passing through
        any submit of ours."""
        sink = dl.Sink(
            partial="work/abc", final="../../../Users/victim/.ssh/authorized_keys"
        )
        with self.assertRaises(dl.EscapesRoot):
            dl.local_sink(self.store, "abc", sink)

        partial, final = dl.local_sink(
            self.store, "abc", dl.Sink(partial="work/abc", final="models/x.gguf")
        )
        for got in (partial, final):
            self.assertTrue(got.startswith(self.dir.name), got)

    def test_escapes_root_answers_without_a_root(self):
        """escapes_root answers about a RECORD, which names no root -- that is
        what lets the three readers refuse the same records. An absolute path
        answers "": it is never joined onto the root, and what a delegate should
        do with one is a separate question this does not decide."""
        for p in (
            "../../../Users/victim/.ssh/authorized_keys",
            "..\\..\\..\\Startup\\evil.bat",
        ):
            self.assertNotEqual(dl.escapes_root(p), "", p)
        for p in (
            "",
            "models/x.gguf",
            "work/abc",
            "D:\\models\\x.gguf",
            "/mnt/models/x.gguf",
            "\\\\nas\\share\\x.gguf",
        ):
            self.assertEqual(dl.escapes_root(p), "", p)

    def test_sink_may_not_name_the_stores_own_files(self):
        """Contained, and still aimed at the store.

        Containment stopped a sink climbing OUT of the root and never stopped
        one naming what is IN it. A final of `jobs/<id>.json` overwrites a job
        record; a final of `work/<other>` overwrites another job's partial.
        Both are inside the root and both passed every check that existed.
        """
        me = "1757000000000-deadbeef"
        other = "1757000000001-cafebabe"
        for p in (
            "jobs/" + other + ".json",
            "jobs/" + me + ".json",
            "jobs/" + me + ".epoch.7",
            "jobs",
            "work",
            "work/" + other,
            "work/" + other + "/part",
            "services.json",
            "supervisor.json",
            "supervisor.json.tmp",
            "supervisor.sock",
            # The spellings a filesystem folds into the ones above. NTFS lands
            # `Jobs/x.json` in `jobs/`, so a rule that folded case only where
            # the host does would refuse this on Windows and accept it on Linux.
            "Jobs/" + other + ".json",
            "jobs\\" + other + ".json",
            "JOBS/" + other + ".json",
            "models/../jobs/" + other + ".json",
            "Supervisor.json",
            "WORK/" + other,
        ):
            self.assertNotEqual(dl.reserved_sink(me, p), "", p)

        # A job's own scratch is the one reserved path it may write -- the
        # default partial goes there -- and nothing else is spellable.
        for p in (
            "",
            "work/" + me,
            "work/" + me + "/part",
            "models/x.gguf",
            "jobsy/x.json",
            "a/jobs/x.json",
            "services.json.bak",
            "D:\\models\\x.gguf",
            "/mnt/models/x.gguf",
        ):
            self.assertEqual(dl.reserved_sink(me, p), "", p)

    def test_reserved_refusal_names_the_path_from_the_record(self):
        """Spelled out in full, because Go and C++ print this same string."""
        self.assertEqual(
            dl.reserved_sink(
                "1757000000000-deadbeef", "jobs/1757000000001-cafebabe.json"
            ),
            "download: sink path is reserved by the store: "
            "jobs/1757000000001-cafebabe.json",
        )

    def test_submit_and_local_sink_refuse_a_reserved_sink(self):
        src = [dl.Source(scheme="https", locator="https://example.invalid/x.gguf")]
        for sink in (
            dl.Sink(final="jobs/1757000000001-cafebabe.json"),
            dl.Sink(final="services.json"),
            dl.Sink(final="models/x.gguf", partial="jobs/1757000000001-cafebabe.json"),
            dl.Sink(final="models/x.gguf", partial="work/1757000000001-cafebabe"),
        ):
            with self.assertRaises(dl.ReservedPath, msg=str(sink)):
                dl.submit(self.store, dl.Spec(sources=src, sink=sink))

        # The id submit chose owns its own scratch, so the partial it invents
        # for itself is accepted -- the case a blanket ban on work/ would break.
        jid = dl.submit(
            self.store, dl.Spec(sources=src, sink=dl.Sink(final="models/x.gguf"))
        )
        spec = dl.spec_of(self.store.load(jid))
        self.assertEqual(spec.sink.partial, "work/" + jid)
        dl.local_sink(self.store, jid, spec.sink)
        with self.assertRaises(dl.ReservedPath):
            dl.local_sink(self.store, "1757000000001-cafebabe", spec.sink)

    def test_absolute_sink_is_refused_on_the_other_platform(self):
        """An absolute sink is honoured only by a machine whose convention it is
        written in.

        The contract claimed a foreign absolute path "fails with no such file
        rather than quietly creating a directory". That is true of reading and
        false of a sink: O_CREAT on "D:\\models\\x.gguf" under Linux makes a
        file of that literal name in the working directory, with a ".part"
        beside it.
        """
        windows = ["D:\\models\\x.gguf", "\\\\nas\\share\\x.gguf", "c:\\models\\x.gguf"]
        posix = ["/mnt/models/x.gguf", "/models/x.gguf"]
        native, foreign = (windows, posix) if os.name == "nt" else (posix, windows)

        for p in foreign:
            self.assertNotEqual(dl.foreign_path(p), "", p)
        for p in native:
            self.assertEqual(dl.foreign_path(p), "", p)
        for p in ("", "models/x.gguf", "work/abc"):
            self.assertEqual(dl.foreign_path(p), "", p)

        with self.assertRaises(dl.ForeignPathError):
            dl.Sink(final=foreign[0]).resolve("/store", "abc")

        # And a record naming another platform is still a VALID record: it is
        # correct on the machine that wrote it, so only the machine about to do
        # the writing may refuse it.
        dl.Spec(
            sources=[dl.Source(scheme="https", locator="https://example.invalid/x")],
            sink=dl.Sink(final=foreign[0]),
        ).validate()


class ClientTest(unittest.TestCase):
    """What an application gets to say, and what it must not have to know.

    Every assertion here was code the ComfyUI node wrote for itself before this
    existed. An application that has to re-derive dedupe, the partial name and
    take-delivery is one that ports to the next adopter by copy and paste.
    """

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.store = FileStore(os.path.join(self.dir.name, "store"))
        self.svc = dl.Client(self.store)
        # deliver returns the moment the record says TRANSFERRED, and the worker
        # is still releasing its lease at that point. Windows will not remove a
        # directory a thread still has a file open in, so the temp directory
        # cleanup failed about one run in three. There is no Close on a Client
        # to do this properly -- see feedback/2026-09-05-python-service.md.
        self.addCleanup(
            lambda: [t.join(timeout=30) for t in self.svc._workers]
        )

    def serve_bytes(self, n=64 * 1024):
        body, digest = payload(n)
        srv, url = serve(body)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return body, digest, url

    def test_submit_runs_it_without_being_told_to(self):
        body, digest, url = self.serve_bytes()
        final = os.path.join(self.dir.name, "models", "x.bin")
        jid = self.svc.submit(
            dl.Spec(
                artifact=dl.Artifact(digest=digest, size=len(body)),
                sources=[dl.Source(scheme="http", locator=url)],
                sink=dl.Sink(final=final),
            )
        )
        self.assertEqual(self.svc.deliver(jid, timeout=30).state, COMPLETE)
        with open(final, "rb") as f:
            self.assertEqual(f.read(), body)

    def test_asking_twice_is_one_piece_of_work(self):
        """Not two records, and not a second partial starting at zero racing the
        first to the same destination."""
        _, digest, url = self.serve_bytes()
        spec = lambda: dl.Spec(  # noqa: E731
            artifact=dl.Artifact(digest=digest, size=64 * 1024),
            sources=[dl.Source(scheme="http", locator=url)],
            sink=dl.Sink(final=os.path.join(self.dir.name, "models", "twice.bin")),
        )
        first = self.svc.submit(spec())
        self.assertEqual(self.svc.submit(spec()), first)
        self.assertEqual(len(self.svc.jobs()), 1)
        # Waited out rather than abandoned: the service starts work in a thread
        # nothing can stop, so a test that walks away leaves it writing into a
        # directory the test is deleting. There is no Close on a Client, in
        # either language -- see feedback/2026-09-05-python-service.md.
        self.svc.deliver(first, timeout=30)

    def test_a_finished_download_does_not_block_a_fresh_one(self):
        """"Download it again" is a real request. A completed record is history,
        not a claim on the destination."""
        body, digest, url = self.serve_bytes()
        final = os.path.join(self.dir.name, "models", "again.bin")
        spec = lambda: dl.Spec(  # noqa: E731
            artifact=dl.Artifact(digest=digest, size=len(body)),
            sources=[dl.Source(scheme="http", locator=url)],
            sink=dl.Sink(final=final),
        )
        first = self.svc.submit(spec())
        self.svc.deliver(first, timeout=30)
        os.remove(final)
        second = self.svc.submit(spec())
        self.assertNotEqual(second, first)
        # Waited out rather than abandoned: the service starts work in a thread
        # nothing can stop, so a test that walks away leaves it writing into a
        # directory the test is deleting. There is no Close on a Client, in
        # either language -- see feedback/2026-09-05-python-service.md.
        self.svc.deliver(second, timeout=30)

    def test_delivered_bytes_are_not_fetched_again(self):
        """With a digest there is no "again": the bytes are the identity. A
        second request for them at the same path is the finished record, and at
        another path it is a copy of the store's own file, not a fetch."""
        body, digest, url = self.serve_bytes()
        final = os.path.join(self.dir.name, "models", "held.bin")
        spec = lambda where: dl.Spec(  # noqa: E731
            artifact=dl.Artifact(digest=digest, size=len(body)),
            sources=[dl.Source(scheme="http", locator=url)],
            sink=dl.Sink(final=where),
        )
        first = self.svc.submit(spec(final))
        self.svc.deliver(first, timeout=30)
        self.assertEqual(self.svc.submit(spec(final)), first)
        elsewhere = os.path.join(self.dir.name, "models", "copy.bin")
        second = self.svc.submit(spec(elsewhere))
        self.assertNotEqual(second, first)
        sources = dl.spec_of(self.store.load(second)).sources
        self.assertEqual((sources[0].scheme, sources[0].attrs.get("job")), ("file", first))
        self.svc.deliver(second, timeout=30)

    def test_the_application_never_names_the_partial(self):
        """An absolute final puts the partial beside the artifact, so delivery is
        a rename on one filesystem rather than a copy into the final NAME -- the
        truncated-file failure this layer exists to refuse."""
        final = os.path.join(self.dir.name, "models", "named.bin")
        jid = dl.submit(
            self.store,
            dl.Spec(
                sources=[dl.Source(scheme="http", locator="http://127.0.0.1:9/x")],
                sink=dl.Sink(final=final),
            ),
        )
        self.assertEqual(
            dl.spec_of(self.store.load(jid)).sink.partial,
            dl.portable(final) + ".part",
        )
        relative = dl.submit(
            self.store,
            dl.Spec(
                sources=[dl.Source(scheme="http", locator="http://127.0.0.1:9/x")],
                sink=dl.Sink(final="models/x.bin"),
            ),
        )
        self.assertEqual(
            dl.spec_of(self.store.load(relative)).sink.partial, "work/" + relative
        )

    def test_get_takes_a_url_and_nothing_else(self):
        body, _, url = self.serve_bytes()
        os.makedirs(os.path.join(self.dir.name, "into"))
        jid = self.svc.get(url, os.path.join(self.dir.name, "into"))
        self.svc.deliver(jid, timeout=30)
        with open(os.path.join(self.dir.name, "into", "blob.bin"), "rb") as f:
            self.assertEqual(f.read(), body)

    def test_deliver_collects_rather_than_leaving_it_transferred(self):
        """A transferred job is finished and proven but not collected. Without
        the second half it waits in the store forever for somebody who never
        comes -- which is what fills a download list with transfers that ended
        days ago."""
        body, digest, url = self.serve_bytes()
        jid = self.svc.submit(
            dl.Spec(
                artifact=dl.Artifact(digest=digest, size=len(body)),
                sources=[dl.Source(scheme="http", locator=url)],
                sink=dl.Sink(final=os.path.join(self.dir.name, "models", "coll.bin")),
            )
        )
        self.assertEqual(self.svc.deliver(jid, timeout=30).state, COMPLETE)

    def test_a_failure_reaches_the_caller(self):
        jid = self.svc.submit(
            dl.Spec(
                sources=[dl.Source(scheme="http", locator="http://127.0.0.1:9/gone")],
                sink=dl.Sink(final=os.path.join(self.dir.name, "models", "no.bin")),
            )
        )
        with self.assertRaises(dl.DownloadError):
            self.svc.deliver(jid, timeout=30)

    def test_a_refused_source_ends_the_job(self):
        """A 401 from a gated repository is the source answering, not the
        transfer stumbling. Left adoptable it is fetched again on every sweep
        for as long as the store exists, and nothing waiting on the record can
        stop waiting."""
        srv, url = serve(b"")
        srv.RequestHandlerClass = refusing(401)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)

        jid = self.svc.submit(
            dl.Spec(
                sources=[dl.Source(scheme="http", locator=url)],
                sink=dl.Sink(final=os.path.join(self.dir.name, "models", "gated.bin")),
            )
        )
        with self.assertRaises(dl.DownloadError):
            self.svc.deliver(jid, timeout=30)

        rec = self.store.load(jid)
        self.assertEqual(rec.state, FAILED)
        self.assertIn("401", rec.error)
        # And nothing sweeping this store will pick it up again.
        self.assertEqual([r.id for r in self.store.orphans()], [])
        self.assertEqual(dl.Runner(self.store).adopt(), 0)

    def test_a_waiter_ends_when_nobody_is_working_the_job(self):
        """The 90-minute hang, in one test.

        Every waiter this layer has ended on something the record SAYS: a
        terminal state, or an error somebody wrote down. A worker that dies
        before it records anything -- a supervisor that stopped after the nudge,
        a claim that was never won -- writes nothing at all, and a waiter with
        only those two exits waits for a transfer that is not happening until
        somebody kills it.
        """
        self.svc.runner.lease_ttl = 0.2
        self.svc.runner.run = lambda job_id: None  # claims nothing, records nothing

        jid = self.svc.submit(
            dl.Spec(
                sources=[dl.Source(scheme="http", locator="http://127.0.0.1:9/x")],
                sink=dl.Sink(final=os.path.join(self.dir.name, "models", "nobody.bin")),
            )
        )
        started = time.monotonic()
        with self.assertRaises(dl.DownloadError):
            self.svc.deliver(jid)
        self.assertLess(time.monotonic() - started, 10)

    def test_a_job_that_failed_is_not_retried_immediately(self):
        """A source that is down is not asked again every thirty seconds until
        somebody notices. The lease epoch is the attempt count, so the wait
        grows without the record growing a field."""
        jid = self.svc.submit(
            dl.Spec(
                sources=[dl.Source(scheme="http", locator="http://127.0.0.1:9/down")],
                sink=dl.Sink(final=os.path.join(self.dir.name, "models", "down.bin")),
            )
        )
        with self.assertRaises(dl.DownloadError):
            self.svc.deliver(jid, timeout=30)

        rec = self.store.load(jid)
        self.assertFalse(rec.terminal(), "a connection refused is not a refusal")
        self.assertIn(jid, [r.id for r in self.store.orphans()])
        self.assertEqual(dl.Runner(self.store).adopt(), 0, "it was tried again at once")
        self.assertGreater(dl.retry_after(rec), time.time())

        # A person asking again is not the sweep asking again: submitting clears
        # the last error, and a job with no error waits for nothing.
        self.svc.submit(
            dl.Spec(
                sources=[dl.Source(scheme="http", locator="http://127.0.0.1:9/down")],
                sink=dl.Sink(final=os.path.join(self.dir.name, "models", "down.bin")),
            )
        )
        for t in self.svc._workers:
            t.join(timeout=30)


def refusing(status):
    class Refuse(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *a):
            pass

    return Refuse


class SpyServer(RangeServer):
    """Serves the bytes and keeps every header it was sent."""

    seen = None

    def do_GET(self):
        type(self).seen.append(dict(self.headers.items()))
        RangeServer.do_GET(self)


# Every attribute this module defines, plus attributes chosen to look exactly
# like headers -- including the one the transport sets for itself.
#
# The Go implementation copied anything it did not recognise into the request,
# so each of these was one forgotten branch away from a third party. The
# assertion below names no particular attribute: it asserts that NONE of them
# arrive, which is a rule rather than a list.
LEAKY_ATTRS = {
    dl.CREDENTIAL_ATTR: "spy",
    dl.CREDENTIAL_HEADER_ATTR: "X-Auth",
    "store": "ollama",
    "boundaries": "16777216",
    "X-Internal-Span-Table": "a-span-table-nobody-outside-should-see",
    "Authorization": "Bearer this-value-is-inert",
    "Cookie": "session=inert",
    "User-Agent": "leaky-agent-value",
    "whatever-anyone-adds-next": "the-value-that-would-have-leaked",
}

SPY_SECRET = "resolved-on-this-machine"

# Headers the client sets on its own behalf, so their presence proves nothing.
# Their VALUES still must never be a record's.
TRANSPORT_OWNED = {"user-agent", "range", "host", "accept-encoding", "connection"}


class HeadersTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.store = FileStore(os.path.join(self.dir.name, "store"))
        self.svc = dl.Client(self.store)
        self.addCleanup(lambda: [t.join(timeout=30) for t in self.svc._workers])
        os.environ["ABSTRACTION_CRED_SPY"] = SPY_SECRET
        os.environ["ABSTRACTION_CRED_SPY_HOSTS"] = "127.0.0.1"
        self.addCleanup(lambda: os.environ.pop("ABSTRACTION_CRED_SPY", None))
        self.addCleanup(lambda: os.environ.pop("ABSTRACTION_CRED_SPY_HOSTS", None))

    def spy(self, n=64 * 1024):
        body, digest = payload(n)
        seen = []
        handler = type("S", (SpyServer,), {"body": body, "seen": seen})
        srv = http.server.HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        return body, digest, "http://127.0.0.1:%d/blob.bin" % srv.server_port, seen

    def test_no_source_attribute_ever_reaches_the_wire(self):
        body, digest, url, seen = self.spy()
        jid = self.svc.submit(
            dl.Spec(
                artifact=dl.Artifact(digest=digest, size=len(body)),
                sources=[dl.Source(scheme="http", locator=url, attrs=dict(LEAKY_ATTRS))],
                sink=dl.Sink(final=os.path.join(self.dir.name, "models", "x.bin")),
            )
        )
        self.svc.deliver(jid, timeout=30)
        self.assertTrue(seen, "the server was never asked for anything")
        for i, sent in enumerate(seen):
            lower = {k.lower(): v for k, v in sent.items()}
            for name, value in LEAKY_ATTRS.items():
                if name.lower() not in TRANSPORT_OWNED:
                    self.assertNotIn(
                        name.lower(),
                        lower,
                        "request %d carried attribute %r as a header" % (i, name),
                    )
                self.assertNotIn(
                    value,
                    list(sent.values()),
                    "request %d sent the value of attribute %r" % (i, name),
                )
            # And the credential still arrives, under the header the source
            # named. A test that only proved nothing was sent would pass just as
            # well against a fetcher that sent no headers at all.
            self.assertEqual(lower.get("x-auth"), "Bearer " + SPY_SECRET)

    def test_an_explicit_header_is_sent(self):
        body, digest, url, seen = self.spy()
        jid = self.svc.submit(
            dl.Spec(
                artifact=dl.Artifact(digest=digest, size=len(body)),
                sources=[
                    dl.Source(
                        scheme="http",
                        locator=url,
                        headers={"X-Repo": "openabstractions"},
                    )
                ],
                sink=dl.Sink(final=os.path.join(self.dir.name, "models", "y.bin")),
            )
        )
        self.svc.deliver(jid, timeout=30)
        for sent in seen:
            lower = {k.lower(): v for k, v in sent.items()}
            self.assertEqual(lower.get("x-repo"), "openabstractions")

    def test_a_record_may_not_carry_a_resolved_header(self):
        for name in ("Authorization", "authorization", "Proxy-Authorization", "Cookie", "Range", " "):
            spec = dl.Spec(
                sources=[
                    dl.Source(
                        scheme="https",
                        locator="https://example.invalid/x",
                        headers={name: "v"},
                    )
                ],
                sink=dl.Sink(final="models/x.gguf"),
            )
            with self.assertRaises(dl.DownloadError, msg=name):
                spec.validate()
        # Including the one the source itself nominated, which is in no fixed
        # list.
        spec = dl.Spec(
            sources=[
                dl.Source(
                    scheme="https",
                    locator="https://example.invalid/x",
                    attrs={dl.CREDENTIAL_ATTR: "hf", dl.CREDENTIAL_HEADER_ATTR: "X-Auth"},
                    headers={"x-auth": "Bearer this-would-shadow-the-credential"},
                )
            ],
            sink=dl.Sink(final="models/x.gguf"),
        )
        with self.assertRaises(dl.DownloadError):
            spec.validate()

    def test_an_old_records_attributes_are_not_sent(self):
        """What a reader does with a record written before the split: the
        attributes are attributes, and none of them are sent."""
        spec = dl.Spec.from_dict(
            {
                "artifact": {"size": 4},
                "sources": [
                    {
                        "scheme": "https",
                        "locator": "https://example.invalid/x",
                        "attrs": {"store": "ollama", "X-Legacy": "whatever-this-was"},
                    }
                ],
                "sink": {"final": "models/x.gguf"},
            }
        )
        self.assertEqual(dl.headers_for(spec.sources[0]), {})

    def test_a_spec_without_headers_is_unchanged_on_the_wire(self):
        d = dl.Spec(
            artifact=dl.Artifact(size=4),
            sources=[dl.Source(scheme="https", locator="https://example.invalid/x")],
            sink=dl.Sink(partial="work/a", final="models/x.gguf"),
        ).to_dict()
        self.assertNotIn("headers", d["sources"][0])


if __name__ == "__main__":
    unittest.main()
