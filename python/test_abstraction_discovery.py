"""Tests for the discovery client, against a fake supervisor in this process.

The real server is Go and lives in the supervisor, so none of these tests can
wait for it. They stand up a listener on the same endpoint the contract names —
a real named pipe on Windows, a real unix socket elsewhere — and drive the
client through every answer the contract distinguishes.

The load-bearing one is `test_accepts_and_never_writes`. A synchronous client
passes every other test in this file and hangs forever on that one, which is
exactly the failure rule 2 exists to prevent, and exactly the failure that is
invisible until somebody runs it on Windows against a real pipe.

The server here also declares its own kernel32 bindings rather than importing
the client's. A test that shares the module's ctypes prototypes cannot catch a
wrong prototype: both sides would be wrong in the same direction.
"""

import ctypes
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest

import abstraction_discovery as disco

IS_WINDOWS = sys.platform == "win32"

GOOD = {
    "content": ["abstraction.discovery/base@1"],
    "critical": ["abstraction.discovery/base@1"],
    "owner": "jobd@host:9242",
    "host": "host",
    "store": "",  # filled in per test
    "delegates_to": "here",
    "pid": 9242,
    "started_at": "2026-09-05T06:34:49.123456Z",
}


def response(store, **overrides):
    doc = dict(GOOD, store=store)
    doc.update(overrides)
    return json.dumps(doc, separators=(",", ":")).encode("utf-8") + b"\n"


# --------------------------------------------------------- the fake server ---


if IS_WINDOWS:
    from ctypes import wintypes

    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _k32.CreateNamedPipeW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD,
        wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
    ]
    _k32.CreateNamedPipeW.restype = wintypes.HANDLE
    _k32.ConnectNamedPipe.argtypes = [wintypes.HANDLE, ctypes.c_void_p]
    _k32.ConnectNamedPipe.restype = wintypes.BOOL
    _k32.ReadFile.argtypes = [
        wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD), ctypes.c_void_p,
    ]
    _k32.ReadFile.restype = wintypes.BOOL
    _k32.WriteFile.argtypes = _k32.ReadFile.argtypes
    _k32.WriteFile.restype = wintypes.BOOL
    _k32.CreateFileW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
        ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
    ]
    _k32.CreateFileW.restype = wintypes.HANDLE
    for _name in ("FlushFileBuffers", "DisconnectNamedPipe", "CloseHandle"):
        getattr(_k32, _name).argtypes = [wintypes.HANDLE]
        getattr(_k32, _name).restype = wintypes.BOOL

    PIPE_ACCESS_DUPLEX = 3
    PIPE_TYPE_BYTE = 0  # byte mode, not message mode: rule 8
    PIPE_UNLIMITED_INSTANCES = 255
    ERROR_PIPE_CONNECTED = 535
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value


class FakeSupervisor:
    """One connection's worth of supervisor.

    `reply` of None means accept, read the request, and never write anything —
    the case a naive client never comes back from.
    """

    def __init__(self, name, reply):
        self.name = name
        self.reply = reply
        self.request = b""
        self.stop = threading.Event()
        self.path = disco.endpoint_path(name)
        self._open()
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    # -- windows ------------------------------------------------------------

    def _open(self):
        if IS_WINDOWS:
            self.handle = _k32.CreateNamedPipeW(
                self.path,
                PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_BYTE,
                PIPE_UNLIMITED_INSTANCES,
                4096, 4096, 0, None,
            )
            assert self.handle != INVALID_HANDLE_VALUE, ctypes.get_last_error()
            return
        os.makedirs(os.path.dirname(self.path), mode=0o700, exist_ok=True)
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(self.path)
        os.chmod(self.path, 0o600)
        self.sock.listen(4)

    def _serve(self):
        if IS_WINDOWS:
            self._serve_pipe()
        else:
            self._serve_socket()

    def _serve_pipe(self):
        h = self.handle
        if not _k32.ConnectNamedPipe(h, None):
            if ctypes.get_last_error() != ERROR_PIPE_CONNECTED:
                return
        if self.stop.is_set():
            return
        buf = ctypes.create_string_buffer(4096)
        moved = wintypes.DWORD(0)
        if _k32.ReadFile(h, buf, 4096, ctypes.byref(moved), None):
            self.request = buf.raw[: moved.value]
        if self.reply is None:
            self.stop.wait()  # accept and never write
        else:
            out = ctypes.create_string_buffer(self.reply, len(self.reply))
            sent = 0
            while sent < len(self.reply):
                if not _k32.WriteFile(
                    h, ctypes.byref(out, sent), len(self.reply) - sent,
                    ctypes.byref(moved), None,
                ):
                    break
                sent += moved.value
            _k32.FlushFileBuffers(h)
        _k32.DisconnectNamedPipe(h)

    def _serve_socket(self):
        try:
            conn, _ = self.sock.accept()
        except OSError:
            return
        with conn:
            # Recorded, not discarded: the request assertion is the only check
            # that the client frames its object the way the contract says.
            self.request = conn.recv(4096)
            if self.reply is None:
                self.stop.wait()
            else:
                try:
                    conn.sendall(self.reply)
                except OSError:
                    pass

    def close(self):
        self.stop.set()
        if IS_WINDOWS:
            # Unblock a thread still sitting in ConnectNamedPipe. Failures here
            # mean it is already connected, which is equally fine.
            h = _k32.CreateFileW(self.path, 0x80000000, 0, None, 3, 0, None)
            if h != INVALID_HANDLE_VALUE:
                _k32.CloseHandle(h)
            self.thread.join(timeout=2)
            _k32.CloseHandle(self.handle)
        else:
            self.sock.close()
            self.thread.join(timeout=2)
            try:
                os.unlink(self.path)
            except OSError:
                pass


# ------------------------------------------------------------------ tests ---


class DiscoveryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.store = self.dir.name
        if not IS_WINDOWS:
            # Keep the socket path inside the 104-byte cap and out of the way of
            # whatever else is on the machine.
            os.environ["XDG_RUNTIME_DIR"] = self.dir.name
        self.name = "abstraction-" + os.urandom(16).hex()

    def publish(self, service="abstraction.downloads", name=None):
        """Write the registry. This is the only way to learn an endpoint."""
        path = os.path.join(self.store, disco.REGISTRY)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({"services": {service: name or self.name}}, fh)

    def listen(self, reply):
        server = FakeSupervisor(self.name, reply)
        self.addCleanup(server.close)
        return server

    def ask(self, service="abstraction.downloads"):
        return disco.ask(self.store, service)

    # -- absent -------------------------------------------------------------

    def test_no_registry_is_absent(self):
        self.assertEqual(self.ask(), disco.ABSENT)

    def test_service_not_in_registry_is_absent(self):
        self.publish(service="abstraction.rights")
        self.assertEqual(self.ask("abstraction.downloads"), disco.ABSENT)

    def test_nothing_listening_is_absent(self):
        self.publish()  # a registry entry, and no server for it
        started = time.monotonic()
        self.assertEqual(self.ask(), disco.ABSENT)
        self.assertLess(time.monotonic() - started, 1.0)

    def test_asking_never_creates_an_endpoint(self):
        """Rule 4. Only a supervisor listens."""
        self.publish()
        self.assertEqual(self.ask(), disco.ABSENT)
        if IS_WINDOWS:
            h = _k32.CreateFileW(disco.endpoint_path(self.name), 0x80000000, 0, None, 3, 0, None)
            self.assertEqual(h, INVALID_HANDLE_VALUE, "the client created a pipe")
        else:
            self.assertFalse(os.path.exists(disco.endpoint_path(self.name)))

    def test_stale_endpoint_is_absent_and_is_not_removed(self):
        """Rule 6: only the SERVER unlinks. A client that tidies up can delete a
        socket a supervisor bound a millisecond earlier, leaving it listening on
        an unlinked inode and undiscoverable — worse than the bug being fixed."""
        self.publish()
        path = disco.endpoint_path(self.name)
        if IS_WINDOWS:
            # A killed pipe server leaves nothing behind at all; the stale case
            # on Windows is a registry entry naming a pipe that no longer exists.
            self.assertEqual(self.ask(), disco.ABSENT)
        else:
            os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
            open(path, "wb").close()  # the corpse a killed process leaves
            self.assertEqual(self.ask(), disco.ABSENT)
            self.assertTrue(os.path.exists(path), "the client unlinked a socket")

    def test_malformed_json_is_absent(self):
        self.publish()
        self.listen(b"{this is not json\n")
        self.assertEqual(self.ask(), disco.ABSENT)

    def test_eof_before_the_newline_is_absent(self):
        self.publish()
        self.listen(response(self.store).rstrip(b"\n"))  # a partial line, then close
        self.assertEqual(self.ask(), disco.ABSENT)

    def test_line_over_the_cap_is_absent(self):
        self.publish()
        doc = dict(GOOD, store=self.store, padding="x" * 70000)
        self.listen(json.dumps(doc).encode("utf-8") + b"\n")
        self.assertEqual(self.ask(), disco.ABSENT)

    def test_store_mismatch_is_absent(self):
        self.publish()
        self.listen(response("/somebody/elses/store"))
        self.assertEqual(self.ask(), disco.ABSENT)

    def test_missing_store_is_absent(self):
        self.publish()
        doc = dict(GOOD)
        doc.pop("store")
        self.listen(json.dumps(doc).encode("utf-8") + b"\n")
        self.assertEqual(self.ask(), disco.ABSENT)

    def test_error_object_is_absent(self):
        self.publish()
        self.listen(b'{"error":"unknown ask"}\n')
        self.assertEqual(self.ask(), disco.ABSENT)

    def test_accepts_and_never_writes(self):
        """The whole reason rule 2 says overlapped I/O.

        A server that accepts the connection and then says nothing. A
        synchronous ReadFile on a Windows pipe never returns from this, and no
        timeout above it can interrupt it.
        """
        self.publish()
        self.listen(None)
        started = time.monotonic()
        self.assertEqual(self.ask(), disco.ABSENT)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 1.0, "the client did not abandon the read")
        self.assertGreaterEqual(elapsed, 0.15, "it gave up before its deadline")

    def test_the_deadline_is_one_budget_not_one_per_phase(self):
        """Rule 2 again: connect, write and read share 200 ms, so a stalled read
        cannot be added to a stalled connect."""
        self.publish()
        self.listen(None)
        started = time.monotonic()
        disco.ask(self.store, "abstraction.downloads", deadline_ms=200)
        self.assertLess(time.monotonic() - started, 0.5)

    # -- present ------------------------------------------------------------

    def test_present(self):
        self.publish()
        server = self.listen(response(self.store))
        self.assertEqual(self.ask(), disco.PRESENT)
        self.assertEqual(server.request, b'{"ask":"who"}\n')

    def test_present_with_no_critical_list(self):
        self.publish()
        doc = dict(GOOD, store=self.store)
        doc.pop("critical")
        self.listen(json.dumps(doc).encode("utf-8") + b"\n")
        self.assertEqual(self.ask(), disco.PRESENT)

    def test_delegates_to_is_advisory(self):
        """A client must work without understanding it."""
        self.publish()
        self.listen(response(self.store, delegates_to="something-invented-tomorrow"))
        self.assertEqual(self.ask(), disco.PRESENT)

    def test_trailing_separator_on_the_store_still_matches(self):
        self.publish()
        self.listen(response(self.store + os.sep))
        self.assertEqual(self.ask(), disco.PRESENT)

    def test_two_names_one_endpoint(self):
        """The whole mechanism of service-topology.md: one process serving two
        services publishes one endpoint under two names, and no client can tell
        that from two processes."""
        path = os.path.join(self.store, disco.REGISTRY)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({"services": {
                "abstraction.jobs": self.name,
                "abstraction.downloads": self.name,
            }}, fh)
        self.listen(response(self.store))
        self.assertEqual(self.ask("abstraction.jobs"), disco.PRESENT)

    # -- incompatible -------------------------------------------------------

    def test_unknown_critical_name_is_incompatible(self):
        """Not absent. This is a NEWER supervisor, and the caller must not hand
        work over — but it is not an error to report either."""
        self.publish()
        self.listen(response(
            self.store,
            critical=["abstraction.discovery/base@1", "abstraction.discovery/leases@2"],
        ))
        self.assertEqual(self.ask(), disco.INCOMPATIBLE)

    def test_incompatible_wins_over_an_unknown_content_name(self):
        """`content` is advisory; only `critical` decides."""
        self.publish()
        self.listen(response(self.store, content=["something/new@9"]))
        self.assertEqual(self.ask(), disco.PRESENT)

    def test_store_mismatch_beats_incompatible(self):
        """A supervisor for a different store is not this caller's supervisor,
        so its extension names say nothing about ours."""
        self.publish()
        self.listen(response("/elsewhere", critical=["unknown@1"]))
        self.assertEqual(self.ask(), disco.ABSENT)

    # -- absence is not an error -------------------------------------------

    def test_absence_logs_nothing_above_debug(self):
        """Rule 1. A machine with no supervisor is the ordinary machine."""
        import logging

        records = []

        class Collector(logging.Handler):
            def emit(self, record):
                records.append(record)

        log = logging.getLogger("abstraction.discovery")
        handler = Collector(level=logging.INFO)
        log.addHandler(handler)
        log.setLevel(logging.DEBUG)
        self.addCleanup(log.removeHandler, handler)
        self.publish()
        self.assertEqual(self.ask(), disco.ABSENT)
        self.assertEqual([r.getMessage() for r in records], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
