"""Discovery over local IPC, in Python — the client half only.

https://github.com/openabstractions/abstractions/blob/main/docs/discovery-ipc.md
and the service-topology document beside it are the contract. This
file is written against those two documents and against nothing else: it does
not read the Go client, and the C++ client in abstraction-job does not read
this. If the two agree it is because the contract said enough, and where they
had to agree by accident that is a defect in the contract, not a feature here.

    import abstraction_discovery as disco

    answer = disco.ask(store_root, "abstraction.downloads")
    if answer is disco.PRESENT:
        ...                       # hand the work over
    elif answer is disco.INCOMPATIBLE:
        ...                       # a NEWER supervisor. Do NOT hand work over
    else:
        ...                       # nobody is listening. The ordinary case

Three things this deliberately is not:

  * It is not a server. Only a supervisor listens, and the supervisor is Go.
    Asking never creates an endpoint, so there is no listener here at all.
  * It never raises. Absence is what a machine without a supervisor looks like,
    which is most machines. A caller that has to write `try:` around this has
    been handed an error where the answer was "no".
  * It never caches. `present` is true for as long as the connection that
    proved it, which is already over.

Standard library only, like the rest of this repository. On Windows that costs
real work: a synchronous read on a named pipe cannot be interrupted, and
`socket.settimeout` does not exist for pipes, so the 200 ms deadline is
enforced with genuine overlapped I/O through ctypes. See `_win.py` notes below
— there is no shortcut, and a client that pretends otherwise hangs forever
against a server that accepts and never writes.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import socket
import sys
import time
from typing import FrozenSet, Optional

# Rule 1: never a warning, never a log line above debug. A machine with no
# supervisor is not having a problem.
_log = logging.getLogger("abstraction.discovery")

# The three answers. Strings rather than an enum so a caller can print one, and
# so the value crossing into a log or a test assertion is the same word the
# contract uses.
ABSENT = "absent"
PRESENT = "present"
INCOMPATIBLE = "incompatible"

# The registry, service name to endpoint. service-topology.md gives the JSON but
# never says which file in the store holds it; this is that decision, and it has
# to be identical in every implementation or two correct clients read two
# different files and both report absent.
REGISTRY = "services.json"

# The request. One object, one 0x0A, no \r, no BOM, no indenting.
_REQUEST = b'{"ask":"who"}\n'

# What this client understands. A `critical` name outside this set is a newer
# supervisor, which is `incompatible` and not `absent`.
KNOWN_CRITICAL: FrozenSet[str] = frozenset({"abstraction.discovery/base@1"})

# One deadline, 200 ms, across connect, write and read together.
DEADLINE_MS = 200

# A line over 64 KiB is absent. The cap exists because a hostile local process
# that can reach the endpoint can otherwise feed unbounded bytes into a client
# that was only ever going to read one object.
MAX_LINE = 64 * 1024

_IS_WINDOWS = sys.platform == "win32"


# ---------------------------------------------------------------- registry ---


def endpoint_for(store_root: str, service: str) -> str:
    """The endpoint name a service is published under, or "" for absent.

    The name is 128 random bits in hex, invented by whoever bound it. It is not
    derived from the store path and must never be: case folding, `\\\\?\\`
    prefixes, short names, UNC versus mapped drive, `/private` symlinks and HFS+
    normalisation each give two names for one store, and every one of them fails
    by silently reporting absent.

    Being able to read the registry is therefore what confers the reference.
    """
    try:
        with open(os.path.join(store_root, REGISTRY), "rb") as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        # No store, no registry, or a registry somebody was halfway through
        # writing. Nothing is listening as far as this caller is concerned.
        return ""
    if not isinstance(doc, dict):
        return ""
    services = doc.get("services")
    if not isinstance(services, dict):
        return ""
    name = services.get(service)
    if not isinstance(name, str) or not name:
        return ""
    return name


def endpoint_path(name: str) -> str:
    """Where an endpoint name lives on this platform.

    Windows is a named pipe. macOS and Linux are a unix socket under
    XDG_RUNTIME_DIR, falling back to a 0700 directory in /tmp keyed by uid.

    Not an abstract socket: those carry no permissions at all, so any uid in the
    namespace could connect, and they are per-netns, so a container or Flatpak
    adopter could not reach a host supervisor.
    """
    if _IS_WINDOWS:
        return r"\\.\pipe" + "\\" + name
    runtime = os.environ.get("XDG_RUNTIME_DIR")
    if runtime:
        return os.path.join(runtime, name + ".sock")
    return os.path.join("/tmp", "abstraction-%d" % os.getuid(), name + ".sock")


# ------------------------------------------------------------------- asking ---


def ask(
    store_root: str,
    service: str,
    deadline_ms: int = DEADLINE_MS,
    known_critical: FrozenSet[str] = KNOWN_CRITICAL,
) -> str:
    """Resolve `service`, connect, ask "who", and return one of three answers.

    Never raises, never warns, never caches. A false `absent` is safe — the
    lease stops two owners — and costs a transfer that could have been handed
    off; a false `present` is not safe, which is why every doubt resolves to
    absent.
    """
    # Rule 2, taken literally: ONE deadline, computed at entry. Everything from
    # here on spends the same budget, so a client cannot be walked through three
    # 200 ms waits by a server that stalls each phase in turn.
    deadline = time.monotonic() + deadline_ms / 1000.0

    name = endpoint_for(store_root, service)
    if not name:
        _log.debug("no registry entry for %s in %s", service, store_root)
        return ABSENT

    line = _exchange(endpoint_path(name), deadline)
    if line is None:
        return ABSENT
    return _interpret(line, store_root, known_critical)


def _interpret(line: bytes, store_root: str, known_critical: FrozenSet[str]) -> str:
    """The response, turned into one of the three answers.

    Everything malformed is absent. The single case that is not absent is a
    `critical` name this client does not know, because that is a supervisor
    newer than this code and handing work to it would be handing work to
    something whose rules we cannot see.
    """
    try:
        doc = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        _log.debug("unparseable response")
        return ABSENT
    if not isinstance(doc, dict):
        return ABSENT

    # {"error": ...} answers a request this client did not send. It carries no
    # store and describes no supervisor, so there is nothing here to hand work
    # to.
    if "error" in doc:
        _log.debug("server refused: %r", doc.get("error"))
        return ABSENT

    # The store check comes BEFORE the critical check on purpose: a response
    # from a different store is not this caller's supervisor at all, so its
    # extension names say nothing about whether OUR supervisor is compatible.
    # Answering `incompatible` there would stop a caller on account of a daemon
    # it was never going to talk to.
    theirs = doc.get("store")
    if not isinstance(theirs, str) or not _same_store(theirs, store_root):
        _log.debug("store mismatch: %r is not %r", theirs, store_root)
        return ABSENT

    critical = doc.get("critical", [])
    if not isinstance(critical, list):
        return ABSENT
    for feature in critical:
        # A non-string here is a name this client cannot possibly know, which is
        # the incompatible case and not the malformed one. Refusing to hand work
        # over is the safe half of that ambiguity.
        if not isinstance(feature, str) or feature not in known_critical:
            _log.debug("critical feature not understood: %r", feature)
            return INCOMPATIBLE

    return PRESENT


def _same_store(theirs: str, ours: str) -> bool:
    """Whether a response describes the store this client opened.

    Trailing separators only. NOT realpath, NOT case folding, NOT normalisation:
    the endpoint section of the contract spends a paragraph on why deriving one
    string from a path is a trap, and every one of those traps applies just as
    hard to comparing two path strings. So this compares what was written, and
    the burden is on the supervisor to publish the same string its callers hold.
    """
    return theirs.rstrip("/\\") == ours.rstrip("/\\")


def _exchange(path: str, deadline: float) -> Optional[bytes]:
    """Connect, send the request, read one line. None means absent."""
    if _IS_WINDOWS:
        return _win_exchange(path, deadline)
    return _unix_exchange(path, deadline)


def _remaining_ms(deadline: float) -> int:
    left = deadline - time.monotonic()
    return 0 if left <= 0 else int(left * 1000)


def _line_from(chunks) -> Optional[bytes]:
    """One 0x0A-terminated line out of what has been read, or None.

    None is both "no newline yet" and "the newline arrived past the cap"; the
    read loops treat the second as absent by way of the same length check they
    already apply to a line that never terminates.
    """
    buf = b"".join(chunks)
    cut = buf.find(b"\n")
    if cut < 0 or cut > MAX_LINE:
        return None
    return buf[:cut]


# ------------------------------------------------------------------- posix ---


def _unix_exchange(path: str, deadline: float) -> Optional[bytes]:
    """macOS and Linux. A unix socket has a real timeout, so this is the short
    half of the file; the interesting one is below."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        left = _remaining_ms(deadline)
        if left <= 0:
            return None
        sock.settimeout(left / 1000.0)
        try:
            sock.connect(path)
        except OSError as exc:
            # Nothing listening, a stale socket file left by a killed process,
            # or a directory that is not there. Rule 6: a client NEVER unlinks.
            # Removing a path a supervisor bound a millisecond ago leaves it
            # listening on an unlinked inode and undiscoverable, which is worse
            # than the stale file.
            _log.debug("connect %s: %s", path, errno.errorcode.get(exc.errno, exc))
            return None

        left = _remaining_ms(deadline)
        if left <= 0:
            return None
        sock.settimeout(left / 1000.0)
        try:
            sock.sendall(_REQUEST)
        except OSError:
            return None

        chunks, total = [], 0
        while True:
            left = _remaining_ms(deadline)
            if left <= 0:
                return None
            sock.settimeout(left / 1000.0)
            try:
                chunk = sock.recv(4096)
            except OSError:
                return None  # including socket.timeout, which is an OSError
            if not chunk:
                return None  # EOF before the newline
            chunks.append(chunk)
            total += len(chunk)
            line = _line_from(chunks)
            if line is not None:
                return line
            if total > MAX_LINE:
                return None  # over the cap and still no newline
    finally:
        sock.close()


# ----------------------------------------------------------------- windows ---
#
# Everything below exists because of one sentence in the contract, and the
# sentence is correct: "a synchronous read against a server that accepts and
# never writes hangs forever".
#
# There is no way around it in the standard library. `open()` on a pipe gives a
# blocking file object. `socket.settimeout` does not apply. The pipe's own
# default timeout is for WaitNamedPipe, i.e. for waiting on a BUSY instance,
# and has nothing to do with how long a read may take. Threads do not help
# either: a thread blocked in a synchronous ReadFile on a pipe cannot be killed
# from Python, so a "timeout" implemented by abandoning a worker thread leaks a
# thread and a handle on every call and still leaves the process unable to
# exit. The only correct answer is FILE_FLAG_OVERLAPPED plus CancelIoEx.

if _IS_WINDOWS:
    import ctypes
    from ctypes import wintypes

    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)

    GENERIC_READ = 0x80000000
    GENERIC_WRITE = 0x40000000
    OPEN_EXISTING = 3
    FILE_FLAG_OVERLAPPED = 0x40000000
    # Rule 8: open with SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION, so a
    # supervisor that calls ImpersonateNamedPipeClient learns who we are and
    # cannot act as us.
    SECURITY_SQOS_PRESENT = 0x00100000
    SECURITY_IDENTIFICATION = 0x00010000

    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
    ERROR_FILE_NOT_FOUND = 2
    ERROR_BROKEN_PIPE = 109
    ERROR_PIPE_BUSY = 231
    ERROR_IO_PENDING = 997
    ERROR_OPERATION_ABORTED = 995
    WAIT_OBJECT_0 = 0

    class _OVERLAPPED(ctypes.Structure):
        _fields_ = [
            ("Internal", ctypes.c_void_p),
            ("InternalHigh", ctypes.c_void_p),
            ("Offset", wintypes.DWORD),
            ("OffsetHigh", wintypes.DWORD),
            ("hEvent", wintypes.HANDLE),
        ]

    _LPOVERLAPPED = ctypes.POINTER(_OVERLAPPED)

    # Declaring these matters: without argtypes ctypes truncates a 64-bit HANDLE
    # to an int, and the failure is a handle that works until it doesn't.
    _k32.CreateFileW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
        ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
    ]
    _k32.CreateFileW.restype = wintypes.HANDLE
    _k32.CreateEventW.argtypes = [
        ctypes.c_void_p, wintypes.BOOL, wintypes.BOOL, wintypes.LPCWSTR,
    ]
    _k32.CreateEventW.restype = wintypes.HANDLE
    _k32.WriteFile.argtypes = [
        wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD), _LPOVERLAPPED,
    ]
    _k32.WriteFile.restype = wintypes.BOOL
    _k32.ReadFile.argtypes = _k32.WriteFile.argtypes
    _k32.ReadFile.restype = wintypes.BOOL
    _k32.GetOverlappedResult.argtypes = [
        wintypes.HANDLE, _LPOVERLAPPED, ctypes.POINTER(wintypes.DWORD), wintypes.BOOL,
    ]
    _k32.GetOverlappedResult.restype = wintypes.BOOL
    _k32.CancelIoEx.argtypes = [wintypes.HANDLE, _LPOVERLAPPED]
    _k32.CancelIoEx.restype = wintypes.BOOL
    _k32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    _k32.WaitForSingleObject.restype = wintypes.DWORD
    _k32.WaitNamedPipeW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD]
    _k32.WaitNamedPipeW.restype = wintypes.BOOL
    _k32.CloseHandle.argtypes = [wintypes.HANDLE]
    _k32.CloseHandle.restype = wintypes.BOOL

    def _open_pipe(path: str, deadline: float):
        """CreateFile on the pipe, with rule 8's one retry on ERROR_PIPE_BUSY.

        Busy is not absent. It means every instance of a pipe that DOES exist is
        currently talking to somebody, and reporting absent there would make a
        supervisor look dead precisely when it is busiest.
        """
        flags = FILE_FLAG_OVERLAPPED | SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION
        for attempt in (0, 1):
            handle = _k32.CreateFileW(
                path, GENERIC_READ | GENERIC_WRITE, 0, None, OPEN_EXISTING, flags, None
            )
            if handle != INVALID_HANDLE_VALUE:
                return handle
            err = ctypes.get_last_error()
            if err != ERROR_PIPE_BUSY or attempt == 1:
                _log.debug("open %s: winerror %d", path, err)
                return None
            left = _remaining_ms(deadline)
            if left <= 0:
                return None
            # Wait inside the remaining budget, never beyond it.
            if not _k32.WaitNamedPipeW(path, left):
                return None
        return None

    def _overlapped_io(fn, handle, buf, nbytes: int, deadline: float) -> Optional[int]:
        """One ReadFile or WriteFile that honours the deadline. None means give up.

        The shape that matters: issue the operation, and if it comes back
        ERROR_IO_PENDING wait on the event for the REMAINING budget only. On
        timeout, CancelIoEx and then wait for the cancellation to actually land
        before the OVERLAPPED and the buffer go out of scope — the kernel is
        still allowed to write into both until the operation completes, and
        letting Python free them first corrupts memory that is no longer ours.
        """
        ov = _OVERLAPPED()
        event = _k32.CreateEventW(None, True, False, None)
        if not event:
            return None
        ov.hEvent = event
        moved = wintypes.DWORD(0)
        try:
            ok = fn(handle, buf, nbytes, ctypes.byref(moved), ctypes.byref(ov))
            if not ok:
                err = ctypes.get_last_error()
                if err != ERROR_IO_PENDING:
                    if err != ERROR_BROKEN_PIPE:
                        _log.debug("io: winerror %d", err)
                    return None
                if _k32.WaitForSingleObject(event, _remaining_ms(deadline)) != WAIT_OBJECT_0:
                    _k32.CancelIoEx(handle, ctypes.byref(ov))
                    # bWait=True: block until the cancelled operation is really
                    # finished with our buffer. This returns quickly.
                    _k32.GetOverlappedResult(
                        handle, ctypes.byref(ov), ctypes.byref(moved), True
                    )
                    _log.debug("io: abandoned at the deadline")
                    return None
                if not _k32.GetOverlappedResult(
                    handle, ctypes.byref(ov), ctypes.byref(moved), False
                ):
                    return None
            return moved.value
        finally:
            _k32.CloseHandle(event)

    def _win_exchange(path: str, deadline: float) -> Optional[bytes]:
        handle = _open_pipe(path, deadline)
        if handle is None:
            return None
        try:
            request = ctypes.create_string_buffer(_REQUEST, len(_REQUEST))
            sent = 0
            while sent < len(_REQUEST):
                if _remaining_ms(deadline) <= 0:
                    return None
                n = _overlapped_io(
                    _k32.WriteFile,
                    handle,
                    ctypes.byref(request, sent),
                    len(_REQUEST) - sent,
                    deadline,
                )
                if not n:
                    return None
                sent += n

            chunks, total = [], 0
            inbuf = ctypes.create_string_buffer(4096)
            while True:
                if _remaining_ms(deadline) <= 0:
                    return None
                n = _overlapped_io(_k32.ReadFile, handle, inbuf, 4096, deadline)
                if n is None:
                    return None
                if n == 0:
                    return None  # EOF before the newline
                chunks.append(inbuf.raw[:n])
                total += n
                line = _line_from(chunks)
                if line is not None:
                    return line
                if total > MAX_LINE:
                    return None
        finally:
            _k32.CloseHandle(handle)

else:  # pragma: no cover - the branch not taken on this platform

    def _win_exchange(path: str, deadline: float) -> Optional[bytes]:
        raise NotImplementedError
