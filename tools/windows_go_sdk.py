"""Prepare the pinned Windows Go SDK with the reviewed pipe-offset backport."""
import argparse
import csv
import ctypes
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile

VERSION = "go1.26.5"
ARCHIVE_SHA256 = "97e6b2a833b6d89f9ff17d25419ac0a7e3b482a044e9ab18cdef834bd834fd38"
ARCHIVE_URL = "https://go.dev/dl/go1.26.5.windows-amd64.zip"
PATCH_SHA256 = "4d3e2e47965a672b6b5f5db49042b90b6fcedb6bd370b068809f71204f4f3904"
UPSTREAM = "f4ac29c3c743b30a1e1a2b7ef4eae2588ac5c6f3"
POLL = "src/internal/poll/fd_windows.go"
POLL_BEFORE = "6bfd788cee539d0b34786baf1e01f79bb610be7e5a8bbf157ccc91b64c4eee77"
POLL_AFTER = "e882639d4f935a8e1f07ba08ef387f6f2388e7aeb31dea3518f8e04b34d1bdeb"
REGRESSION_SHA256 = "21cdd87ebcdda644d2b425d8b336ee93f29071292fa1e17e7546af73142c3cb0"
HERE = Path(__file__).resolve().parent


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _unlinked(path):
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("SDK path must be absolute and contain no parent traversal")
    for entry in (path, *path.parents):
        if entry.is_symlink() or (entry.exists() and getattr(entry.lstat(), "st_file_attributes", 0) & 0x400):
            raise ValueError("SDK path is linked: " + str(entry))


def _private_root(path, create=False):
    _unlinked(path)
    new = not path.exists()
    if create:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not path.is_dir():
        raise ValueError("SDK cache root is not a directory")
    if os.name != "nt":
        if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077:
            raise ValueError("SDK cache root must be private to its owner")
    else:
        sid = list(csv.reader(io.StringIO(subprocess.run(["whoami", "/user", "/fo", "csv", "/nh"],
                    capture_output=True, text=True, check=True).stdout)))[0][1]
        if create and new:
            # Elevated Windows jobs can default a new directory's owner to Administrators.
            subprocess.run(["icacls", str(path), "/setowner", "*" + sid], capture_output=True, check=True)
            subprocess.run(["icacls", str(path), "/inheritance:r", "/grant:r", "*" + sid + ":(OI)(CI)F"],
                           capture_output=True, check=True)
        security = ctypes.WinDLL("advapi32", use_last_error=True)
        owner, dacl, descriptor = ctypes.c_void_p(), ctypes.c_void_p(), ctypes.c_void_p()
        security.GetNamedSecurityInfoW.argtypes = [ctypes.c_wchar_p, ctypes.c_int, ctypes.c_ulong,
            ctypes.POINTER(ctypes.c_void_p), ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        security.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        security.GetAce.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.POINTER(ctypes.c_void_p)]
        kernel = ctypes.WinDLL("kernel32", use_last_error=True); kernel.LocalFree.argtypes = [ctypes.c_void_p]
        def stringify(value):
            result = ctypes.c_void_p()
            if not security.ConvertSidToStringSidW(value, ctypes.byref(result)):
                raise ValueError("SDK directory SID unavailable")
            try: return ctypes.wstring_at(result)
            finally: kernel.LocalFree(result)
        if security.GetNamedSecurityInfoW(str(path), 1, 5, ctypes.byref(owner), None,
                                         ctypes.byref(dacl), None, ctypes.byref(descriptor)):
            raise ValueError("SDK directory ownership unavailable")
        try:
            if not owner.value or stringify(owner) != sid or not dacl.value:
                raise ValueError("SDK cache root must belong to its current owner")
            count = ctypes.c_ushort.from_address(dacl.value + 4).value
            for index in range(count):
                ace = ctypes.c_void_p()
                if not security.GetAce(dacl, index, ctypes.byref(ace)):
                    raise ValueError("SDK directory ACL unavailable")
                kind = ctypes.c_ubyte.from_address(ace.value).value
                if kind == 0 and stringify(ctypes.c_void_p(ace.value + 8)) not in (sid, "S-1-5-18", "S-1-5-32-544", "S-1-3-4"):
                    raise ValueError("SDK cache root grants another principal access")
                if kind not in (0, 1):
                    raise ValueError("SDK cache root has an unsupported access grant")
        finally:
            kernel.LocalFree(descriptor)


def _patch_poll(raw):
    """Apply the exact reviewed unified diff without relying on a Git installation."""
    # Split outputs may be checked out with CRLF; the reviewed text recipe is LF.
    patch = (HERE / "windows_go_sdk.patch").read_bytes().replace(b"\r\n", b"\n")
    if _hash(patch) != PATCH_SHA256 or _hash(raw) != POLL_BEFORE:
        raise ValueError("SDK patch or base poll source differs from the pinned recipe")
    lines = raw.decode().splitlines(keepends=True)
    result, cursor = [], 0
    chunks = patch.decode().splitlines(keepends=True)
    index = 2
    while index < len(chunks):
        header = chunks[index]
        if not header.startswith("@@ "):
            raise ValueError("invalid pinned patch hunk")
        first = int(header.split()[1].split(",")[0][1:]) - 1
        result.extend(lines[cursor:first]); cursor = first; index += 1
        while index < len(chunks) and not chunks[index].startswith("@@ "):
            line = chunks[index]; index += 1
            if line[0] in " -":
                if cursor >= len(lines) or lines[cursor] != line[1:]:
                    raise ValueError("SDK patch context differs")
                cursor += 1
            if line[0] in " +":
                result.append(line[1:])
    result.extend(lines[cursor:])
    fixed = "".join(result).encode()
    if _hash(fixed) != POLL_AFTER:
        raise ValueError("SDK patched poll source differs")
    return fixed


def _archive_files(archive):
    _unlinked(archive)
    raw_archive = archive.read_bytes()
    if _hash(raw_archive) != ARCHIVE_SHA256:
        raise ValueError("official SDK archive checksum differs")
    files = {}
    with zipfile.ZipFile(io.BytesIO(raw_archive)) as z:
        for item in z.infolist():
            name = PurePosixPath(item.filename)
            if name.is_absolute() or ".." in name.parts or not name.parts or name.parts[0] != "go" or "\\" in item.filename:
                raise ValueError("SDK archive member escapes its root")
            if stat.S_ISLNK(item.external_attr >> 16):
                raise ValueError("SDK archive contains a link")
            if item.is_dir():
                continue
            relative = name.relative_to("go").as_posix()
            if relative in files:
                raise ValueError("SDK archive contains duplicate members")
            files[relative] = z.read(item)
    if POLL not in files or "LICENSE" not in files or "bin/go.exe" not in files:
        raise ValueError("SDK archive lacks its required closure")
    files[POLL] = _patch_poll(files[POLL])
    return files


def _identity(files):
    rows = [{"path": name, "sha256": _hash(raw)} for name, raw in sorted(files.items())]
    source = [row for row in rows if row["path"].startswith("src/")]
    value = {"recipe": "windows-pipe-offset-backport-v1", "version": VERSION,
             "platform": "windows/amd64", "archive_sha256": ARCHIVE_SHA256,
             "semantic_patch_sha256": PATCH_SHA256, "upstream_commit": UPSTREAM,
             "stdlib_sha256": _hash(_canonical(source)), "sdk_sha256": _hash(_canonical(rows)),
             "go_exe_sha256": _hash(files["bin/go.exe"]), "license_sha256": _hash(files["LICENSE"]),
             "production_patch": POLL, "regression_fixture": "separate upstream test; excluded from product SDK"}
    value["digest"] = _hash(_canonical(value))
    return value


def validate_sdk_receipt(receipt):
    """Validate actual SDK source and compiler bytes against the pinned archive and patch."""
    if not isinstance(receipt, dict) or set(receipt) != {"schema", "go", "goroot", "receipt_path", "source_identity"} or receipt["schema"] != 1:
        raise ValueError("invalid SDK receipt schema")
    sdk = Path(receipt["goroot"]); parent = sdk.parent
    _private_root(parent.parent); _unlinked(parent); _unlinked(sdk)
    if parent.name != VERSION + "-" + PATCH_SHA256[:12] or sdk.name != "go" or Path(receipt["go"]) != sdk / "bin/go.exe" or Path(receipt["receipt_path"]) != parent / "receipt.json":
        raise ValueError("SDK receipt is outside its pinned namespace")
    files = _archive_files(parent / "archive.zip")
    identity = _identity(files)
    if receipt["source_identity"] != identity:
        raise ValueError("SDK receipt source identity differs from the pinned recipe")
    observed = set()
    for directory, folders, names in os.walk(sdk, followlinks=False):
        for entry in [*folders, *names]:
            path = Path(directory) / entry
            if path.is_symlink() or getattr(path.lstat(), "st_file_attributes", 0) & 0x400:
                raise ValueError("SDK source/compiler path is linked")
        for entry in names:
            path = Path(directory) / entry
            name = path.relative_to(sdk).as_posix(); observed.add(name)
            if name not in files or _hash(path.read_bytes()) != _hash(files[name]):
                raise ValueError("SDK source/compiler bytes changed: " + name)
    if observed != set(files):
        raise ValueError("SDK source/compiler closure is incomplete")
    return receipt


def prepare_sdk(cache_dir, archive_path=None):
    """Prepare or validate the owner-scoped official SDK; never modify installed Go."""
    cache = Path(cache_dir); _private_root(cache, create=True)
    parent = cache / (VERSION + "-" + PATCH_SHA256[:12]); _unlinked(parent)
    if parent.exists():
        _unlinked(parent / "receipt.json")
        receipt = json.loads((parent / "receipt.json").read_bytes())
        return validate_sdk_receipt(receipt)
    temporary = Path(tempfile.mkdtemp(prefix="sdk-preparing-", dir=cache)); archive = temporary / "archive.zip"
    try:
        if archive_path is None:
            urllib.request.urlretrieve(ARCHIVE_URL, archive)
        else:
            selected = Path(archive_path); _unlinked(selected); shutil.copyfile(selected, archive)
        files = _archive_files(archive)
        sdk = temporary / "go"
        for name, raw in files.items():
            destination = sdk / name; destination.parent.mkdir(parents=True, exist_ok=True); destination.write_bytes(raw)
        receipt = {"schema": 1, "go": str(parent / "go/bin/go.exe"), "goroot": str(parent / "go"),
                   "receipt_path": str(parent / "receipt.json"), "source_identity": _identity(files)}
        (temporary / "receipt.json").write_bytes(json.dumps(receipt, sort_keys=True, indent=2).encode() + b"\n")
        try:
            temporary.rename(parent)
        except FileExistsError:
            return prepare_sdk(cache, archive_path)
        return validate_sdk_receipt(receipt)
    finally:
        if temporary.exists():
            _unlinked(temporary)
            if temporary.parent != cache or not temporary.name.startswith("sdk-preparing-"):
                raise ValueError("unexpected SDK preparation cleanup path")
            shutil.rmtree(temporary)


def sdk_environment(receipt, base_env):
    """Format an already verified receipt; owners validate each frozen phase boundary."""
    if not isinstance(receipt, dict) or receipt.get("schema") != 1 or \
            receipt.get("source_identity", {}).get("archive_sha256") != ARCHIVE_SHA256 or \
            receipt.get("source_identity", {}).get("semantic_patch_sha256") != PATCH_SHA256:
        raise ValueError("environment needs a verified pinned SDK receipt")
    env = dict(base_env)
    env.update(GOROOT=receipt["goroot"], GOTOOLCHAIN="local", GOENV="off",
               OA_WINDOWS_GO_SDK_RECEIPT=receipt["receipt_path"])
    env["PATH"] = str(Path(receipt["go"]).parent) + os.pathsep + env.get("PATH", "")
    return env


def smoke_sdk(receipt, work_dir, base_env):
    """Run the upstream named-pipe duplex regression with a private test overlay."""
    work = Path(work_dir); _private_root(work, create=True)
    receipt = validate_sdk_receipt(receipt)
    env = sdk_environment(receipt, base_env)
    fixture = (HERE / "windows_go_sdk_regression.go.txt").read_bytes().replace(b"\r\n", b"\n")
    if _hash(fixture) != REGRESSION_SHA256:
        raise ValueError("upstream named-pipe regression fixture differs")
    target = Path(receipt["goroot"]) / "src/os/os_windows_test.go"
    replacement = work / "os_windows_test.go"
    replacement.write_bytes(target.read_bytes() + b"\n" + fixture)
    overlay = work / "overlay.json"
    overlay.write_bytes(json.dumps({"Replace": {str(target): str(replacement)}}).encode())
    cache = Path(env.get("GOCACHE") or work / "gocache")
    _private_root(cache, create=True)
    env.update(GOCACHE=str(cache), GOMODCACHE=str(work / "gomod"), GOPROXY="off", GOSUMDB="off", CGO_ENABLED="1")
    argv = [receipt["go"], "test", "-overlay=" + str(overlay), "-race", "-count=1", "-timeout=60s",
            "-run=^TestNamedPipeConcurrentReadWrite$", "os"]
    start = time.monotonic()
    result = subprocess.run(argv, cwd=work, env=env, capture_output=True, timeout=240)
    validate_sdk_receipt(receipt)
    log = work / "named-pipe-race.log"; log.write_bytes(result.stdout + result.stderr)
    record = {"exit": result.returncode, "argv": argv, "seconds": time.monotonic() - start,
              "log": str(log), "log_sha256": _hash(log.read_bytes()), "source_identity": receipt["source_identity"],
              "regression_sha256": REGRESSION_SHA256}
    (work / "smoke-receipt.json").write_bytes(json.dumps(record, sort_keys=True, indent=2).encode() + b"\n")
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--github-env", action="store_true", help="write selected SDK to this job's GITHUB_ENV/GITHUB_PATH")
    parser.add_argument("--smoke-dir", type=Path, help="run the upstream duplex race regression in this owned directory")
    args = parser.parse_args(argv)
    if args.cache_dir is None:
        parser.print_help(); return 0
    explicit = os.environ.get("OA_WINDOWS_GO_SDK_RECEIPT")
    if explicit:
        path = Path(explicit); _unlinked(path)
        receipt = validate_sdk_receipt(json.loads(path.read_bytes()))
    else:
        receipt = prepare_sdk(args.cache_dir, args.archive)
    if args.github_env:
        env = sdk_environment(receipt, os.environ)
        paths = [Path(os.environ[key]) for key in ("GITHUB_ENV", "GITHUB_PATH")]
        for path in paths:
            _unlinked(path)
            if not path.is_file():
                raise ValueError("GitHub job environment output file is missing")
        values = {key: env[key] for key in ("GOROOT", "GOTOOLCHAIN", "GOENV", "OA_WINDOWS_GO_SDK_RECEIPT")}
        if any("\n" in value or "\r" in value for value in [*values.values(), receipt["go"]]):
            raise ValueError("SDK path contains a newline")
        with paths[0].open("ab") as stream:
            stream.write("".join(key + "=" + value + "\n" for key, value in values.items()).encode())
        with paths[1].open("ab") as stream:
            stream.write((str(Path(receipt["go"]).parent) + "\n").encode())
    if args.smoke_dir:
        smoke = smoke_sdk(receipt, args.smoke_dir, os.environ)
        print(json.dumps({"receipt": receipt, "smoke": smoke}, sort_keys=True, indent=2)); return smoke["exit"]
    print(json.dumps(receipt, sort_keys=True, indent=2)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
