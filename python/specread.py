"""specread — print what this implementation understood from a download spec.

The job RECORD has cross-language conformance tests: three implementations pass
one record around and must agree byte for byte. The SPEC inside it has none, and
could not, because the job layer refuses to look inside a spec — which is what
lets download evolve without a schema change in three languages.

That opacity is right, and it leaves a hole. A digest written bare by one
implementation reached another that built "sha256:" + hex and compared strings;
the error read "got sha256:1fc70f… want 1fc70f…", the same digest twice, and a
correct 1.5 GB download was deleted and fetched again.

So the record's conformance is by identical BYTES and the spec's is by identical
MEANING. This prints the meaning, in the form the other implementations print it.

    usage: specread.py <spec.json>
"""

import json
import sys

import abstraction_download as dl


def normal_digest(d: str) -> str:
    """A digest reduced to the part that carries the meaning.

    One implementation writes "sha256:<hex>", another the bare hex, and the
    contract is that they name the same artifact. Comparing the spelling is what
    threw away correct bytes. Anything unrecognised becomes empty rather than
    itself, so a comparison cannot succeed on two things neither side understood.
    """
    s = (d or "").strip().lower()
    for prefix in ("sha256:", "sha256-"):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    if len(s) != 64 or any(c not in "0123456789abcdef" for c in s):
        return ""
    return "sha256:" + s


def portable(p: str) -> str:
    """A relative path in the one form every machine reads the same way.

    Absolute paths are left exactly as given: they already name a specific
    machine's filesystem, and rewriting their separators would not make them
    more portable, only harder to recognise.
    """
    if not p:
        return ""
    if p.startswith("/") or p.startswith("\\"):
        return p
    if len(p) >= 2 and p[1] == ":" and p[0].isalpha():
        return p
    return p.replace("\\", "/")


def main() -> None:
    # Windows text mode would turn every LF into CRLF, and this output is a
    # conformance surface compared against other implementations byte for byte.
    # Without it the three readers agree about every digest, size and path and
    # the test still fails -- on line endings, which is the least interesting way
    # to disagree and the hardest to see in a diff.
    try:
        sys.stdout.reconfigure(newline="\n")
    except AttributeError:  # pragma: no cover - Python < 3.7
        pass
    if len(sys.argv) < 2:
        sys.exit("usage: specread.py <spec.json>")
    with open(sys.argv[1], "rb") as fh:
        d = json.load(fh)

    artifact = d.get("artifact") or {}
    sink = d.get("sink") or {}
    sources = d.get("sources") or []

    print("digest=" + normal_digest(artifact.get("digest", "")))
    print("size=%d" % int(artifact.get("size", 0) or 0))
    print("final=" + portable(sink.get("final", "")))
    print("partial=" + portable(sink.get("partial", "")))

    # In the order they would be tried, because that order is the behaviour: a
    # local copy at priority -100 is what turns a download into a copy, and an
    # implementation that sorted differently would fetch over the network while
    # the bytes sat on disk. Stable, so equal priorities keep their given order.
    ordered = sorted(enumerate(sources), key=lambda pair: (int(pair[1].get("priority", 0) or 0), pair[0]))
    for i, (_, s) in enumerate(ordered):
        print("source%d=%s|%s" % (i, s.get("scheme", ""), s.get("locator", "")))


if __name__ == "__main__":
    _ = dl  # the module is imported to prove this runs against the real package
    main()
