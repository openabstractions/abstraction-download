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


def pairs(m) -> str:
    """A string map in the one spelling every implementation prints."""
    return ",".join("%s=%s" % (k, (m or {})[k]) for k in sorted(m or {}))


def main() -> None:
    # Windows text mode would turn every LF into CRLF, and this output is a
    # conformance surface compared against other implementations byte for byte.
    # Without it the three readers agree about every digest, size and path and
    # the test still fails -- on line endings, which is the least interesting way
    # to disagree and the hardest to see in a diff.
    # And the encoding for the same reason. Windows picks it from the console
    # code page, so a spec naming `models/café.gguf` printed mojibake on one
    # machine, printed correctly on another, and refused a spec naming an emoji
    # outright -- three verdicts from one implementation depending on where it
    # ran. The other two readers write UTF-8 whatever the host thinks.
    try:
        sys.stdout.reconfigure(newline="\n", encoding="utf-8")
    except AttributeError:  # pragma: no cover - Python < 3.7
        pass
    if len(sys.argv) < 2:
        sys.exit(
            "usage: specread.py <spec.json>\n"
            "       specread.py --echo <spec.json>\n"
            "       specread.py --partial <final> <id>\n"
            "       specread.py --portable <path>\n"
            "       specread.py --reserved <owner-id> <path>\n"
            "       specread.py --foreign <path>"
        )

    # The spelling a path gets when it is written into a record. Every other
    # check here reads a path back; this one is the only view of what the layer
    # WROTE, and the disagreement it pins was visible in a window before it was
    # visible to any of them: two finished jobs whose destinations were spelled
    # differently, one of them changing convention halfway along.
    if sys.argv[1] == "--portable":
        if len(sys.argv) < 3:
            sys.exit("usage: specread.py --portable <path>")
        print("portable=" + dl.portable(sys.argv[2]))
        return

    # The spec as this implementation would carry it in a record. Go holds it as
    # raw bytes and Python and C++ hold it parsed, so a spec's bytes may change
    # on the way through one implementation and not another -- a number
    # respelled, an escape policy applied -- and every reader downstream sees the
    # changed ones. Compared compact because whitespace is the record writer's
    # choice; escapes and number spellings survive compaction and are not.
    if sys.argv[1] == "--echo":
        if len(sys.argv) < 3:
            sys.exit("usage: specread.py --echo <spec.json>")
        with open(sys.argv[2], "rb") as fh:
            print("echo=" + json.dumps(json.load(fh), separators=(",", ":")))
        return

    # The partial name a caller who chose none would get. It is not read out of a
    # spec -- it is INVENTED, by whichever implementation happens to submit --
    # and it then lands in the record for the others to resume from. Two
    # implementations inventing it independently is the same shape of bug as the
    # digest that cost a 1.5 GB download, so it is compared here too.
    if sys.argv[1] == "--partial":
        if len(sys.argv) < 4:
            sys.exit("usage: specread.py --partial <final> <id>")
        print("partial=" + dl.partial_for(dl.portable(sys.argv[2]), sys.argv[3]))
        return

    # Whether a sink names the store's own layout. Contained paths, all of them,
    # and every one able to overwrite a job record or another job's partial --
    # so the three implementations have to refuse exactly the same set, and the
    # set is not spellable in a fixture because it depends on which job asks.
    if sys.argv[1] == "--reserved":
        if len(sys.argv) < 4:
            sys.exit("usage: specread.py --reserved <owner-id> <path>")
        print("reserved=" + dl.reserved_sink(sys.argv[2], sys.argv[3]))
        return

    # Whether this machine may write an absolute sink at all. The only answer
    # here that depends on the host, and it must still be the same answer from
    # all three implementations ON that host.
    if sys.argv[1] == "--foreign":
        if len(sys.argv) < 3:
            sys.exit("usage: specread.py --foreign <path>")
        print("foreign=" + dl.foreign_path(sys.argv[2]))
        return

    with open(sys.argv[1], "rb") as fh:
        d = json.load(fh)

    artifact = d.get("artifact") or {}
    sink = d.get("sink") or {}
    sources = d.get("sources") or []

    print("digest=" + dl.normal_digest(artifact.get("digest", "")))
    print("size=%d" % int(artifact.get("size", 0) or 0))
    print("final=" + dl.portable(sink.get("final", "")))
    print("partial=" + dl.portable(sink.get("partial", "")))

    # Whether each sink path stays under the store root, and in the same words
    # everywhere. A relative path that climbs out of the root is refused by the
    # machine that would do the writing, and the three implementations have to
    # refuse the same records for the same stated reason -- otherwise a record
    # one of them will not touch is quietly acted on by another. Empty means
    # nothing to refuse, the same way an unreadable digest reads as empty.
    #
    # Asked of the package rather than reimplemented here: a second copy of a
    # containment check in the same language is a second thing to get wrong.
    print("final_refusal=" + dl.escapes_root(sink.get("final", "")))
    print("partial_refusal=" + dl.escapes_root(sink.get("partial", "")))

    # In the order they would be tried, because that order is the behaviour: a
    # local copy at priority -100 is what turns a download into a copy, and an
    # implementation that sorted differently would fetch over the network while
    # the bytes sat on disk. Stable, so equal priorities keep their given order.
    ordered = sorted(enumerate(sources), key=lambda pair: (int(pair[1].get("priority", 0) or 0), pair[0]))
    # Which keys describe the source and which are sent to the server. One bag
    # used to be both in the Go implementation, so an attribute nobody
    # remembered to exclude went out as a header -- and no unit test in one
    # language could see that another classified the same key differently. This
    # is the line that makes the split a contract rather than a habit.
    for i, (_, s) in enumerate(ordered):
        print("source%d=%s|%s" % (i, s.get("scheme", ""), s.get("locator", "")))
        print("source%d.attrs=%s" % (i, pairs(s.get("attrs"))))
        print("source%d.headers=%s" % (i, pairs(s.get("headers"))))


if __name__ == "__main__":
    _ = dl  # the module is imported to prove this runs against the real package
    main()
