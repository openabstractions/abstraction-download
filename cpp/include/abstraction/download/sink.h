// Where a download's bytes may land, and where they may not.
//
// A record is written by one machine and acted on by another, so an
// implementation that merely READS one has to say the same "no" about it as the
// implementation that would run it. A record that Go refuses and C++ reports as
// fine is worse than either answer on its own, because it means the refusal
// depends on who looked.
//
// Header-only on purpose: it is a few string rules, and an adopter should be
// able to take it without taking a build system, a runner or a JSON parser with
// it. The one header it includes is the job layer's layout, which is
// header-only for the same reason — the store's directory names belong to the
// store, and this side asks rather than spelling them.

#ifndef ABSTRACTION_DOWNLOAD_SINK_H
#define ABSTRACTION_DOWNLOAD_SINK_H

#include <abstraction/job/layout.h>

#include <cctype>
#include <string>
#include <vector>

namespace abstraction {
namespace download {

// Is p relative under BOTH conventions?
//
// Asking the host OS alone answers for the machine running it: on Linux
// "D:\models\x.gguf" is "relative", and joining it onto the store root would
// silently produce a directory literally named "D:\models" on the NAS. A path
// absolute anywhere is treated as absolute everywhere, so a mistake surfaces as
// a plain "no such file" rather than as a strange one.
inline bool relative_everywhere(const std::string& p) {
    if (p.empty()) {
        return false;
    }
    if (p[0] == '/' || p[0] == '\\') {
        return false;  // POSIX absolute, or a UNC path
    }
    if (p.size() >= 2 && p[1] == ':' && std::isalpha(static_cast<unsigned char>(p[0]))) {
        return false;  // Windows drive letter
    }
    return true;
}

// A path in the one spelling a record uses: "/" is the only separator,
// everywhere, whatever wrote it.
//
// Absolute paths used to be exempt, on the argument that they already name one
// machine and respelling them buys nothing. What that missed is that the
// separator then records WHICH machine wrote the record -- a job delegated to a
// NAS came back spelled "C:/Users/..." and the same file fetched locally was
// "C:\Users\..." -- and that two spellings of one destination do not compare
// equal, so "are we already fetching this?" answers no and the artifact is
// fetched twice. An adopter joining a native directory to a file name with a
// hardcoded "/" produced a path that changed convention halfway through, and
// nothing refused it.
//
// Nothing is lost by the rewrite: a drive letter and a UNC root still say
// Windows afterwards -- windows_shaped reads "//server/share" as UNC for exactly
// this reason -- and Windows accepts either separator in every path it is given.
//
// A POSIX-rooted path is returned untouched, because there a backslash is a
// legal character in a file's name and rewriting it would name a different file.
inline std::string portable(const std::string& p) {
    if (!p.empty() && p[0] == '/') {
        return p;
    }
    std::string out = p;
    for (char& c : out) {
        if (c == '\\') {
            c = '/';
        }
    }
    return out;
}

// Where the bytes accumulate before they earn the final name, for a caller that
// did not choose somewhere. Here rather than in the runner for the same reason
// the refusals are: the name goes into the record, so a reader that never
// fetches anything still has to know what the implementation that WROTE it
// would have chosen.
//
// A relative final resolves under the store root on whichever machine picks the
// job up, so the partial goes in the store's own work directory. An absolute one
// names a filesystem already, possibly a different volume from the store, where
// delivery could not be a rename — so it sits beside the artifact and the final
// name does not exist until the bytes are all there.
//
// An empty final names no volume at all, so it answers like a relative one. The
// submitting implementations refuse it long before a record is written; a rule
// with an unspecified corner is one three implementations fill in differently.
inline std::string partial_for(const std::string& final_path, const std::string& id) {
    if (final_path.empty() || relative_everywhere(final_path)) {
        return "work/" + id;
    }
    return final_path + ".part";
}

// A path reduced to the one spelling in which two of them can be compared:
// cleaned of "." and "..", forward slashes, no trailing separator, and
// case-folded only where the filesystem itself ignores case.
//
// Written out rather than handed to std::filesystem, because the answer must
// match what Go's filepath.Clean and Python's os.path.normpath produce for the
// same input on any host — a cleaner that disagrees about one edge is a record
// two implementations read differently, which is the whole thing being
// prevented.
inline std::string comparable_path(const std::string& p) {
    if (p.empty()) {
        return "";
    }
    std::string s;
    s.reserve(p.size());
    for (char c : p) {
        s.push_back(c == '\\' ? '/' : c);
    }

    // The part no ".." may climb above, kept verbatim.
    std::string prefix;
    std::size_t i = 0;
    bool rooted = false;
    if (s.size() >= 2 && s[0] == '/' && s[1] == '/') {
        prefix = "//";  // a UNC path: \\server\share
        i = 2;
        rooted = true;
    } else if (s.size() >= 2 && s[1] == ':' && std::isalpha(static_cast<unsigned char>(s[0]))) {
        prefix = s.substr(0, 2);
        i = 2;
        if (i < s.size() && s[i] == '/') {
            prefix += '/';
            ++i;
            rooted = true;
        }
    } else if (s[0] == '/') {
        prefix = "/";
        i = 1;
        rooted = true;
    }

    std::vector<std::string> out;
    std::string seg;
    const auto take = [&]() {
        if (seg.empty() || seg == ".") {
            // nothing to keep
        } else if (seg == "..") {
            if (!out.empty() && out.back() != "..") {
                out.pop_back();
            } else if (!rooted) {
                // A relative path may keep climbing; there is nothing above an
                // absolute one, so the climb is simply dropped.
                out.push_back(seg);
            }
        } else {
            out.push_back(seg);
        }
        seg.clear();
    };
    for (; i < s.size(); ++i) {
        if (s[i] == '/') {
            take();
        } else {
            seg.push_back(s[i]);
        }
    }
    take();

    // No separator is inserted after the prefix: every prefix that needs one
    // ("/", "//", "C:/") already carries it, and "C:foo" — drive-relative —
    // must not gain one it never had.
    std::string joined = prefix;
    for (std::size_t k = 0; k < out.size(); ++k) {
        if (k > 0) {
            joined += '/';
        }
        joined += out[k];
    }
    if (joined.empty()) {
        joined = ".";
    }
    // A bare root keeps its separator; nothing else does.
    while (joined.size() > 1 && joined.back() == '/' && joined != "//") {
        joined.pop_back();
    }
#ifdef _WIN32
    for (char& c : joined) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
#endif
    return joined;
}

// Does resolved name a location inside root?
//
// Asked of the RESULT of the join, never of the input. Scanning the input for
// ".." is the check everybody writes and it is defeated by a path that spells
// the climb some other way, and it fires on paths like "a/../b" that are
// perfectly contained. Resolve first, then ask where the answer landed.
//
// Every shortcut in the comparison itself is wrong somewhere, so none is taken:
//
//   - "C:\store2" starts with "C:\store" and is a different directory, so the
//     prefix has to end on a separator boundary.
//   - Windows ignores case in a path and POSIX does not, so folding is
//     conditional on the OS rather than done to be safe.
//   - "\\nas\share\store", "C:\", "/" and a trailing separator must not change
//     the answer, so both sides are reduced to one spelling first.
//
// What this does NOT close: a directory inside the root that is itself a
// symlink or a junction pointing out of it. "models/x.gguf" is then contained
// by every lexical measure and the bytes still land elsewhere. Closing it needs
// the path resolved at the moment of the write — the file does not exist yet
// when this runs, and resolving it here would only move the race earlier — and
// none of Go, Python and C++ has a portable "open without following a link".
// This is lexical containment and claims nothing more.
inline bool under(const std::string& root, const std::string& resolved) {
    std::string r = comparable_path(root);
    const std::string c = comparable_path(resolved);
    if (r.empty() || r == ".") {
        // The store's binding is not a filesystem, so there is no root to be
        // inside. All that can be said is that the path did not climb out of
        // wherever it eventually gets resolved, which the cleaning has already
        // made visible.
        return c != ".." && c.rfind("../", 0) != 0;
    }
    if (c == r) {
        return true;
    }
    if (r.back() != '/') {
        r += '/';
    }
    return c.rfind(r, 0) == 0;
}

// kProbeRoot is a stand-in store root, used to answer the containment question
// about a path that has no root to hand — a record being read rather than run.
// One segment deep, because containment is measured from the store root and a
// deeper stand-in would absorb a ".." that a real root would not.
inline const char* probe_root() { return "/probe"; }

// The refusal for a relative sink path that would resolve outside the store
// root — whichever root it is resolved against — or "" if there is none.
//
// Root-independent, and that is a fact about the question rather than a
// shortcut: containment is measured from the store root, so one ".." climbs out
// of it no matter how deep the root sits on any particular machine. That is what
// lets a reader answer this about a RECORD, which deliberately names no root.
//
// Absolute paths answer "". They are never joined onto the root, so they cannot
// escape it in this sense; what a machine adopting a record should do with one
// is a separate question and is not decided here.
//
// The wording is the contract, not decoration: Go's ErrEscapesRoot and Python's
// EscapesRoot spell it exactly this way, and scripts/spec-conformance.sh
// compares the three byte for byte.
inline std::string escapes_root(const std::string& p) {
    if (!relative_everywhere(p)) {
        return "";
    }
    if (under(probe_root(), std::string(probe_root()) + "/" + p)) {
        return "";
    }
    return "download: sink path escapes the store root: " + p;
}

// What this layer keeps in the store root, beside the store's own jobs/, work/
// and services.json: the supervisor heartbeat, its temporary, and the nudge
// socket. Written by the Go and Python supervisors; named here because a reader
// has to refuse the same sinks a runner would.
inline bool beside_the_store(const std::string& name) {
    return name == "supervisor.json" || name == "supervisor.json.tmp" ||
           name == "supervisor.sock";
}

// The refusal for a sink that names the store's own layout, or this layer's own
// files beside it — or "" if there is none.
//
// Containment stopped a sink climbing OUT of the root and never stopped one
// naming what is IN it: a final of "jobs/<id>.json" overwrites a job record,
// and a final of "work/<other>" overwrites another job's partial. Both are
// contained, and both were accepted by every check that existed.
//
// owner is the id of the job the sink belongs to. Its own scratch is not
// reserved against it — that is where its partial goes — and is reserved
// against every other job. An empty owner reserves the whole work area, which
// is the right answer for a reader that has no id to hand.
//
// Absolute paths answer "": they are never joined onto the root, so they name
// the store's contents only by a coincidence no rule here could see. What a
// machine should do with an absolute sink is foreign_path's question.
//
// The wording is the contract, not decoration: Go's ErrReservedPath and
// Python's ReservedPath spell it exactly this way, and
// scripts/spec-conformance.sh compares the three byte for byte.
inline std::string reserved_sink(const std::string& owner, const std::string& p) {
    if (!relative_everywhere(p)) {
        return "";
    }
    if (abstraction::job::reserved(owner, p) ||
        beside_the_store(abstraction::job::root_name(p))) {
        return "download: sink path is reserved by the store: " + p;
    }
    return "";
}

// Is an absolute path spelled the way Windows spells one — a drive letter, or
// the two leading separators of a UNC path? Anything else that is absolute is
// rooted at a single "/", which is POSIX's spelling.
//
// "//server/share" counts because portable writes a UNC root that way, and a
// record whose UNC sink stopped being recognised as Windows would be refused on
// the only host that can write it. POSIX reaches that spelling only for a path
// whose leading "//" POSIX itself leaves implementation-defined; refusing that
// one on Linux is the safe direction, and comparable_path has always read
// "\\server\share" as "//server/share" anyway.
inline bool windows_shaped(const std::string& p) {
    if (p.empty()) {
        return false;
    }
    if (p[0] == '\\' || p.rfind("//", 0) == 0) {
        return true;
    }
    return p.size() >= 2 && p[1] == ':' && std::isalpha(static_cast<unsigned char>(p[0]));
}

// The refusal for an absolute sink written in a convention this machine does
// not use — or "" if there is none.
//
// The contract said an absolute path is left alone, so a Windows path handed to
// Linux "fails with no such file rather than quietly creating a directory".
// That is true of a path being READ and false of a sink: opening
// "D:\models\x.gguf" with O_CREAT on Linux succeeds and makes a file of that
// literal name in the working directory, with a ".part" beside it. The runners
// create the parent first, so the mirror case is no better.
//
// This is the one answer here that depends on the host, so it is asked by the
// machine about to do the writing and never at submission: a record carrying
// such a path is valid on the machine that wrote it. A record meant to be
// adopted elsewhere uses a RELATIVE sink.
inline std::string foreign_path(const std::string& p) {
#ifdef _WIN32
    const bool here_is_windows = true;
#else
    const bool here_is_windows = false;
#endif
    if (p.empty() || relative_everywhere(p) || windows_shaped(p) == here_is_windows) {
        return "";
    }
    return "download: sink path names another platform's filesystem: " + p;
}

// The refusal for an absolute sink on a store several machines can write — or
// "" if there is none.
//
// Containment (escapes_root) refuses a RELATIVE sink that climbs out of the
// store root. It never saw an absolute one: an absolute path is never joined
// onto the root, so it climbs out of nothing. foreign_path refuses only the
// OTHER platform's absolute spelling, so a path in THIS machine's own
// convention — /etc/cron.d/evil on a NAS — passes every lexical check and is
// written with this machine's authority, at a destination a record another
// machine wrote chose. The same confused deputy, one spelling past the one that
// was closed.
//
// A relative sink is the portable, contained form; an absolute sink names a
// filesystem directly and is legitimate only for a caller writing to its own
// machine — `dl -o`, never an adopted record. So a supervisor over a shared
// store refuses it. This is a machine's policy about the store, checked by the
// machine about to write, not a judgement about the record, which is valid where
// it was made — the same division foreign_path draws.
inline std::string unportable_sink(const std::string& p) {
    if (p.empty() || relative_everywhere(p)) {
        return "";
    }
    return "download: a shared store's record may only name a relative sink: " + p;
}

}  // namespace download
}  // namespace abstraction

#endif  // ABSTRACTION_DOWNLOAD_SINK_H
