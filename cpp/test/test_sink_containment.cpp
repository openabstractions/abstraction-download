// The same cases as download/go/containment_test.go and
// download/python/test_abstraction_download.py, deliberately. A record one
// implementation refuses and another acts on is worse than either behaviour on
// its own, and this side reads records it does not run — so agreeing about the
// refusal is the only thing it can contribute.

#include <abstraction/download/sink.h>

#include <cstdio>
#include <string>

using abstraction::download::escapes_root;
using abstraction::download::foreign_path;
using abstraction::download::reserved_sink;
using abstraction::download::under;

static int g_failures = 0;

static void check(const char* name, bool ok) {
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", name);
    if (!ok) ++g_failures;
}

// The confused deputy, pinned.
//
// A PC submits the record and a NAS adopts it, so the sink is a destination the
// SUBMITTER chose and the ADOPTER writes to, with the adopter's authority.
// Joining the two without checking resolved these to real files outside the
// store — an ssh authorized_keys and a Startup directory among them.
// Reproduced against a store root of C:\store\jobs.
static void test_sink_may_not_escape_the_store_root() {
    const std::string root = "C:\\store\\jobs";
    const auto resolves_inside = [&](const std::string& p) {
        return under(root, root + "/" + p);
    };

    const char* out[] = {
        "../../../Users/victim/.ssh/authorized_keys",
        "..\\..\\..\\Startup\\evil.bat",
        "..",
        "a/../../b",            // clean on its own and still one level out
        "models/../../x.gguf",  // the climb is not at the front
    };
    for (const char* p : out) {
        check((std::string("escapes: ") + p).c_str(), !resolves_inside(p));
        check((std::string("refused: ") + p).c_str(), !escapes_root(p).empty());
    }

    const char* in[] = {
        "models/x.gguf",
        "work/2f8a-b1",
        "models/org/repo/rev/x.gguf",
        "models/./x.gguf",
        "models/tmp/../x.gguf",  // clean it and it never leaves
    };
    for (const char* p : in) {
        check((std::string("contained: ") + p).c_str(), resolves_inside(p));
        check((std::string("allowed: ") + p).c_str(), escapes_root(p).empty());
    }
}

// Spelled out in full, because Go and Python must print this same string: see
// download/go/containment_test.go and
// download/python/test_abstraction_download.py. It names the path FROM THE
// RECORD, not the one it resolved to, so a caller can find the field it got
// wrong.
static void test_refusal_names_the_path_from_the_record() {
    check("refusal wording",
          escapes_root("../../../Users/victim/.ssh/authorized_keys") ==
              "download: sink path escapes the store root: "
              "../../../Users/victim/.ssh/authorized_keys");
    check("refusal wording, backslashes verbatim",
          escapes_root("..\\..\\..\\Startup\\evil.bat") ==
              "download: sink path escapes the store root: ..\\..\\..\\Startup\\evil.bat");
}

// The classic way this fix ships broken: C:\store2 starts with C:\store, so a
// prefix test on the raw strings calls a different directory contained.
static void test_containment_is_not_a_prefix_test() {
    struct Case {
        const char* root;
        const char* resolved;
        bool want;
    };
    const Case cases[] = {
        {"C:\\store", "C:\\store2\\x.gguf", false},
        {"C:\\store", "C:\\store\\x.gguf", true},
        {"C:\\store", "C:\\store", true},
        {"C:\\store\\", "C:\\store\\x.gguf", true},  // a trailing separator
        {"/store", "/store2/x.gguf", false},
        {"/store", "/store/x.gguf", true},
        {"/", "/x.gguf", true},

        // A UNC root: the share is part of the root, so a sibling share is out.
        {"\\\\nas\\models\\store", "\\\\nas\\models\\store\\x.gguf", true},
        {"\\\\nas\\models\\store", "\\\\nas\\models\\store2\\x.gguf", false},
        {"\\\\nas\\models\\store", "\\\\nas\\other\\store\\x.gguf", false},

        // Nothing to be under: the store's binding is not a filesystem.
        {"", "models/x.gguf", true},
        {"", "../x.gguf", false},
        {"", "..", false},
    };
    for (const Case& c : cases) {
        const std::string name =
            std::string("under(") + c.root + ", " + c.resolved + ")";
        check(name.c_str(), under(c.root, c.resolved) == c.want);
    }

    // Windows ignores case in a path and POSIX does not, so this is asked of
    // the platform the test is running on rather than assumed either way.
#ifdef _WIN32
    const bool case_folds = true;
#else
    const bool case_folds = false;
#endif
    check("case folding follows the platform",
          under("C:\\Store", "C:\\store\\x.gguf") == case_folds);
}

// escapes_root answers about a RECORD, which names no root — that is what lets
// the three readers refuse the same records. An absolute path answers "": it is
// never joined onto the root, and what a machine adopting a record should do
// with one is a separate question this does not decide.
static void test_escapes_root_answers_without_a_root() {
    const char* absolute[] = {
        "", "D:\\models\\x.gguf", "/mnt/models/x.gguf", "\\\\nas\\share\\x.gguf",
    };
    for (const char* p : absolute) {
        check((std::string("absolute is not this check's business: ") + p).c_str(),
              escapes_root(p).empty());
    }
}

// Contained, and still aimed at the store.
//
// Containment stopped a sink climbing OUT of the root and never stopped one
// naming what is IN it. A final of "jobs/<id>.json" overwrites a job record; a
// final of "work/<other>" overwrites another job's partial. Both are inside the
// root and both passed every check that existed.
static void test_sink_may_not_name_the_stores_own_files() {
    const std::string me = "1757000000000-deadbeef";
    const std::string other = "1757000000001-cafebabe";

    const std::string out[] = {
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
        // The spellings a filesystem folds into the ones above. NTFS lands
        // "Jobs/x.json" in "jobs/", so a rule that folded case only where the
        // host does would refuse this on Windows and accept it on Linux.
        "Jobs/" + other + ".json",
        "jobs\\" + other + ".json",
        "JOBS/" + other + ".json",
        "models/../jobs/" + other + ".json",
        "Supervisor.json",
        "WORK/" + other,
    };
    for (const std::string& p : out) {
        check(("reserved: " + p).c_str(), !reserved_sink(me, p).empty());
    }

    // A job's own scratch is the one reserved path it may write — the default
    // partial goes there — and nothing else about the store is spellable.
    const std::string in[] = {
        "",
        "work/" + me,
        "work/" + me + "/part",
        "models/x.gguf",
        "jobsy/x.json",
        "a/jobs/x.json",
        "services.json.bak",
        "D:\\models\\x.gguf",
        "/mnt/models/x.gguf",
    };
    for (const std::string& p : in) {
        check(("free: " + p).c_str(), reserved_sink(me, p).empty());
    }

    // Spelled out in full, because Go and Python print this same string.
    check("reserved wording",
          reserved_sink(me, "jobs/" + other + ".json") ==
              "download: sink path is reserved by the store: jobs/" + other + ".json");
}

// An absolute sink is honoured only by a machine whose convention it is written
// in. The contract claimed a foreign absolute path "fails with no such file
// rather than quietly creating a directory"; that is true of reading and false
// of a sink, where O_CREAT makes a file of the literal name instead.
static void test_absolute_sink_is_refused_on_the_other_platform() {
    const std::string windows[] = {"D:\\models\\x.gguf", "\\\\nas\\share\\x.gguf",
                                   "c:\\models\\x.gguf"};
    const std::string posix[] = {"/mnt/models/x.gguf", "/models/x.gguf"};

    for (const std::string& p : windows) {
#ifdef _WIN32
        check(("this platform's own: " + p).c_str(), foreign_path(p).empty());
#else
        check(("foreign: " + p).c_str(), !foreign_path(p).empty());
#endif
    }
    for (const std::string& p : posix) {
#ifdef _WIN32
        check(("foreign: " + p).c_str(), !foreign_path(p).empty());
#else
        check(("this platform's own: " + p).c_str(), foreign_path(p).empty());
#endif
    }
    // Relative paths are the portable form and never this check's business.
    for (const char* p : {"", "models/x.gguf", "work/abc"}) {
        check((std::string("relative: ") + p).c_str(), foreign_path(p).empty());
    }
#ifdef _WIN32
    const std::string one = posix[0];
#else
    const std::string one = windows[0];
#endif
    check("foreign wording",
          foreign_path(one) ==
              "download: sink path names another platform's filesystem: " + one);
}

// The confused deputy at the sink field, one spelling past the containment that
// closed the relative case. On a store several machines can write, an ABSOLUTE
// sink in this machine's own convention passes every lexical check -- escapes
// nothing (it is never joined onto the root) and is not foreign -- and lands
// wherever a foreign record chose. unportable_sink refuses it; a relative sink
// is untouched, because it is the portable, contained form.
static void test_absolute_sink_is_refused_on_a_shared_store() {
#ifdef _WIN32
    const std::string native = "C:\\Windows\\System32\\evil.exe";
#else
    const std::string native = "/etc/cron.d/evil";
#endif
    check("a native absolute sink is refused on a shared store",
          !abstraction::download::unportable_sink(native).empty());
    check("unportable wording",
          abstraction::download::unportable_sink(native) ==
              "download: a shared store's record may only name a relative sink: " + native);

    // A relative sink is the portable form and is never refused by this rule --
    // containment covers whether it stays under the root.
    for (const char* p : {"", "models/x.gguf", "work/abc", "models/org/repo/x.gguf"}) {
        check((std::string("relative is portable: ") + p).c_str(),
              abstraction::download::unportable_sink(p).empty());
    }
}

int main() {
    // Unbuffered, so a crash still tells you which check it died after.
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    test_sink_may_not_escape_the_store_root();
    test_refusal_names_the_path_from_the_record();
    test_containment_is_not_a_prefix_test();
    test_escapes_root_answers_without_a_root();
    test_sink_may_not_name_the_stores_own_files();
    test_absolute_sink_is_refused_on_the_other_platform();
    test_absolute_sink_is_refused_on_a_shared_store();

    std::printf("\n%s: %d failure(s)\n", g_failures == 0 ? "OK" : "FAILED", g_failures);
    return g_failures == 0 ? 0 : 1;
}
