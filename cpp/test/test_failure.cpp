// A failure written by any of the three implementations, recovered here.
//
// This is the half of a failure that no exception carries across a process.
// `Record.error` is prose; rebuilding from it alone answers "retryable" about
// every refusal this layer declares forever, and a job that stays adoptable is
// fetched again on every sweep for as long as the store exists.
//
//     test_failure <corpus-dir>

#include <abstraction/download/failure.h>
#include <abstraction/job/record.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>

namespace fs = std::filesystem;

using abstraction::download::last_failure;
using abstraction::job::Record;

static int g_failures = 0;

static void check(const std::string& name, bool ok) {
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", name.c_str());
    if (!ok) {
        ++g_failures;
    }
}

static std::string read(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

// The corpus and what each record must still MEAN, read off the table that
// ships beside it rather than restated here: a second copy of the answers is a
// second thing to keep true.
static std::map<std::string, std::string> corpus(const fs::path& dir) {
    std::map<std::string, std::string> want;
    std::istringstream lines(read(dir / "expect.txt"));
    std::string line;
    while (std::getline(lines, line)) {
        std::istringstream fields(line);
        std::string name, cls;
        if (!(fields >> name >> cls) || name.empty() || name[0] == '#') {
            continue;
        }
        want[name] = cls;
    }
    return want;
}

static std::string failure_class(const Record& r) {
    const auto last = last_failure(r);
    if (!last) return "none";
    return last->permanent() ? "permanent" : "retryable";
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: test_failure <corpus-dir>\n");
        return 2;
    }
    const fs::path dir = argv[1];
    const auto want = corpus(dir);
    check("the corpus table names at least one record", !want.empty());

    for (const auto& [name, cls] : want) {
        const Record r = Record::decode(read(dir / (name + ".json")));
        check(name + " means " + cls, failure_class(r) == cls);
    }

    // A record added to the directory and left out of the table is asserted
    // about by nothing, which is the shape of every instrument that quietly
    // stopped measuring.
    for (const auto& entry : fs::directory_iterator(dir)) {
        const std::string file = entry.path().filename().string();
        if (file.size() > 5 && file.substr(file.size() - 5) == ".json") {
            check(file + " is named by expect.txt",
                  want.count(file.substr(0, file.size() - 5)) == 1);
        }
    }

    std::printf("%s\n", g_failures == 0 ? "ok" : "FAILED");
    return g_failures == 0 ? 0 : 1;
}
