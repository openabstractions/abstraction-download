// specread — print what this implementation understood from a download spec.
//
// The job RECORD has cross-language conformance tests: three implementations
// pass one record around and must agree byte for byte. The SPEC inside it had
// none, and could not, because the job layer refuses to look inside a spec —
// which is what lets download evolve without a schema change in three languages.
//
// That opacity is right and it left a hole, and this program exists because the
// hole cost something. This implementation wrote a digest bare; the Go one built
// "sha256:" + hex and compared strings. The error read "got sha256:1fc70f… want
// 1fc70f…" — the same digest twice — and a correct 1.5 GB download was deleted
// and fetched again.
//
// So the record's conformance is by identical BYTES and the spec's is by
// identical MEANING. This prints the meaning, in the form the Go and Python
// readers print it.
//
//     usage: specread <spec.json>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <abstraction/download/digest.h>
#include <abstraction/download/sink.h>
#include <abstraction/job/record.h>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif
#include <sstream>
#include <string>
#include <vector>

using abstraction::job::Json;

namespace {

using abstraction::download::normal_digest;
using abstraction::download::portable;

std::string str_at(const Json& obj, const char* key) {
    if (!obj.is_object() || !obj.contains(key) || !obj.at(key).is_string()) {
        return "";
    }
    return obj.at(key).get<std::string>();
}

// A string map in the one spelling every implementation prints: sorted by key,
// "k=v" joined with commas. Sorted because a JSON object has no order and this
// output is compared against two other readers.
std::string pairs(const Json& obj, const char* key) {
    if (!obj.is_object() || !obj.contains(key) || !obj.at(key).is_object()) {
        return "";
    }
    std::vector<std::pair<std::string, std::string>> kv;
    for (auto it = obj.at(key).begin(); it != obj.at(key).end(); ++it) {
        if (it.value().is_string()) {
            kv.emplace_back(it.key(), it.value().get<std::string>());
        }
    }
    std::sort(kv.begin(), kv.end());
    std::string out;
    for (const auto& one : kv) {
        if (!out.empty()) {
            out += ",";
        }
        out += one.first + "=" + one.second;
    }
    return out;
}

std::int64_t int_at(const Json& obj, const char* key) {
    if (!obj.is_object() || !obj.contains(key) || !obj.at(key).is_number_integer()) {
        return 0;
    }
    return obj.at(key).get<std::int64_t>();
}

// Everything that touches the parsed document, in one place, so that main can
// wrap it in the one catch. A `size` of 9223372036854775808 parses as a number
// and then throws on the way into an int64, and with the catch around the parse
// alone that reached std::terminate: the reader neither read the spec nor
// refused it, and a caller with a pipe saw an empty answer at exit 127.
int print_spec(const Json& d) {
    const Json artifact = d.contains("artifact") ? d.at("artifact") : Json::object();
    const Json sink = d.contains("sink") ? d.at("sink") : Json::object();

    std::printf("digest=%s\n", normal_digest(str_at(artifact, "digest")).c_str());
    std::printf("size=%lld\n", static_cast<long long>(int_at(artifact, "size")));
    std::printf("final=%s\n", portable(str_at(sink, "final")).c_str());
    std::printf("partial=%s\n", portable(str_at(sink, "partial")).c_str());

    // Whether each sink path stays under the store root, and in the same words
    // everywhere. A relative path that climbs out of the root is refused by the
    // machine that would do the writing, and the three implementations have to
    // refuse the same records for the same stated reason — otherwise a record
    // one of them will not touch is quietly acted on by another. Empty means
    // nothing to refuse, the same way an unreadable digest reads as empty.
    std::printf("final_refusal=%s\n",
                abstraction::download::escapes_root(str_at(sink, "final")).c_str());
    std::printf("partial_refusal=%s\n",
                abstraction::download::escapes_root(str_at(sink, "partial")).c_str());

    // In the order they would be tried, because that order is the behaviour: a
    // local copy at priority -100 is what turns a download into a copy, and an
    // implementation that sorted differently would fetch over the network while
    // the bytes sat on disk. Stable, so equal priorities keep their given order.
    std::vector<std::pair<std::int64_t, Json>> sources;
    if (d.contains("sources") && d.at("sources").is_array()) {
        for (const Json& s : d.at("sources")) {
            sources.emplace_back(int_at(s, "priority"), s);
        }
    }
    std::stable_sort(sources.begin(), sources.end(),
                     [](const auto& a, const auto& b) { return a.first < b.first; });
    // Which keys describe the source and which are sent to the server. One bag
    // used to be both, so an attribute nobody remembered to exclude went out as
    // a header — and no unit test in one language could see that another
    // classified the same key differently. This is the line that makes the
    // split a contract rather than one implementation's habit.
    for (std::size_t i = 0; i < sources.size(); ++i) {
        std::printf("source%zu=%s|%s\n", i, str_at(sources[i].second, "scheme").c_str(),
                    str_at(sources[i].second, "locator").c_str());
        std::printf("source%zu.attrs=%s\n", i, pairs(sources[i].second, "attrs").c_str());
        std::printf("source%zu.headers=%s\n", i, pairs(sources[i].second, "headers").c_str());
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
#ifdef _WIN32
    // Text mode would turn every LF into CRLF, and this output is a conformance
    // surface compared against other implementations byte for byte. Without
    // this the three readers agree about every digest, size and path, and the
    // test still fails — on line endings, which is the least interesting way to
    // disagree and the hardest to see in a diff.
    _setmode(_fileno(stdout), _O_BINARY);
#endif
    if (argc < 2) {
        std::cerr << "usage: specread <spec.json>\n"
                     "       specread --echo <spec.json>\n"
                     "       specread --partial <final> <id>\n"
                     "       specread --portable <path>\n"
                     "       specread --reserved <owner-id> <path>\n"
                     "       specread --foreign <path>\n";
        return 2;
    }
    // The spelling a path gets when it is written into a record. Every other
    // check here reads a path back; this one is the only view of what the layer
    // WROTE, and the disagreement it pins was visible in a window before it was
    // visible to any of them: two finished jobs whose destinations were spelled
    // differently, one of them changing convention halfway along.
    if (std::string(argv[1]) == "--portable") {
        if (argc < 3) {
            std::cerr << "usage: specread --portable <path>\n";
            return 2;
        }
        std::printf("portable=%s\n", portable(argv[2]).c_str());
        return 0;
    }
    // The spec as this implementation would carry it in a record. Go holds it
    // as raw bytes and Python and C++ hold it parsed, so a spec's bytes may
    // change on the way through one implementation and not another — a number
    // respelled, an escape policy applied — and every reader downstream sees
    // the changed ones. Compared compact because whitespace is the record
    // writer's choice; escapes and number spellings survive compaction and are
    // not.
    if (std::string(argv[1]) == "--echo") {
        if (argc < 3) {
            std::cerr << "usage: specread --echo <spec.json>\n";
            return 2;
        }
        std::ifstream src(argv[2], std::ios::binary);
        if (!src) {
            std::cerr << "specread: cannot open " << argv[2] << "\n";
            return 1;
        }
        std::ostringstream text;
        text << src.rdbuf();
        try {
            std::printf("echo=%s\n", Json::parse(text.str()).dump().c_str());
        } catch (const std::exception& e) {
            std::cerr << "specread: " << e.what() << "\n";
            return 1;
        }
        return 0;
    }
    // The partial name a caller who chose none would get. It is not read out of
    // a spec — it is INVENTED, by whichever implementation happens to submit —
    // and it then lands in the record for the others to resume from. Two
    // implementations inventing it independently is the same shape of bug as the
    // digest that cost a 1.5 GB download, so it is compared here too.
    if (std::string(argv[1]) == "--partial") {
        if (argc < 4) {
            std::cerr << "usage: specread --partial <final> <id>\n";
            return 2;
        }
        std::printf("partial=%s\n",
                    abstraction::download::partial_for(portable(argv[2]), argv[3]).c_str());
        return 0;
    }
    // Whether a sink names the store's own layout. Contained paths, all of
    // them, and every one able to overwrite a job record or another job's
    // partial — so the three implementations have to refuse exactly the same
    // set, and the set is not spellable in a fixture because it depends on
    // which job is asking.
    if (std::string(argv[1]) == "--reserved") {
        if (argc < 4) {
            std::cerr << "usage: specread --reserved <owner-id> <path>\n";
            return 2;
        }
        std::printf("reserved=%s\n",
                    abstraction::download::reserved_sink(argv[2], argv[3]).c_str());
        return 0;
    }
    // Whether this machine may write an absolute sink at all. The only answer
    // here that depends on the host, and it must still be the same answer from
    // all three implementations ON that host.
    if (std::string(argv[1]) == "--foreign") {
        if (argc < 3) {
            std::cerr << "usage: specread --foreign <path>\n";
            return 2;
        }
        std::printf("foreign=%s\n", abstraction::download::foreign_path(argv[2]).c_str());
        return 0;
    }
    std::ifstream in(argv[1], std::ios::binary);
    if (!in) {
        std::cerr << "specread: cannot open " << argv[1] << "\n";
        return 1;
    }
    std::ostringstream body;
    body << in.rdbuf();

    try {
        return print_spec(Json::parse(body.str()));
    } catch (const std::exception& e) {
        std::cerr << "specread: " << e.what() << "\n";
        return 1;
    }
}
