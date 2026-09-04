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

// A digest reduced to the part that carries the meaning. One implementation
// writes "sha256:<hex>", another the bare hex, and the contract is that they
// name the same artifact. Anything unrecognised becomes empty rather than
// itself, so a comparison cannot succeed on two things neither side understood.
std::string normal_digest(const std::string& raw) {
    std::string s;
    for (char c : raw) {
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
            s.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
    }
    for (const char* prefix : {"sha256:", "sha256-"}) {
        const std::string p(prefix);
        if (s.rfind(p, 0) == 0) {
            s = s.substr(p.size());
            break;
        }
    }
    if (s.size() != 64) {
        return "";
    }
    for (char c : s) {
        const bool hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        if (!hex) {
            return "";
        }
    }
    return "sha256:" + s;
}

// A relative path in the one form every machine reads the same way. Absolute
// paths are left exactly as given: they already name a specific machine's
// filesystem, and rewriting their separators would not make them more portable.
std::string portable(const std::string& p) {
    if (p.empty()) {
        return "";
    }
    if (p[0] == '/' || p[0] == '\\') {
        return p;
    }
    if (p.size() >= 2 && p[1] == ':' && std::isalpha(static_cast<unsigned char>(p[0]))) {
        return p;
    }
    std::string out = p;
    std::replace(out.begin(), out.end(), '\\', '/');
    return out;
}

std::string str_at(const Json& obj, const char* key) {
    if (!obj.is_object() || !obj.contains(key) || !obj.at(key).is_string()) {
        return "";
    }
    return obj.at(key).get<std::string>();
}

std::int64_t int_at(const Json& obj, const char* key) {
    if (!obj.is_object() || !obj.contains(key) || !obj.at(key).is_number_integer()) {
        return 0;
    }
    return obj.at(key).get<std::int64_t>();
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
        std::cerr << "usage: specread <spec.json>\n";
        return 2;
    }
    std::ifstream in(argv[1], std::ios::binary);
    if (!in) {
        std::cerr << "specread: cannot open " << argv[1] << "\n";
        return 1;
    }
    std::ostringstream body;
    body << in.rdbuf();

    Json d;
    try {
        d = Json::parse(body.str());
    } catch (const std::exception& e) {
        std::cerr << "specread: " << e.what() << "\n";
        return 1;
    }

    const Json artifact = d.contains("artifact") ? d.at("artifact") : Json::object();
    const Json sink = d.contains("sink") ? d.at("sink") : Json::object();

    std::printf("digest=%s\n", normal_digest(str_at(artifact, "digest")).c_str());
    std::printf("size=%lld\n", static_cast<long long>(int_at(artifact, "size")));
    std::printf("final=%s\n", portable(str_at(sink, "final")).c_str());
    std::printf("partial=%s\n", portable(str_at(sink, "partial")).c_str());

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
    for (std::size_t i = 0; i < sources.size(); ++i) {
        std::printf("source%zu=%s|%s\n", i, str_at(sources[i].second, "scheme").c_str(),
                    str_at(sources[i].second, "locator").c_str());
    }
    return 0;
}
