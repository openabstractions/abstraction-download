#include <abstraction/discovery/client.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <abstraction/json/value.h>

#include <abstraction/ipc/client.hpp>
#ifndef _WIN32
#include <unistd.h>
#endif

namespace abstraction {
namespace discovery {

using Json = json::Value;
using Clock = std::chrono::steady_clock;  // monotonic: a deadline must not move
                                          // when somebody corrects the clock
using Deadline = Clock::time_point;

const char* const kRegistryFile = "services.json";
const char* const kBaseFeature = "abstraction.discovery/base@1";

// One object, one 0x0A. No \r, no BOM, no indenting: a pretty-printed object is
// unframeable, and the heartbeat writer this project already has indents.
static const char kRequest[] = "{\"ask\":\"who\"}\n";
static const std::size_t kRequestLen = sizeof(kRequest) - 1;

const char* to_string(Answer answer) {
    switch (answer) {
        case Answer::Present: return "present";
        case Answer::Incompatible: return "incompatible";
        default: return "absent";
    }
}

namespace {

std::string path_join(const std::string& dir, const std::string& leaf) {
#ifdef _WIN32
    const char sep = '\\';
#else
    const char sep = '/';
#endif
    if (dir.empty()) return leaf;
    if (dir.back() == '/' || dir.back() == '\\') return dir + leaf;
    return dir + sep + leaf;
}

// Trailing separators only. NOT realpath, NOT case folding, NOT any other
// normalisation: the contract spends a paragraph explaining why turning a path
// into one canonical string is a trap, and every one of those traps applies
// just as hard to comparing two path strings. So this compares what was
// written, and the burden is on the supervisor to publish the same string its
// callers hold.
bool same_store(const std::string& theirs, const std::string& ours) {
    auto trim = [](std::string s) {
        while (!s.empty() && (s.back() == '/' || s.back() == '\\')) s.pop_back();
        return s;
    };
    return trim(theirs) == trim(ours);
}

// One 0x0A-terminated line out of what has been read. `found` distinguishes
// "not yet" from "here it is"; a newline arriving past the cap is not a line.
bool line_from(const std::string& buffer, std::string* line) {
    const std::size_t cut = buffer.find('\n');
    if (cut == std::string::npos || cut > kMaxLine) return false;
    *line = buffer.substr(0, cut);
    return true;
}

Answer interpret(const std::string& line, const std::string& store_root,
                 const std::set<std::string>& known_critical) {
    // Non-throwing parse. A response is untrusted input from whatever managed
    // to bind the endpoint, and an exception escaping into a caller that asked
    // a yes/no question is the thing rule 1 forbids.
    bool parsed = false;
    const Json doc = Json::parse(line, &parsed);
    if (!parsed || !doc.is_object()) return Answer::Absent;

    // {"error": ...} answers a request this client did not send. It carries no
    // store and describes no supervisor, so there is nothing to hand work to.
    if (doc.contains("error")) return Answer::Absent;

    // The store check comes BEFORE the critical check deliberately. A response
    // from a different store is not this caller's supervisor at all, so its
    // extension names say nothing about whether OUR supervisor is compatible;
    // answering incompatible there would stop a caller on account of a daemon
    // it was never going to talk to.
    if (!doc.contains("store") || !doc["store"].is_string()) return Answer::Absent;
    if (!same_store(doc["store"].get<std::string>(), store_root)) return Answer::Absent;

    if (doc.contains("critical")) {
        if (!doc["critical"].is_array()) return Answer::Absent;
        for (const Json& feature : doc["critical"]) {
            // A non-string here is a name this client cannot possibly know,
            // which is the incompatible case rather than the malformed one.
            // Refusing to hand work over is the safe half of that ambiguity.
            if (!feature.is_string() ||
                known_critical.find(feature.get<std::string>()) == known_critical.end()) {
                return Answer::Incompatible;
            }
        }
    }
    return Answer::Present;
}

bool exchange(const std::string& path, Deadline deadline, std::string* line) {
    ipc::Stream stream(path, deadline);
    if (!stream.valid() || !stream.write_all(std::string_view(kRequest, kRequestLen))) return false;
    std::string buffer;
    char chunk[4096];
    for (;;) {
        std::size_t moved = 0;
        if (!stream.read_some(chunk, sizeof(chunk), moved)) return false;
        buffer.append(chunk, moved);
        if (line_from(buffer, line)) return true;
        if (buffer.size() > kMaxLine) return false;
    }
}

}  // namespace

std::string endpoint_for(const std::string& store_root, const std::string& service) {
    std::ifstream in(path_join(store_root, kRegistryFile), std::ios::binary);
    if (!in) return {};  // no store, or no registry. Nothing is listening.
    std::ostringstream body;
    body << in.rdbuf();

    bool parsed = false;
    const Json doc = Json::parse(body.str(), &parsed);
    if (!parsed || !doc.is_object()) return {};
    if (!doc.contains("services") || !doc["services"].is_object()) return {};
    const Json& services = doc["services"];
    if (!services.contains(service) || !services[service].is_string()) return {};
    return services[service].get<std::string>();
}

std::string endpoint_path(const std::string& name) {
#ifdef _WIN32
    return "\\\\.\\pipe\\" + name;
#else
    const char* runtime = std::getenv("XDG_RUNTIME_DIR");
    if (runtime != nullptr && runtime[0] != '\0') {
        return path_join(runtime, name + ".sock");
    }
    return path_join("/tmp/abstraction-" + std::to_string(static_cast<long>(::getuid())),
                     name + ".sock");
#endif
}

Answer ask(const std::string& store_root, const std::string& service, const Options& options) {
    // Rule 2, taken literally: ONE deadline, computed at entry, spent by
    // everything after it. A client whose budget restarts per phase can be
    // walked through three full timeouts by a server that stalls each in turn.
    const Deadline deadline = Clock::now() + options.deadline;

    const std::string name = endpoint_for(store_root, service);
    if (name.empty()) return Answer::Absent;

    std::string line;
    if (!exchange(endpoint_path(name), deadline, &line)) return Answer::Absent;
    return interpret(line, store_root, options.known_critical);
}

}  // namespace discovery
}  // namespace abstraction
