// replay applies a scripted sequence of operations to a job store and prints
// what an observer would have seen after each one.
//
// The C++ third of scripts/behaviour-conformance.sh. Same scenario file, same
// transcript, no knowledge of any other implementation.

#include <abstraction/download/runner.h>
#include <abstraction/download/sink.h>
#include <abstraction/download/wanted.h>
#include <abstraction/job/awake.h>
#include <abstraction/job/store.h>
#include <abstraction/job/watch.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

namespace {

void setenv_now(const std::string& key, const std::string& value) {
#ifdef _WIN32
    _putenv_s(key.c_str(), value.c_str());
#else
    setenv(key.c_str(), value.c_str(), 1);
#endif
}

using abstraction::job::FileStore;
using abstraction::job::Json;
using abstraction::job::Record;

// Where download/testdata/fixture.py is listening, or "" when the harness
// started no server. A driver that cannot reach one must not claim the wire
// capability: a scenario nobody ran is counted as unproven, and a scenario
// silently skipped reads as a pass.
std::string fixture() {
    const char* const v = std::getenv("ABSTRACTION_FIXTURE");
    return v == nullptr ? std::string() : std::string(v);
}

std::string capabilities() {
    if (fixture().empty()) {
        return "store transfer";
    }
    if (!abstraction::download::https_available()) {
        return "store transfer wanted";
    }
    return "store transfer wire wanted";
}

// Every content-set name this implementation can read, and whether it may be
// marked critical. A record naming anything absent here in `critical` is
// refused, so the roster is the whole of what this reader negotiates over and it
// belongs where a harness can diff it against the other two.
std::string models() {
    std::ostringstream o;
    for (const std::string& name : abstraction::job::known_features()) {
        o << name << (abstraction::job::is_never_critical_feature(name) ? " never-critical"
                                                                     : " critical-ok")
          << "\n";
    }
    return o.str();
}

std::map<std::string, std::string> g_ids;
std::map<std::string, std::string> g_refused;
std::map<std::string, std::int64_t> g_epochs;
std::map<std::string, std::unique_ptr<abstraction::job::Subscription>> g_subs;
std::map<std::string, std::unique_ptr<abstraction::job::KeepAwake>> g_holds;

// Names a refusal in a vocabulary all three implementations share. The wording
// of an error is not a contract and must never become one; which class of
// refusal happened is exactly what a caller branches on.
std::string outcome(const abstraction::job::JobError* e) {
    if (e == nullptr) {
        return "ok";
    }
    const std::string name = e->name();
    if (name == "NotFound") return "not-found";
    if (name == "LeaseHeld") return "lease-held";
    if (name == "StaleEpoch") return "stale-epoch";
    if (name == "LeaseExpired") return "lease-expired";
    if (name == "TerminalState") return "terminal";
    if (name == "UnknownSchema") return "unknown-model";
    if (name == "Invalid") return "invalid";
    return "refused";
}

std::string fields(const Record& r) {
    std::ostringstream o;
    o << "state=" << r.state << " epoch=" << r.lease.epoch
      << " held=" << (r.lease.held(abstraction::job::Clock::now()) ? "yes" : "no")
      << " recall=" << (r.lease.recalled() ? r.lease.recall->reason : "none")
      << " want=" << r.wants() << " done=" << r.progress.done
      << " err=" << (r.error.empty() ? "none" : "set")
      << " cp=" << (r.checkpoint ? r.checkpoint->dump() : "none") << " content=";
    for (std::size_t i = 0; i < r.content.size(); ++i) {
        o << (i == 0 ? "" : ",") << r.content[i];
    }
    o << " crit=";
    for (std::size_t i = 0; i < r.critical.size(); ++i) {
        o << (i == 0 ? "" : ",") << r.critical[i];
    }
    const auto hold = g_holds.find(r.id);
    o << " awake=" << (hold != g_holds.end() && hold->second->held() ? "yes" : "no");
    return o.str();
}

class Replay {
public:
    explicit Replay(std::string work) : work_(std::move(work)), store_(work_ + "/store") {}

    std::string run_line(const std::vector<std::string>& f) {
        const std::string& op = f[0];
        if (op == "submit") return submit(f[1], flags(f));
        if (op == "claim") return claim(f[1], f[2], std::stoll(f[3]));
        if (op == "renew") return renew(f[1], f[2], std::stoll(f[3]));
        if (op == "progress") return progress(f[1], f[2], std::stoll(f[3]), rest(f, 4));
        if (op == "hold") return hold(f[1]);
        if (op == "release") return release(f[1], f[2]);
        if (op == "finish") return finish(f[1], f[2], f[3]);
        if (op == "intent") return intent(f[1], f[2]);
        if (op == "recall") return recall(f[1], f[2], std::stoll(f[3]), rest(f, 4));
        if (op == "state") return show(f[1], nullptr);
        if (op == "orphans") return orphans();
        if (op == "run") return run(f[1], f[2], false);
        if (op == "credential") return credential(f[1], f.size() > 2 ? f[2] : "-");
        if (op == "runshared") return run(f[1], f[2], true);
        if (op == "refuse") {
            g_refused[f[1]] = rest(f, 2);
            return "ok";
        }
        if (op == "allow") {
            g_refused.erase(f[1]);
            return "ok";
        }
        if (op == "stage") return stage(f[1], std::stoll(f[2]), f.size() > 3 ? f[3] : "");
        if (op == "plant") return plant(f[1], f[2], f[3]);
        if (op == "watch") return watch(f[1], f.size() > 2 ? std::stoll(f[2]) : 0);
        if (op == "next") return next(f[1]);
        if (op == "close") return close(f[1]);
        if (op == "sleep") {
            std::this_thread::sleep_for(std::chrono::milliseconds(std::stoll(f[1])));
            return "ok";
        }
        if (op == "drop") return drop(f[1], flags(f));
        if (op == "sweep") return sweep();
        return "unsupported-op";
    }

private:
    // The remainder of the line, rejoined. A checkpoint is JSON and JSON
    // carries spaces -- an HTTP date is nothing but spaces -- so the last
    // argument of an operation cannot be one whitespace-separated word.
    static std::string rest(const std::vector<std::string>& f, std::size_t i) {
        std::string joined;
        for (; i < f.size(); ++i) {
            joined += (joined.empty() ? "" : " ") + f[i];
        }
        return joined;
    }

    static std::map<std::string, std::string> flags(const std::vector<std::string>& f) {
        std::map<std::string, std::string> m;
        for (std::size_t i = 2; i < f.size(); ++i) {
            const auto eq = f[i].find('=');
            if (eq != std::string::npos) {
                m[f[i].substr(0, eq)] = f[i].substr(eq + 1);
            }
        }
        return m;
    }

    static std::string flag(const std::map<std::string, std::string>& m, const std::string& k) {
        const auto it = m.find(k);
        return it == m.end() ? std::string() : it->second;
    }

    // The verdict, then what the record looks like from outside. Printed even
    // when the operation was refused: what a refusal leaves behind is the half
    // of it a caller has to live with.
    std::string watch(const std::string& name, std::int64_t budget_ms) {
        g_subs[name] = std::make_unique<abstraction::job::Subscription>(
            store_, abstraction::download::kKind, std::chrono::milliseconds(budget_ms));
        return "ok";
    }

    // What a listener was handed: the kind of notice, then every job the
    // scenario named as state/done. Never the silence -- clocks are not compared.
    std::string next(const std::string& name) {
        const auto it = g_subs.find(name);
        if (it == g_subs.end()) return "not-found";
        const auto n = it->second->next();
        if (!n) return "closed";
        return std::string(n->quiet ? "quiet " : "changed ") + present(n->records);
    }

    static std::string present(const std::vector<Record>& records) {
        std::map<std::string, std::string> by_id;
        for (const auto& entry : g_ids) by_id[entry.second] = entry.first;
        std::vector<std::string> parts;
        for (const Record& r : records) {
            const auto a = by_id.find(r.id);
            if (a == by_id.end()) continue;
            parts.push_back(a->second + "=" + r.state + "/" + std::to_string(r.progress.done));
        }
        std::sort(parts.begin(), parts.end());
        std::string joined;
        for (const std::string& p : parts) joined += (joined.empty() ? "" : " ") + p;
        return joined.empty() ? "-" : joined;
    }

    std::string close(const std::string& name) {
        const auto it = g_subs.find(name);
        if (it == g_subs.end()) return "not-found";
        it->second->close();
        return "ok";
    }

    std::string show(const std::string& alias, const abstraction::job::JobError* e) {
        try {
            return outcome(e) + " " + fields(store_.load(g_ids[alias]));
        } catch (const abstraction::job::JobError& loaded) {
            return outcome(&loaded);
        }
    }

    template <typename F>
    std::string attempt(const std::string& alias, F call) {
        try {
            call();
        } catch (const abstraction::job::JobError& e) {
            return show(alias, &e);
        }
        return show(alias, nullptr);
    }

    // The bytes a scenario transfers. Content is a function of the offset alone,
    // so every implementation makes the same file and the same digest without
    // any of them being told what it is.
    static std::string artifact(std::int64_t size) {
        std::string body(static_cast<std::size_t>(size < 0 ? 0 : size), '\0');
        for (std::size_t i = 0; i < body.size(); ++i) {
            body[i] = static_cast<char>(i % 251);
        }
        return body;
    }

    // Holds the canary token under `name`, bound to `hosts` -- what a machine
    // that holds a secret looks like to the runner.
    static std::string credential(const std::string& name, const std::string& hosts) {
        std::string key = "ABSTRACTION_CRED_";
        for (char c : name) {
            key += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        setenv_now(key, "hf_thisMustNeverAppearOnDisk_EXAMPLE");
        setenv_now(key + "_HOSTS", hosts == "-" ? "" : hosts);
        return "ok";
    }

    // Names the sink. `foreign` is an absolute path in the OTHER platform's
    // convention, and the driver spells it rather than the scenario because
    // which spelling is foreign depends on the host: a scenario that named
    // `/mnt/...` would assert one thing on Windows and its opposite on Linux,
    // and the behaviour under test is the same on both.
    static std::string final_for(const std::string& alias, const std::string& kind) {
        if (kind == "foreign") {
#ifdef _WIN32
            return "/mnt/store/models/" + alias + ".bin";
#else
            return "C:\\store\\models\\" + alias + ".bin";
#endif
        }
        if (kind == "abs") {
            // THIS machine's own absolute convention, so foreign_path does not
            // catch it: the exact hole a shared-store runner must close.
#ifdef _WIN32
            return "C:\\abstraction-deputy\\" + alias + ".bin";
#else
            return "/etc/cron.d/" + alias;
#endif
        }
        return "models/" + alias + ".bin";
    }

    std::string submit(const std::string& alias, const std::map<std::string, std::string>& a) {
        const std::int64_t size = a.count("size") ? std::stoll(flag(a, "size")) : 0;
        const std::string body = artifact(size);
        {
            std::ofstream out(work_ + "/artifact.bin", std::ios::binary);
            out.write(body.data(), static_cast<std::streamsize>(body.size()));
        }

        Record r;
        // The id is chosen here rather than by the store, because the partial's
        // name is derived from it and goes into the record — a successor in
        // another language finds a predecessor's bytes only by inventing the
        // same name.
        r.id = abstraction::job::new_id();
        r.kind = abstraction::download::kKind;

        Json spec;
        spec["artifact"] = Json::object();
        if (size > 0) {
            spec["artifact"]["size"] = size;
        }
        const std::string digest = flag(a, "digest");
        if (digest == "good") {
            spec["artifact"]["digest"] = abstraction::download::sha256_of(body.data(), body.size());
        } else if (digest == "bad") {
            spec["artifact"]["digest"] = "sha256:" + std::string(64, '0');
        }

        const std::string src = flag(a, "src");
        Json source = Json::object();
        if (src == "file") {
            source = {{"scheme", "file"}, {"locator", work_ + "/artifact.bin"}};
        } else if (src == "missing") {
            source = {{"scheme", "file"}, {"locator", work_ + "/absent.bin"}};
        } else if (src == "nofetcher") {
            source = {{"scheme", "gopher"}, {"locator", "gopher://example.invalid/x"}};
        } else if (src.rfind("http:", 0) == 0) {
            // The behaviour is a path segment, so a new wire case needs a
            // fixture answer and a scenario and no driver in any language
            // changes.
            source = {{"scheme", "http"},
                      {"locator", fixture() + "/" + src.substr(5) + "/" + std::to_string(size)}};
        }
        if (!flag(a, "cred").empty()) {
            source["attrs"] = {{"credential", flag(a, "cred")}};
        }
        spec["sources"] = Json::array({source});
        const std::string final_path = final_for(alias, flag(a, "sink"));
        spec["sink"] = {{"final", final_path},
                        {"partial", abstraction::download::partial_for(final_path, r.id)}};

        r.spec = spec;
        r.progress.total = size;
        try {
            g_ids[alias] = store_.submit(std::move(r));
        } catch (const abstraction::job::JobError& e) {
            return outcome(&e);
        }
        return show(alias, nullptr);
    }

    // Forges what a newer writer would have written: a content-set name this
    // implementation has never heard of, in `content` alone or in `critical`
    // too.
    //
    // It edits the file rather than going through the store because no
    // conforming writer can produce this record -- an implementation validates
    // its own declaration on the way out and refuses a name it could not read
    // back -- so the refusal path exists and nothing in the language could reach
    // it. The edit is textual on purpose: the encoding is fixed at two-space
    // indent, so inserting one element at the head of an array is the same three
    // lines in three languages.
    std::string plant(const std::string& alias, const std::string& where,
                      const std::string& name) {
        const std::string path = work_ + "/store/jobs/" + g_ids[alias] + ".json";
        std::vector<std::string> lines;
        {
            std::ifstream in(path, std::ios::binary);
            std::string line;
            while (std::getline(in, line)) {
                lines.push_back(line);
            }
        }
        std::vector<std::string> keys = {"content"};
        if (where == "critical") {
            keys.push_back("critical");
        }
        for (const std::string& key : keys) {
            std::vector<std::string> out;
            for (const std::string& line : lines) {
                out.push_back(line);
                if (line == "  \"" + key + "\": [") {
                    out.push_back("    \"" + name + "\",");
                }
            }
            lines = out;
        }
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        for (const std::string& line : lines) {
            out << line << "\n";
        }
        out.close();
        return show(alias, nullptr);
    }

    // Puts bytes in the partial before a run, so a resume has a prefix to
    // continue from and sends the Range request the wire scenarios are about.
    //
    // "stale" writes bytes from a DIFFERENT artifact. That is the case a bare
    // Range cannot see: the server replaced the file, the range it answers is
    // honest and belongs to a version the prefix never came from, and the
    // splice has the right length and the wrong contents.
    std::string stage(const std::string& alias, std::int64_t n, const std::string& kind) {
        std::string body = artifact(n);
        if (kind == "stale") {
            for (std::size_t i = 0; i < body.size(); ++i) {
                body[i] = static_cast<char>((i + 7) % 251);
            }
        }
        const std::string path =
            work_ + "/store/" +
            abstraction::download::partial_for("models/" + alias + ".bin", g_ids[alias]);
        std::error_code ec;
        std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(body.data(), static_cast<std::streamsize>(body.size()));
        out.close();
        return show(alias, nullptr);
    }

    std::string run(const std::string& alias, const std::string& owner, bool shared) {
        abstraction::download::Runner runner(store_, owner);
        runner.lease_ttl = std::chrono::milliseconds(5000);
        runner.shared_store = shared;
        runner.reach = [](const std::string& host) {
            const auto it = g_refused.find(host);
            return it == g_refused.end() ? std::string() : it->second;
        };
        try {
            runner.run(g_ids[alias]);
        } catch (const std::exception&) {
            // The verdict for a transfer that stopped is not one of the store's
            // refusal classes: nothing was refused, the bytes did not arrive.
            // What the record was left holding is the half that matters.
            try {
                return "transfer-failed " + fields(store_.load(g_ids[alias]));
            } catch (const abstraction::job::JobError& e) {
                return outcome(&e);
            }
        }
        return show(alias, nullptr);
    }

    std::string claim(const std::string& alias, const std::string& owner, long long ttl_ms) {
        try {
            const Record r = store_.claim(g_ids[alias], owner, std::chrono::milliseconds(ttl_ms));
            g_epochs[owner] = r.lease.epoch;
        } catch (const abstraction::job::JobError& e) {
            return show(alias, &e);
        }
        return show(alias, nullptr);
    }

    // A verb no driver has is a verb no scenario can reach, and a divergence a
    // scenario cannot reach is one no harness will ever report. This was the
    // only store operation the scenario language could not say.
    std::string renew(const std::string& alias, const std::string& owner, long long ttl_ms) {
        return attempt(alias, [&] {
            store_.renew(g_ids[alias], g_epochs[owner], std::chrono::milliseconds(ttl_ms));
        });
    }

    std::string progress(const std::string& alias, const std::string& owner, std::int64_t done,
                         const std::string& checkpoint) {
        return attempt(alias, [&] {
            store_.update(g_ids[alias], g_epochs[owner], [&](Record& r) {
                r.progress.done = done;
                r.progress.updated_at = abstraction::job::Clock::now();
                if (!checkpoint.empty()) {
                    r.checkpoint = Json::parse(checkpoint);
                }
            });
        });
    }

    // Keeps the machine awake for the lease the record carries right now, the
    // way a runner does for the lease it just claimed.
    std::string hold(const std::string& alias) {
        return attempt(alias, [&] {
            g_holds[g_ids[alias]] = std::make_unique<abstraction::job::KeepAwake>(store_, store_.load(g_ids[alias]));
        });
    }

    std::string release(const std::string& alias, const std::string& owner) {
        return attempt(alias, [&] { store_.release(g_ids[alias], g_epochs[owner]); });
    }

    std::string finish(const std::string& alias, const std::string& owner,
                       const std::string& state) {
        return attempt(alias, [&] {
            store_.update(g_ids[alias], g_epochs[owner], [&](Record& r) {
                if (!abstraction::job::is_valid_state(state)) {
                    throw abstraction::job::Invalid("state \"" + state + "\"");
                }
                r.state = state;
            });
        });
    }

    std::string intent(const std::string& alias, const std::string& want) {
        return attempt(alias, [&] { store_.set_intent(g_ids[alias], want, "replay"); });
    }

    // Issued against the epoch the named owner holds, which is the epoch an
    // issuer would have read off the record: naming an owner with no epoch is a
    // recall decided against a holding that never existed.
    std::string recall(const std::string& alias, const std::string& owner, long long grace_ms,
                       const std::string& reason) {
        return attempt(alias, [&] {
            store_.recall(g_ids[alias], g_epochs[owner], reason, "replay",
                          std::chrono::milliseconds(grace_ms));
        });
    }

    std::string orphans() {
        std::map<std::string, std::string> by_id;
        for (const auto& kv : g_ids) {
            by_id[kv.second] = kv.first;
        }
        std::vector<std::string> names;
        for (const Record& r : store_.orphans()) {
            const auto it = by_id.find(r.id);
            if (it != by_id.end()) {
                names.push_back(it->second);
            }
        }
        std::sort(names.begin(), names.end());
        if (names.empty()) {
            return "ok -";
        }
        std::string joined;
        for (const std::string& n : names) {
            joined += (joined.empty() ? "" : " ") + n;
        }
        return "ok " + joined;
    }

    // One request in the drop folder, spelled the way a person would spell it.
    // `text` replaces the whole line, for a request that is not one.
    std::string drop(const std::string& name, const std::map<std::string, std::string>& a) {
        const std::int64_t size = a.count("size") ? std::stoll(flag(a, "size")) : 0;
        const std::string body = artifact(size);
        std::string line = flag(a, "text");
        if (line.empty()) {
            const std::string src = flag(a, "src");
            if (src == "file") {
                line = "file:///" + abstraction::download::portable(work_) + "/artifact.bin";
            } else if (src.rfind("http:", 0) == 0) {
                line = fixture() + "/" + src.substr(5) + "/" + std::to_string(size);
            } else {
                line = src;
            }
            const std::string digest = flag(a, "digest");
            if (digest == "good") {
                line += " " + abstraction::download::sha256_of(body.data(), body.size());
            } else if (digest == "bad") {
                line += " sha256:" + std::string(64, '0');
            }
            if (!flag(a, "dest").empty()) {
                line += " " + flag(a, "dest");
            }
        }
        const std::string dir = work_ + "/store/wanted";
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        std::ofstream out(dir + "/" + name, std::ios::binary | std::ios::trunc);
        out << line << "\n";
        return out ? "ok" : "refused";
    }

    // One pass of the drop folder, then what a person sees in it. An accepted
    // request's first job becomes the alias, so the scenario can run it.
    std::string sweep() {
        abstraction::download::Wanted w(store_, [this](const abstraction::download::Spec& s) {
            return abstraction::download::submit(store_, s);
        });
        try {
            w.answer();
            w.take_in();
        } catch (const abstraction::job::JobError& e) {
            return outcome(&e);
        }
        std::vector<std::string> seen;
        for (const auto& e : std::filesystem::directory_iterator(w.dir())) {
            const std::string entry = e.path().filename().string();
            const auto dot = entry.find('.');
            const std::string name = entry.substr(0, dot);
            const std::string state = dot == std::string::npos ? "" : entry.substr(dot + 1);
            seen.push_back(name + "=" + state);
            if (state != "accepted") continue;
            std::ifstream in(e.path(), std::ios::binary);
            for (std::string line; std::getline(in, line);) {
                const auto arrow = line.find(" -> ");
                if (line.rfind("# job ", 0) == 0 && arrow != std::string::npos) {
                    g_ids[name] = line.substr(6, arrow - 6);
                    break;
                }
            }
        }
        std::sort(seen.begin(), seen.end());
        std::string joined;
        for (const std::string& s : seen) joined += (joined.empty() ? "" : " ") + s;
        return "ok " + joined;
    }

    std::string work_;
    FileStore store_;
};

}  // namespace

int main(int argc, char** argv) {
#ifdef _WIN32
    _setmode(_fileno(stdout), _O_BINARY);
#endif
    if (argc == 2 && std::string(argv[1]) == "--capabilities") {
        std::cout << capabilities() << "\n";
        return 0;
    }
    if (argc == 2 && std::string(argv[1]) == "--models") {
        std::cout << models();
        return 0;
    }
    if (argc != 3) {
        std::cerr << "usage: replay <workdir> <scenario> | replay --capabilities | --models\n";
        return 2;
    }
    const std::string work = argv[1];
    std::ifstream script(argv[2]);
    if (!script) {
        std::cerr << "replay: cannot read " << argv[2] << "\n";
        return 1;
    }

    Replay replay(work);
    std::string raw;
    int n = 0;
    while (std::getline(script, raw)) {
        while (!raw.empty() && (raw.back() == '\r' || raw.back() == ' ')) {
            raw.pop_back();
        }
        const auto first = raw.find_first_not_of(" \t");
        if (first == std::string::npos) {
            continue;
        }
        const std::string line = raw.substr(first);
        if (line[0] == '#') {
            continue;
        }
        std::vector<std::string> f;
        std::istringstream words(line);
        std::string w;
        while (words >> w) {
            f.push_back(w);
        }
        ++n;
        std::printf("%02d %s -> %s\n", n, line.c_str(), replay.run_line(f).c_str());
    }
    return 0;
}
