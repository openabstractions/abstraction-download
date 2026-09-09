// Tests for the discovery client, against a fake supervisor in this process.
//
// The real server is Go and lives in the supervisor, so nothing here can wait
// for it. Each test stands up a listener on the endpoint the contract names — a
// real named pipe on Windows, a real unix socket elsewhere — and drives the
// client through every answer the contract distinguishes.
//
// The load-bearing one is test_accepts_and_never_writes. A synchronous client
// passes every other test in this file and hangs forever on that one. That is
// the failure rule 2 exists to prevent, it is invisible until somebody runs it
// on Windows against a real pipe, and a suite without it proves nothing.

#include <abstraction/discovery/client.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>

#include <abstraction/json/value.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;
using namespace abstraction::discovery;
using Json = abstraction::json::Value;

static int g_failures = 0;

static void check(const char* name, bool ok) {
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", name);
    if (!ok) ++g_failures;
}

static void check_answer(const char* name, Answer got, Answer want) {
    const bool ok = got == want;
    std::printf("[%s] %s (want %s, got %s)\n", ok ? "PASS" : "FAIL", name, to_string(want),
                to_string(got));
    if (!ok) ++g_failures;
}

static std::string random_name() {
    static std::mt19937_64 rng(std::random_device{}());
    char buf[48];
    std::snprintf(buf, sizeof(buf), "abstraction-%016llx%016llx",
                  static_cast<unsigned long long>(rng()),
                  static_cast<unsigned long long>(rng()));
    return buf;
}

// The response the contract prints, with whatever a test wants changed.
static Json good_response(const std::string& store) {
    return Json{
        {"content", Json::array({"abstraction.discovery/base@1"})},
        {"critical", Json::array({"abstraction.discovery/base@1"})},
        {"owner", "jobd@host:9242"},
        {"host", "host"},
        {"store", store},
        {"delegates_to", "here"},
        {"pid", 9242},
        {"started_at", "2026-09-05T06:34:49.123456Z"},
    };
}

static std::string line_of(const Json& doc) { return doc.dump() + "\n"; }

// ------------------------------------------------------------ fake server ---

// One connection's worth of supervisor. An empty `reply` means accept, read the
// request, and then never write anything — the case a naive client never comes
// back from.
class FakeSupervisor {
public:
    FakeSupervisor(const std::string& name, std::string reply, bool ever_writes = true)
        : path_(endpoint_path(name)), reply_(std::move(reply)), ever_writes_(ever_writes) {
        open();
        thread_ = std::thread([this] { serve(); });
    }

    ~FakeSupervisor() { close(); }

    const std::string& request() const { return request_; }

private:
    std::string path_;
    std::string reply_;
    bool ever_writes_;
    std::string request_;
    std::thread thread_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_ = false;

    void wait_for_stop() {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return stop_; });
    }

    void signal_stop() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
    }

#ifdef _WIN32
    HANDLE pipe_ = INVALID_HANDLE_VALUE;

    void open() {
        const std::wstring wide(path_.begin(), path_.end());
        pipe_ = ::CreateNamedPipeW(wide.c_str(), PIPE_ACCESS_DUPLEX,
                                   PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                                   PIPE_UNLIMITED_INSTANCES, 4096, 4096, 0, nullptr);
        if (pipe_ == INVALID_HANDLE_VALUE) {
            std::printf("CreateNamedPipeW failed: %lu\n", ::GetLastError());
            std::abort();
        }
    }

    void serve() {
        if (!::ConnectNamedPipe(pipe_, nullptr) && ::GetLastError() != ERROR_PIPE_CONNECTED) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stop_) return;
        }
        char buf[4096];
        DWORD moved = 0;
        if (::ReadFile(pipe_, buf, sizeof(buf), &moved, nullptr)) {
            request_.assign(buf, moved);
        }
        if (!ever_writes_) {
            wait_for_stop();
        } else {
            std::size_t sent = 0;
            while (sent < reply_.size()) {
                if (!::WriteFile(pipe_, reply_.data() + sent,
                                 static_cast<DWORD>(reply_.size() - sent), &moved, nullptr)) {
                    break;
                }
                sent += moved;
            }
            // Rule 8: flush before disconnecting, or the last bytes can be
            // discarded along with the connection.
            ::FlushFileBuffers(pipe_);
        }
        ::DisconnectNamedPipe(pipe_);
    }

    void close() {
        signal_stop();
        // Unblock a thread still sitting in ConnectNamedPipe. A failure here
        // means it is already connected, which is equally fine.
        const std::wstring wide(path_.begin(), path_.end());
        const HANDLE poke =
            ::CreateFileW(wide.c_str(), GENERIC_READ, 0, nullptr, OPEN_EXISTING, 0, nullptr);
        if (poke != INVALID_HANDLE_VALUE) ::CloseHandle(poke);
        if (thread_.joinable()) thread_.join();
        if (pipe_ != INVALID_HANDLE_VALUE) ::CloseHandle(pipe_);
    }
#else
    int listener_ = -1;

    void open() {
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        if (path_.size() >= sizeof(addr.sun_path)) std::abort();
        std::memcpy(addr.sun_path, path_.c_str(), path_.size());
        fs::create_directories(fs::path(path_).parent_path());
        ::chmod(fs::path(path_).parent_path().c_str(), 0700);
        listener_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
        if (listener_ < 0) std::abort();
        ::unlink(path_.c_str());  // only the SERVER unlinks, and only before bind
        if (::bind(listener_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) std::abort();
        ::chmod(path_.c_str(), 0600);  // rule 7
        ::listen(listener_, 4);
    }

    void serve() {
        const int conn = ::accept(listener_, nullptr, nullptr);
        if (conn < 0) return;
        char buf[4096];
        const ssize_t n = ::recv(conn, buf, sizeof(buf), 0);
        if (n > 0) request_.assign(buf, static_cast<std::size_t>(n));
        if (!ever_writes_) {
            wait_for_stop();
        } else {
            std::size_t sent = 0;
            while (sent < reply_.size()) {
                const ssize_t w = ::send(conn, reply_.data() + sent, reply_.size() - sent, 0);
                if (w <= 0) break;
                sent += static_cast<std::size_t>(w);
            }
        }
        ::close(conn);
    }

    void close() {
        signal_stop();
        if (listener_ >= 0) ::shutdown(listener_, SHUT_RDWR);
        if (listener_ >= 0) ::close(listener_);
        if (thread_.joinable()) thread_.join();
        ::unlink(path_.c_str());
    }
#endif
};

// ------------------------------------------------------------------ store ---

class TempStore {
public:
    TempStore() {
        const auto stamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
        root_ = fs::temp_directory_path() / ("abstraction-disco-" + std::to_string(stamp));
        fs::create_directories(root_);
#ifndef _WIN32
        // Keep the socket path inside the 104-byte cap and away from whatever
        // else is on the machine.
        ::setenv("XDG_RUNTIME_DIR", root_.string().c_str(), 1);
#endif
    }
    ~TempStore() {
        std::error_code ec;
        fs::remove_all(root_, ec);
    }

    std::string path() const { return root_.string(); }

    void publish(const std::string& service, const std::string& endpoint) {
        Json services = Json::object();
        std::ifstream in(root_ / kRegistryFile, std::ios::binary);
        if (in) {
            std::ostringstream body;
            body << in.rdbuf();
            bool parsed = false;
            const Json existing = Json::parse(body.str(), &parsed);
            if (parsed && existing.is_object() && existing.contains("services")) {
                services = existing["services"];
            }
        }
        services[service] = endpoint;
        std::ofstream out(root_ / kRegistryFile, std::ios::binary);
        out << Json{{"services", services}}.dump();
    }

private:
    fs::path root_;
};

static const char* kService = "abstraction.downloads";

// ------------------------------------------------------------------ tests ---

static void test_absent_without_a_registry() {
    TempStore store;
    check_answer("absent: no registry at all", ask(store.path(), kService), Answer::Absent);

    store.publish("abstraction.rights", random_name());
    check_answer("absent: the service is not in the registry", ask(store.path(), kService),
                 Answer::Absent);
}

static void test_absent_when_nothing_listens() {
    TempStore store;
    const std::string name = random_name();
    store.publish(kService, name);

    const auto started = std::chrono::steady_clock::now();
    const Answer answer = ask(store.path(), kService);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    check_answer("absent: a registry entry with no listener", answer, Answer::Absent);
    check("absent: and it comes back promptly", elapsed < 1000);

    // Rule 4: asking never creates an endpoint.
#ifdef _WIN32
    const std::string path = endpoint_path(name);
    const std::wstring wide(path.begin(), path.end());
    const HANDLE h =
        ::CreateFileW(wide.c_str(), GENERIC_READ, 0, nullptr, OPEN_EXISTING, 0, nullptr);
    check("absent: asking created no pipe", h == INVALID_HANDLE_VALUE);
    if (h != INVALID_HANDLE_VALUE) ::CloseHandle(h);
#else
    check("absent: asking created no socket", !fs::exists(endpoint_path(name)));
#endif
}

static void test_stale_endpoint() {
    TempStore store;
    const std::string name = random_name();
    store.publish(kService, name);
    const std::string path = endpoint_path(name);

#ifndef _WIN32
    // The corpse a killed process leaves behind.
    fs::create_directories(fs::path(path).parent_path());
    { std::ofstream(path, std::ios::binary); }
    check_answer("stale: a leftover socket file is absent", ask(store.path(), kService),
                 Answer::Absent);
    // Rule 6: only the SERVER unlinks. A client that tidies up can remove a
    // socket a supervisor bound a millisecond earlier, leaving it listening on
    // an unlinked inode and undiscoverable — worse than the bug being fixed.
    check("stale: and the client did not unlink it", fs::exists(path));
#else
    // A killed pipe server leaves nothing behind at all, so the stale case on
    // Windows is a registry entry naming a pipe that no longer exists.
    check_answer("stale: a registry entry for a dead pipe is absent",
                 ask(store.path(), kService), Answer::Absent);
#endif
}

static void test_malformed_answers_are_absent() {
    {
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        FakeSupervisor server(name, "{this is not json\n");
        check_answer("absent: malformed JSON", ask(store.path(), kService), Answer::Absent);
    }
    {
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        // A partial line: everything but the 0x0A, then the connection closes.
        FakeSupervisor server(name, good_response(store.path()).dump());
        check_answer("absent: EOF before the newline", ask(store.path(), kService),
                     Answer::Absent);
    }
    {
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        Json doc = good_response(store.path());
        doc["padding"] = std::string(70000, 'x');
        FakeSupervisor server(name, line_of(doc));
        check_answer("absent: a line over the 64 KiB cap", ask(store.path(), kService),
                     Answer::Absent);
    }
    {
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        FakeSupervisor server(name, "{\"error\":\"unknown ask\"}\n");
        check_answer("absent: an error object", ask(store.path(), kService), Answer::Absent);
    }
}

static void test_store_mismatch() {
    {
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        FakeSupervisor server(name, line_of(good_response("/somebody/elses/store")));
        check_answer("absent: the store does not match", ask(store.path(), kService),
                     Answer::Absent);
    }
    {
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        Json doc = good_response(store.path());
        doc.erase("store");
        FakeSupervisor server(name, line_of(doc));
        check_answer("absent: no store field at all", ask(store.path(), kService),
                     Answer::Absent);
    }
    {
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
#ifdef _WIN32
        FakeSupervisor server(name, line_of(good_response(store.path() + "\\")));
#else
        FakeSupervisor server(name, line_of(good_response(store.path() + "/")));
#endif
        check_answer("present: a trailing separator still matches", ask(store.path(), kService),
                     Answer::Present);
    }
    {
        // A supervisor for a different store is not this caller's supervisor,
        // so its extension names say nothing about ours.
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        Json doc = good_response("/elsewhere");
        doc["critical"] = Json::array({"unknown@1"});
        FakeSupervisor server(name, line_of(doc));
        check_answer("absent: a store mismatch beats an unknown critical name",
                     ask(store.path(), kService), Answer::Absent);
    }
}

static void test_present() {
    {
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        FakeSupervisor server(name, line_of(good_response(store.path())));
        check_answer("present: the canonical response", ask(store.path(), kService),
                     Answer::Present);
        check("present: the request was exactly one framed object",
              server.request() == "{\"ask\":\"who\"}\n");
    }
    {
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        Json doc = good_response(store.path());
        doc.erase("critical");
        FakeSupervisor server(name, line_of(doc));
        check_answer("present: no critical list", ask(store.path(), kService), Answer::Present);
    }
    {
        // delegates_to is advisory; a client must work without understanding it.
        TempStore store;
        const std::string name = random_name();
        store.publish(kService, name);
        Json doc = good_response(store.path());
        doc["delegates_to"] = "something-invented-tomorrow";
        doc["content"] = Json::array({"something/new@9"});  // content is advisory too
        FakeSupervisor server(name, line_of(doc));
        check_answer("present: advisory fields nobody understands", ask(store.path(), kService),
                     Answer::Present);
    }
    {
        // service-topology.md's whole mechanism: one process serving two
        // services publishes ONE endpoint under two names, and no client can
        // tell that from two processes.
        TempStore store;
        const std::string name = random_name();
        store.publish("abstraction.jobs", name);
        store.publish("abstraction.downloads", name);
        FakeSupervisor server(name, line_of(good_response(store.path())));
        check_answer("present: two service names, one endpoint",
                     ask(store.path(), "abstraction.jobs"), Answer::Present);
    }
}

static void test_incompatible() {
    TempStore store;
    const std::string name = random_name();
    store.publish(kService, name);
    Json doc = good_response(store.path());
    doc["critical"] =
        Json::array({"abstraction.discovery/base@1", "abstraction.discovery/leases@2"});
    FakeSupervisor server(name, line_of(doc));
    // Not absent. This is a NEWER supervisor: the caller must not hand work
    // over, and it is not an error to report either.
    check_answer("incompatible: a critical name this client does not know",
                 ask(store.path(), kService), Answer::Incompatible);
}

static void test_accepts_and_never_writes() {
    // The reason rule 2 says overlapped I/O, and the only test in this file that
    // a synchronous implementation cannot pass. It does not fail against a
    // naive client — it never returns.
    TempStore store;
    const std::string name = random_name();
    store.publish(kService, name);
    FakeSupervisor server(name, "", /*ever_writes=*/false);

    const auto started = std::chrono::steady_clock::now();
    const Answer answer = ask(store.path(), kService);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    std::printf("      (abandoned after %lld ms)\n", static_cast<long long>(elapsed));
    check_answer("hang: a server that accepts and never writes", answer, Answer::Absent);
    check("hang: the read was abandoned, not waited out", elapsed < 1000);
    check("hang: and not abandoned before the deadline", elapsed >= 150);
}

static void test_one_budget_not_one_per_phase() {
    // Rule 2 again: connect, write and read share 200 ms, so a stalled read
    // cannot be added to a stalled connect.
    TempStore store;
    const std::string name = random_name();
    store.publish(kService, name);
    FakeSupervisor server(name, "", /*ever_writes=*/false);

    Options opts;
    opts.deadline = std::chrono::milliseconds(200);
    const auto started = std::chrono::steady_clock::now();
    ask(store.path(), kService, opts);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    check("deadline: one budget covers every phase", elapsed < 500);
}

int main() {
    // Unbuffered, so a crash still says which check it died after. A piped
    // stdout is fully buffered, and a test that dies mid-run then prints
    // nothing at all.
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    test_absent_without_a_registry();
    test_absent_when_nothing_listens();
    test_stale_endpoint();
    test_malformed_answers_are_absent();
    test_store_mismatch();
    test_present();
    test_incompatible();
    test_accepts_and_never_writes();
    test_one_budget_not_one_per_phase();

    std::printf("\n%s: %d failure(s)\n", g_failures == 0 ? "OK" : "FAILED", g_failures);
    return g_failures == 0 ? 0 : 1;
}
