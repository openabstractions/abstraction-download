#include <abstraction/discovery/client.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <abstraction/json/value.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
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

int remaining_ms(Deadline deadline) {
    const auto left =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
    if (left <= 0) return 0;
    return static_cast<int>(left);
}

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

// ------------------------------------------------------------------ windows ---
//
// Everything in this block exists because of one sentence in the contract, and
// the sentence is right: a synchronous read against a server that accepts and
// never writes hangs forever. There is no timeout above it that helps. The
// pipe's own default timeout applies to WaitNamedPipe — to waiting for a BUSY
// instance — and has nothing to do with how long a read may take. A worker
// thread does not help either: a thread blocked in a synchronous ReadFile on a
// pipe cannot be cancelled, so abandoning it leaks a thread and a handle per
// call and leaves the process unable to exit. FILE_FLAG_OVERLAPPED plus
// CancelIoEx is the only correct answer.

#ifdef _WIN32

class PipeHandle {
public:
    explicit PipeHandle(HANDLE h) : h_(h) {}
    ~PipeHandle() {
        if (h_ != INVALID_HANDLE_VALUE) ::CloseHandle(h_);
    }
    PipeHandle(const PipeHandle&) = delete;
    PipeHandle& operator=(const PipeHandle&) = delete;
    HANDLE get() const { return h_; }
    bool valid() const { return h_ != INVALID_HANDLE_VALUE; }

private:
    HANDLE h_;
};

// One ReadFile or WriteFile that honours the deadline. Returns false to give up.
//
// The shape that matters: issue the operation; if it comes back
// ERROR_IO_PENDING, wait on the event for the REMAINING budget only. On timeout
// CancelIoEx, and then wait for the cancellation to actually land before the
// OVERLAPPED and the buffer go out of scope — the kernel may write into both
// until the operation completes, and letting them die first corrupts stack that
// is no longer ours. That last wait is the step a first attempt always omits.
template <typename Fn>
bool overlapped_io(Fn&& fn, HANDLE handle, void* buffer, DWORD bytes, Deadline deadline,
                   DWORD* moved) {
    OVERLAPPED ov{};
    ov.hEvent = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);
    if (ov.hEvent == nullptr) return false;

    bool ok = fn(handle, buffer, bytes, moved, &ov) != 0;
    if (!ok) {
        const DWORD err = ::GetLastError();
        if (err != ERROR_IO_PENDING) {
            ::CloseHandle(ov.hEvent);
            return false;  // including ERROR_BROKEN_PIPE, which is EOF
        }
        if (::WaitForSingleObject(ov.hEvent, remaining_ms(deadline)) != WAIT_OBJECT_0) {
            ::CancelIoEx(handle, &ov);
            // bWait = TRUE: block until the cancelled operation is genuinely
            // finished with our buffer. It returns promptly.
            ::GetOverlappedResult(handle, &ov, moved, TRUE);
            ::CloseHandle(ov.hEvent);
            return false;
        }
        ok = ::GetOverlappedResult(handle, &ov, moved, FALSE) != 0;
    }
    ::CloseHandle(ov.hEvent);
    return ok;
}

// CreateFile on the pipe, with rule 8's single retry on ERROR_PIPE_BUSY.
//
// Busy is not absent. It means every instance of a pipe that DOES exist is
// currently talking to somebody, and reporting absent there makes a supervisor
// look dead precisely when it is busiest.
HANDLE open_pipe(const std::string& path, Deadline deadline) {
    const std::wstring wide(path.begin(), path.end());  // the name is hex ASCII
    const DWORD flags =
        FILE_FLAG_OVERLAPPED | SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION;
    for (int attempt = 0; attempt < 2; ++attempt) {
        const HANDLE h = ::CreateFileW(wide.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr,
                                       OPEN_EXISTING, flags, nullptr);
        if (h != INVALID_HANDLE_VALUE) return h;
        if (::GetLastError() != ERROR_PIPE_BUSY || attempt == 1) return INVALID_HANDLE_VALUE;
        const int left = remaining_ms(deadline);
        // Wait inside the remaining budget, never beyond it.
        if (left <= 0 || !::WaitNamedPipeW(wide.c_str(), static_cast<DWORD>(left))) {
            return INVALID_HANDLE_VALUE;
        }
    }
    return INVALID_HANDLE_VALUE;
}

bool exchange(const std::string& path, Deadline deadline, std::string* line) {
    PipeHandle pipe(open_pipe(path, deadline));
    if (!pipe.valid()) return false;  // nothing listening, or a name long gone

    std::size_t sent = 0;
    while (sent < kRequestLen) {
        if (remaining_ms(deadline) <= 0) return false;
        DWORD moved = 0;
        if (!overlapped_io(&::WriteFile, pipe.get(), const_cast<char*>(kRequest + sent),
                           static_cast<DWORD>(kRequestLen - sent), deadline, &moved) ||
            moved == 0) {
            return false;
        }
        sent += moved;
    }

    std::string buffer;
    char chunk[4096];
    for (;;) {
        if (remaining_ms(deadline) <= 0) return false;
        DWORD moved = 0;
        if (!overlapped_io(&::ReadFile, pipe.get(), chunk, sizeof(chunk), deadline, &moved)) {
            return false;
        }
        if (moved == 0) return false;  // EOF before the newline
        buffer.append(chunk, moved);
        if (line_from(buffer, line)) return true;
        if (buffer.size() > kMaxLine) return false;  // over the cap, still no newline
    }
}

#else

// -------------------------------------------------------------------- posix ---
//
// A unix socket can be given a real timeout, but only if it is non-blocking and
// every wait goes through poll(). SO_RCVTIMEO would time out each read
// separately, which is three budgets, not one — a server that stalls each phase
// in turn would get 600 ms out of a client that promised 200.

class Fd {
public:
    explicit Fd(int fd) : fd_(fd) {}
    ~Fd() {
        if (fd_ >= 0) ::close(fd_);
    }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    int get() const { return fd_; }

private:
    int fd_;
};

// A write to a socket the peer has closed raises SIGPIPE, and the default
// disposition kills the process. A discovery call that terminates its caller
// because a supervisor exited mid-handshake is the loudest possible version of
// the failure rule 1 says must be silent. Linux spells the fix MSG_NOSIGNAL on
// the send; macOS has no such flag and spells it SO_NOSIGPIPE on the socket.
#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

void suppress_sigpipe(int fd) {
#if defined(SO_NOSIGPIPE)
    const int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#else
    (void)fd;
#endif
}

bool wait_ready(int fd, short events, Deadline deadline) {
    const int left = remaining_ms(deadline);
    if (left <= 0) return false;
    struct pollfd pfd {};
    pfd.fd = fd;
    pfd.events = events;
    for (;;) {
        const int rc = ::poll(&pfd, 1, remaining_ms(deadline));
        if (rc > 0) return true;
        if (rc == 0) return false;                 // the deadline
        if (errno != EINTR) return false;
        if (remaining_ms(deadline) <= 0) return false;  // a signal, then the deadline
    }
}

bool exchange(const std::string& path, Deadline deadline, std::string* line) {
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    // Unix socket paths are capped near 104 bytes. A path that does not fit is
    // not an endpoint this client can reach; the supervisor is required to fail
    // loudly at startup rather than bind something shorter.
    if (path.size() >= sizeof(addr.sun_path)) return false;
    std::memcpy(addr.sun_path, path.c_str(), path.size());

    Fd sock(::socket(AF_UNIX, SOCK_STREAM, 0));
    if (sock.get() < 0) return false;
    suppress_sigpipe(sock.get());
    const int flags = ::fcntl(sock.get(), F_GETFL, 0);
    if (flags < 0 || ::fcntl(sock.get(), F_SETFL, flags | O_NONBLOCK) < 0) return false;

    if (::connect(sock.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        // ECONNREFUSED here is a stale socket file left by a killed process, and
        // it is absent. Rule 6: the client does NOT unlink it. Removing a path a
        // supervisor bound a millisecond ago leaves it listening on an unlinked
        // inode and undiscoverable, which is worse than the bug being fixed.
        if (errno != EINPROGRESS) return false;
        if (!wait_ready(sock.get(), POLLOUT, deadline)) return false;
        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(sock.get(), SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0) {
            return false;
        }
    }

    std::size_t sent = 0;
    while (sent < kRequestLen) {
        if (!wait_ready(sock.get(), POLLOUT, deadline)) return false;
        const ssize_t n = ::send(sock.get(), kRequest + sent, kRequestLen - sent, kSendFlags);
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        return false;
    }

    std::string buffer;
    char chunk[4096];
    for (;;) {
        if (!wait_ready(sock.get(), POLLIN, deadline)) return false;
        const ssize_t n = ::recv(sock.get(), chunk, sizeof(chunk), 0);
        if (n == 0) return false;  // EOF before the newline
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
            return false;
        }
        buffer.append(chunk, static_cast<std::size_t>(n));
        if (line_from(buffer, line)) return true;
        if (buffer.size() > kMaxLine) return false;
    }
}

#endif

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
