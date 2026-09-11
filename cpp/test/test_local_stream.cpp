#include <abstraction/ipc/client.hpp>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#endif
#include <cstring>
#include <atomic>
#include <cstdio>
#include <functional>
#include <thread>
#include <vector>
using namespace abstraction::ipc;
using namespace std::chrono_literals;
#ifdef _WIN32
using Native = HANDLE;
#else
using Native = int;
#endif
static int failures = 0;
static void check(const char* name, bool ok) {
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", name);
    if (!ok) ++failures;
}
static std::size_t read_bytes(Native h, char* bytes, std::size_t count) {
#ifdef _WIN32
    DWORD n = 0;
    return ::ReadFile(h, bytes, static_cast<DWORD>(count), &n, nullptr) ? n : 0;
#else
    auto n = ::recv(h, bytes, count, 0);
    return n > 0 ? static_cast<std::size_t>(n) : 0;
#endif
}
static bool write_bytes(Native h, std::string_view bytes) {
    while (!bytes.empty()) {
#ifdef _WIN32
        DWORD n = 0;
        if (!::WriteFile(h, bytes.data(), static_cast<DWORD>(bytes.size()), &n, nullptr) || !n) return false;
#else
        auto n = ::send(h, bytes.data(), bytes.size(),
#ifdef MSG_NOSIGNAL
                        MSG_NOSIGNAL
#else
                        0
#endif
                        );
        if (n <= 0) return false;
#endif
        bytes.remove_prefix(n);
    }
    return true;
}
// Listeners exist only in this fixture, never in the production stream API.
class Listener {
public:
    std::string path;
    explicit Listener(std::function<void(Native)> action) {
        static std::atomic<int> serial{0};
        auto suffix = std::to_string(Clock::now().time_since_epoch().count()) + "-" + std::to_string(++serial);
#ifdef _WIN32
        path = "\\\\.\\pipe\\oa-stream-" + suffix;
        std::wstring wide(path.begin(), path.end());
        handle = ::CreateNamedPipeW(wide.c_str(), PIPE_ACCESS_DUPLEX, PIPE_TYPE_BYTE | PIPE_WAIT, 1, 1024, 1024, 0, nullptr);
        if (handle == INVALID_HANDLE_VALUE) std::abort();
        worker = std::thread([this, action] {
            if (::ConnectNamedPipe(handle, nullptr) || ::GetLastError() == ERROR_PIPE_CONNECTED) action(handle);
            ::DisconnectNamedPipe(handle);
        });
#else
        path = "/tmp/oa-stream-" + suffix;
        handle = ::socket(AF_UNIX, SOCK_STREAM, 0);
        sockaddr_un addr{}; addr.sun_family = AF_UNIX;
        std::memcpy(addr.sun_path, path.c_str(), path.size());
        if (handle < 0 || ::bind(handle, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) || ::listen(handle, 1)) std::abort();
        worker = std::thread([this, action] {
            auto peer = ::accept(handle, nullptr, nullptr);
            if (peer >= 0) {
#ifdef SO_NOSIGPIPE
            int on = 1; ::setsockopt(peer, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
                action(peer); ::close(peer);
            }
        });
#endif
    }
    ~Listener() {
        worker.join();
#ifdef _WIN32
        ::CloseHandle(handle);
#else
        ::close(handle); ::unlink(path.c_str());
#endif
    }
private:
    Native handle;
    std::thread worker;
};
int main() {
    std::string payload;
    for (int i = 0; i < 262144; ++i) payload.push_back(static_cast<char>(i % 256));
    std::string received;
    {
        Listener server([&](Native peer) {
            char part[127];
            while (received.size() < payload.size()) {
                auto n = read_bytes(peer, part, sizeof(part));
                if (!n) return;
                received.append(part, n);
            }
            // Several writes, newline and NUL are just bytes.
            write_bytes(peer, std::string_view("a\n\0b\n", 5));
            write_bytes(peer, "tail");
            char ack; read_bytes(peer, &ack, 1); // retain pipe until reply drained
        });
        Stream client(server.path, Clock::now() + 2s);
        check("connect", client.valid());
        check("empty write", client.write_all({}));
        check("large binary send with small receiver reads", client.write_all(payload));
        std::string reply;
        char part[2];
        while (reply.size() < 9) {
            std::size_t n = 0;
            if (!client.read_some(part, sizeof(part), n)) break;
            reply.append(part, n);
        }
        check("partial reads preserve multiline and NUL", reply == std::string("a\n\0b\ntail", 9));
        client.write_all("!");
    }
    check("all 256 byte values sent unchanged", received == payload);
    {
        Listener server([](Native peer) {
            char request; read_bytes(peer, &request, 1);
            std::this_thread::sleep_for(60ms);
            write_bytes(peer, "a");
            std::this_thread::sleep_for(70ms);
            write_bytes(peer, "b");
            std::this_thread::sleep_for(70ms);
            write_bytes(peer, "c");
        });
        auto start = Clock::now();
        Stream client(server.path, start + 120ms);
        check("deadline request", client.write_all("!"));
        char part; std::size_t n = 0;
        check("first read before deadline", client.read_some(&part, 1, n) && part == 'a');
        std::string later;
        while (later.size() < 2 && client.read_some(&part, 1, n)) later.push_back(part);
        check("delayed bytes cannot renew original deadline", later.size() < 2 && n == 0 && client.status() == Status::timeout);
        check("timeout stays bounded", Clock::now() - start < 1s);
    }
    {
        Listener server([](Native) { std::this_thread::sleep_for(150ms); });
        oa_ipc_connection* client = nullptr;
        check("C ABI connect for blocked write", oa_ipc_open(server.path.data(), server.path.size(), 60, &client) == OA_IPC_OK);
        std::string large(8 * 1024 * 1024, 'x');
        size_t moved = large.size() + 1;
        check("blocked write expires and drains cancellation", oa_ipc_write(client, large.data(), large.size(), &moved) == OA_IPC_TIMEOUT);
        check("failed write reports bounded confirmed prefix", moved < large.size());
        oa_ipc_close(client);
    }
    {
        Listener server([](Native peer) { char request; read_bytes(peer, &request, 1); });
        Stream client(server.path, Clock::now() + 1s);
        client.write_all("!");
        char byte; std::size_t n;
        check("peer close is disconnected", !client.read_some(&byte, 1, n) && client.status() == Status::disconnected);
    }
    {
        Listener server([](Native peer) { char request; read_bytes(peer, &request, 1); });
        Stream client(server.path, Clock::now() + 1s);
        client.write_all("!");
        std::this_thread::sleep_for(20ms);
        // A closed Unix peer must produce failure, never process-wide SIGPIPE.
        check("closed peer write survives", !client.write_all(payload) && client.status() == Status::disconnected);
    }
    std::string expired_received;
    {
        Listener server([&](Native peer) {
            char part[16];
            auto n = read_bytes(peer, part, sizeof(part));
            expired_received.assign(part, n);
            write_bytes(peer, "!");
            read_bytes(peer, part, 1);
        });
        Stream expired_live(server.path, Clock::now() - 1ms);
        check("expired live endpoint refuses send", !expired_live.write_all("unexpected"));
        Stream control(server.path, Clock::now() + 1s);
        control.write_all("control");
        char ack; std::size_t moved;
        control.read_some(&ack, 1, moved);
        control.write_all("!");
    }
    check("expired connection transfers no bytes", expired_received == "control");
    Stream nul(std::string("unused\0suffix", 13), Clock::now() + 1s);
    check("embedded NUL endpoint rejected", !nul.valid() && nul.status() == Status::invalid_argument);
#ifdef _WIN32
    for (const auto* path : {"ordinary-file", "\\\\remote\\pipe\\x", "\\\\.\\pipe\\"}) {
        Stream invalid(path, Clock::now() + 1s);
        check("non-local or empty pipe path rejected", !invalid.valid() && invalid.status() == Status::io_error);
    }
    Stream non_ascii("\\\\.\\pipe\\name\xc3\xa9", Clock::now() + 1s);
    check("non-ASCII pipe name explicitly rejected", !non_ascii.valid() && non_ascii.status() == Status::io_error);
#endif
    Stream expired("unused", Clock::now() - 1ms);
    check("expired budget does not connect", !expired.valid() && expired.status() == Status::timeout);
    return failures ? 1 : 0;
}
