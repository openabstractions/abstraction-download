// Discovery over local IPC, in C++ — the client half only.
//
// The contract lives in another repository, because it is shared by every
// implementation of it:
//
//   https://github.com/openabstractions/abstractions/blob/main/docs/discovery-ipc.md
//   https://github.com/openabstractions/abstractions/blob/main/docs/service-topology.md
//
// This
// is written against those two documents and against nothing else: it does not
// read the Python client in abstraction-download, and that one does not read
// this. Where the two agree, the contract said enough. Where they had to agree
// by accident, that is a hole in the contract and is written up rather than
// quietly patched over here.
//
//     using namespace abstraction::discovery;
//     switch (ask(store_root, "abstraction.downloads")) {
//         case Answer::Present:      /* hand the work over            */ break;
//         case Answer::Incompatible: /* a NEWER supervisor. Do NOT    */ break;
//         case Answer::Absent:       /* nobody listening. The ordinary
//                                       case, and not an error        */ break;
//     }
//
// Three things this deliberately is not:
//
//   * Not a server. Only a supervisor listens and the supervisor is Go, so
//     asking never creates an endpoint and there is no listener in this file.
//   * Not a source of exceptions. Absence is what a machine without a
//     supervisor looks like, which is most machines; a caller forced to write a
//     try/catch has been handed an error where the answer was "no". Nothing
//     here throws, including on a malformed registry or a hostile response.
//   * Not a cache. `present` was true for the length of a connection that is
//     already closed.
//
// # Why this lives in its own library and not in abstraction::job
//
// The contract says discovery belongs to the download layer and that nothing in
// `job` may depend on it, so this lives under download/cpp as
// `abstraction::discovery`, in a separate target that `abstraction_job` does
// not link and that does not link `abstraction_job`. The dependency the
// contract forbids therefore cannot be formed by accident.

#pragma once

#include <chrono>
#include <cstddef>
#include <set>
#include <string>

namespace abstraction {
namespace discovery {

// The three answers. Two would be one too few: a supervisor newer than this
// code is neither "there is nothing here" nor "something went wrong".
enum class Answer { Absent, Present, Incompatible };

// The word the contract uses, for a log line or a test failure.
const char* to_string(Answer answer);

// The registry, service name to endpoint. service-topology.md gives the JSON
// but never names the file that holds it; this is that decision, and it has to
// be the same decision in every implementation or two correct clients read two
// different files and both report absent.
extern const char* const kRegistryFile;  // "services.json"

// The one extension name this client understands. Anything else appearing in a
// response's `critical` is a newer supervisor: incompatible, not absent.
extern const char* const kBaseFeature;  // "abstraction.discovery/base@1"

// One deadline, across connect, write and read together.
constexpr int kDeadlineMs = 200;

// A line over 64 KiB is absent. The cap is not tidiness: a hostile local
// process that can reach the endpoint would otherwise feed unbounded bytes to a
// client that was only ever going to read one object.
constexpr std::size_t kMaxLine = 64 * 1024;

struct Options {
    std::chrono::milliseconds deadline{kDeadlineMs};
    std::set<std::string> known_critical{"abstraction.discovery/base@1"};
};

// The endpoint name a service is published under, or "" for absent.
//
// The name is 128 random bits in hex, chosen by whoever bound it, and is NOT
// derived from the store path. Deriving it looks tidy and is a trap: case
// folding, `\\?\` prefixes, 8.3 short names, a mapped drive against its UNC,
// symlinks under /private and HFS+ normalisation each produce two names for one
// store, and every one of them fails by silently reporting absent. Reading the
// registry is therefore what confers the reference.
std::string endpoint_for(const std::string& store_root, const std::string& service);

// Where an endpoint name lives on this platform: a named pipe on Windows, a
// unix socket under XDG_RUNTIME_DIR (else /tmp/abstraction-<uid>/) elsewhere.
// Never an abstract socket — those carry no permissions at all and are
// per-netns, so a container or Flatpak adopter could not reach a host
// supervisor.
std::string endpoint_path(const std::string& name);

// Resolve, connect, ask "who", answer. Never throws, never blocks past the
// deadline, never caches.
Answer ask(const std::string& store_root, const std::string& service,
           const Options& options = Options());

}  // namespace discovery
}  // namespace abstraction
