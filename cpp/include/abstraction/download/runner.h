// The runner: claim a job, get the bytes, prove them, deliver them, decide how
// it ended.
//
// Everything that must be identical across implementations is here rather than
// in a Fetcher — hashing, the resume point, progress persistence, lease renewal,
// honouring an intent, the final rename. A transfer begun in Go and finished
// here has to reach the same record, so the decisions live where every language
// makes them the same way.

#ifndef ABSTRACTION_DOWNLOAD_RUNNER_H
#define ABSTRACTION_DOWNLOAD_RUNNER_H

#include <abstraction/download/fetcher.h>
#include <abstraction/job/store.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace abstraction {
namespace download {

// The job kind this package understands. A process meeting an unknown kind
// leaves it alone rather than guessing at its spec.
constexpr const char* kKind = "download";

struct Artifact {
    std::string digest;
    std::int64_t size = 0;
};

struct Sink {
    std::string partial;
    std::string final_path;
};

struct Spec {
    Artifact artifact;
    std::vector<Source> sources;  // in the order they would be tried
    Sink sink;
};

// The host a locator opens a connection to, lowercased and without a port, or
// "" when it names none: a local path reaches nothing.
std::string host_of(const std::string& locator);

// Reads the download spec out of a record. Unknown keys are ignored at every
// level: the spec is payload the layer above extends, unlike the record, which
// refuses a field it does not know.
Spec spec_of(const job::Record& r);
Spec spec_from(const job::Json& d);

// The spec as a record carries it.
job::Json spec_json(const Spec& s);

// The refusal a spec earns from the job that would carry it, or "".
std::string invalid(const Spec& s, const std::string& owner);

// A new job carrying the spec; job::Invalid when the spec is refused.
std::string submit(job::Store& store, Spec spec);

// The digest of some bytes, as "sha256:" + 64 lowercase hex — the form the
// record carries. Exported because a caller that takes delivery of a file a
// delegate wrote has to be able to check it: BITS verifies size and timestamp,
// not content, and says so in its own documentation.
std::string sha256_of(const void* data, std::size_t n);

class Runner {
public:
    Runner(job::Store& store, std::string owner);

    // Claims the job and takes it as far as it can. Stopping is not a
    // catastrophe: the checkpoint is on disk, the lease lapses, and the next
    // runner resumes from the last proven byte.
    void run(const std::string& id);

    Fetchers fetchers = default_fetchers();

    // Asked for every host before a connection is opened to it, at the same
    // last moment a credential would be resolved. An empty answer is yes; any
    // other is the reason, written into the record for an application to show.
    // Unset reaches everything.
    std::function<std::string(const std::string& host)> reach;

    // The store may be written by machines other than this one — a NAS share.
    // On such a store an absolute sink is refused: it names THIS machine's
    // filesystem and the record was written by somebody else. A supervisor sets
    // this; a bare runner leaves it off, which is what a caller writing to its
    // own machine wants. See unportable_sink in sink.h.
    bool shared_store = false;

    // Short on purpose: a crashed owner's job becomes available again after
    // roughly this long, and the cost of a short lease is one small file write
    // more often.
    std::chrono::milliseconds lease_ttl{30000};

    // How much may be transferred, or how long may pass, before the checkpoint
    // is written down. Bytes alone is silently wrong on a slow link — a real
    // 313 MB download killed after twelve seconds had checkpointed nothing —
    // and the interval also carries lease renewal, so a slow transfer does not
    // let its own lease expire underneath it.
    std::int64_t persist_every = 8 << 20;
    std::chrono::milliseconds persist_interval{5000};

private:
    struct Transferred {
        std::int64_t total = 0;
        std::string digest;
        Validators validators;
    };

    void execute(const job::Record& rec, std::int64_t epoch);
    Transferred transfer(const job::Record& rec, const Spec& spec, std::int64_t epoch,
                         const std::string& partial);
    void keep_proven(const std::string& id, std::int64_t epoch, std::int64_t prefix,
                     const Validators& seen);
    void honour(const std::string& want, const std::string& id, std::int64_t epoch);
    void resolve(const std::string& owner_id, const Sink& sink, std::string& partial,
                 std::string& final_path) const;

    job::Store& store_;
    std::string owner_;
};

}  // namespace download
}  // namespace abstraction

#endif  // ABSTRACTION_DOWNLOAD_RUNNER_H
