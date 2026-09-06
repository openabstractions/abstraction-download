// Something that can move bytes for one family of sources.
//
// Deliberately small and dumb. A Fetcher appends bytes and reports how many. It
// does not verify, does not retry across sources, does not decide where the file
// goes and never touches the job record — all of that lives in the Runner, so it
// is done identically whichever Fetcher ran. That is what lets a transfer begun
// by one implementation be finished by a different one.

#ifndef ABSTRACTION_DOWNLOAD_FETCHER_H
#define ABSTRACTION_DOWNLOAD_FETCHER_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace abstraction {
namespace download {

// Somewhere the bytes can be obtained from, and deliberately not a URL. An SMB
// share, a local file and another machine's store are sources too, and none of
// them is expressible as an http URL without lying.
struct Source {
    std::string scheme;
    std::string locator;
    // attrs describe the source to THIS layer and never reach the wire; headers
    // are what the caller means to send. One bag used to be both, and an
    // attribute nobody remembered to exclude went out as a header.
    std::map<std::string, std::string> attrs;
    std::map<std::string, std::string> headers;
    int priority = 0;
};

// A transfer that stopped, and whether trying again unchanged would serve any
// purpose. That bit is the whole difference between the two endings a failed
// download can have: a refusal nobody will ever satisfy must end the job, and a
// source that merely was not there must leave it adoptable.
class Error : public std::runtime_error {
public:
    Error(const std::string& what, bool permanent)
        : std::runtime_error(what), permanent_(permanent) {}
    bool permanent() const { return permanent_; }

private:
    bool permanent_;
};

// Somebody asked the job to stop while it was running. Not a failure, and not
// an Error: a person watching to see that their own button worked must not find
// "the transfer was cancelled" written into the record as the reason it stopped.
struct Stopped {
    std::string want;
};

struct Result {
    std::int64_t written = 0;
    std::int64_t total = 0;  // 0 means the source never said
};

// Which VERSION of an artifact the bytes on disk came from.
//
// Nothing in a bare Range request says which file it means, so a source that
// replaced the artifact answers the range honestly and the answer is a valid
// range of something else. Spliced onto the prefix that is a file of exactly
// the right length holding two versions: no transport error, no short read, and
// nothing a download without a digest could catch. Only strong validators go in
// here — a weak ETag asserts semantic equivalence, not byte equality, which is
// the one distinction a resume depends on.
struct Validators {
    std::string etag;
    std::string last_modified;

    bool empty() const { return etag.empty() && last_modified.empty(); }
    // If-Range rather than If-Match: a failed If-Match is a 412 with an empty
    // body and costs a second round trip, a failed If-Range is the new file in
    // the same response.
    std::string if_range() const { return etag.empty() ? last_modified : etag; }
};

struct Request {
    Source source;
    // from is where to begin; to is exclusive, and 0 means "to the end of
    // whatever the source holds".
    std::int64_t from = 0;
    std::int64_t to = 0;
    std::map<std::string, std::string> headers;

    // The version the bytes already on disk came from, sent with the range so
    // the source can answer for that version or hand back the whole new one.
    Validators validators;

    std::function<void(const char*, std::size_t)> out;

    // What the source says about the version it is serving, so the next attempt
    // can ask for the same one. On a first download this is the only chance to
    // learn it; on a resume it confirms it.
    std::function<void(const Validators&)> observed;

    // The stream about to be written begins at zero rather than at `from`,
    // because the source turned out to be serving a different artifact than the
    // one already on disk. Called before the first byte of the new stream; a
    // fetcher that cannot rewind must fail rather than append.
    std::function<void()> restart;

    std::function<void(std::int64_t written, std::int64_t total)> report;
};

// What a Fetcher promises. Bindings differ enormously and a facade that presents
// them as interchangeable lies to its caller on the tier most people run: an
// in-process GET dies with its process, and no source's scheme says so.
namespace capability {
constexpr const char* kResume = "resume";
constexpr const char* kSurvivesProcessExit = "survives_process_exit";
constexpr const char* kVerifiesContent = "verifies_content";
}  // namespace capability

class Fetcher {
public:
    virtual ~Fetcher() = default;
    virtual std::vector<std::string> schemes() const = 0;
    virtual std::vector<std::string> capabilities() const = 0;
    virtual Result fetch(const Request& req) = 0;
};

class Fetchers {
public:
    void add(std::shared_ptr<Fetcher> f) { fetchers_.push_back(std::move(f)); }
    // The first fetcher that serves this scheme and has every capability the job
    // requires, or nullptr.
    Fetcher* pick(const Source& src, const std::vector<std::string>& required) const;

private:
    std::vector<std::shared_ptr<Fetcher>> fetchers_;
};

// Everything that needs nothing configured on the machine it runs on.
Fetchers default_fetchers();

// Whether this build can fetch over https at all.
//
// It cannot everywhere, and saying so is the point. Windows has WinHTTP and
// macOS has NSURLSession — both already on the machine, both trusted by it, both
// free of any dependency. Linux furnishes no such facility, so a build there
// registers no https fetcher and a job with an https source is refused by name
// rather than served by something that quietly does not verify a certificate.
bool https_available();

}  // namespace download
}  // namespace abstraction

#endif  // ABSTRACTION_DOWNLOAD_FETCHER_H
