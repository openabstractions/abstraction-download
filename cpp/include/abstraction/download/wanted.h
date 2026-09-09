// The drop folder: a text file put in wanted/ inside the store is a request,
// and the folder answers it by renaming the file. [DL-W1]..[DL-W4] on the
// contract page.

#ifndef ABSTRACTION_DOWNLOAD_WANTED_H
#define ABSTRACTION_DOWNLOAD_WANTED_H

#include <abstraction/download/runner.h>
#include <abstraction/job/store.h>

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace abstraction {
namespace download {

constexpr const char* kFilesDir = "files";
constexpr const char* kWantedDir = "wanted";
constexpr std::int64_t kRequestLimit = 64 << 10;

namespace answered {
constexpr const char* kAccepted = ".accepted";
constexpr const char* kDone = ".done";
constexpr const char* kFailed = ".failed";
constexpr const char* kRefused = ".refused";
}  // namespace answered

class RequestRefused : public std::runtime_error {
public:
    explicit RequestRefused(const std::string& why)
        : std::runtime_error("download: request refused: " + why) {}
};

// One dropped file as work: # lines ignored, a file beginning { is a spec as
// the contract page spells it, anything else is one download per line.
std::vector<Spec> parse_wanted(const std::string& text);

// The refusal the drop folder gives a spec a record could carry, or "".
std::string refused_at_the_door(const Spec& s);

class Wanted {
public:
    Wanted(job::Store& store, std::function<std::string(const Spec&)> submit);

    // The folder, created if absent.
    std::string dir() const;

    // Every new file becomes work and is answered in place; the ids taken.
    std::vector<std::string> take_in();

    // Every accepted request moves on: .done, .failed, or its progress rewritten.
    void answer();

private:
    std::vector<std::string> take(const std::string& path);
    void follow(const std::string& path);
    std::pair<std::string, std::string> progress(const std::string& id, const std::string& final_path);

    job::Store& store_;
    std::function<std::string(const Spec&)> submit_;
};

}  // namespace download
}  // namespace abstraction

#endif  // ABSTRACTION_DOWNLOAD_WANTED_H
