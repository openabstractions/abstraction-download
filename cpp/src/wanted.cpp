#include <abstraction/download/wanted.h>

#include <abstraction/download/digest.h>
#include <abstraction/download/sink.h>
#include <abstraction/job/layout.h>

#include "paths.h"

#include <algorithm>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>

namespace abstraction {
namespace download {
namespace {

namespace fs = std::filesystem;

constexpr const char* kDash = "\xe2\x80\x94";

std::string trim(const std::string& s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) {
        return "";
    }
    return s.substr(b, s.find_last_not_of(" \t\r\n") - b + 1);
}

bool starts(const std::string& s, const std::string& prefix) { return s.rfind(prefix, 0) == 0; }

bool ends(const std::string& s, const std::string& suffix) {
    return s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

std::string lower(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

std::vector<std::string> split_lines(const std::string& text) {
    std::vector<std::string> out;
    std::string::size_type at = 0;
    for (;;) {
        const auto nl = text.find('\n', at);
        if (nl == std::string::npos) {
            out.push_back(text.substr(at));
            return out;
        }
        out.push_back(text.substr(at, nl - at));
        at = nl + 1;
    }
}

std::string name_from(std::string locator) {
    locator = locator.substr(0, locator.find('?'));
    const std::string name = locator.substr(locator.find_last_of('/') + 1);
    return name.empty() || name == "." ? "download.bin" : name;
}

std::string scheme_from(const std::string& locator) {
    const auto at = locator.find("://");
    return at == std::string::npos || at == 0 ? "https" : locator.substr(0, at);
}

bool into_wanted(const std::string& p) {
    const std::vector<std::string> segs = job::store_segments(p);
    return !segs.empty() && segs[0] == kWantedDir;
}

std::string refused_sink(const std::string& p) {
    if (p.empty()) {
        return "";
    }
    if (!relative_everywhere(p)) {
        return "destination is outside the store: " + p;
    }
    if (!escapes_root(p).empty()) {
        return "destination escapes the store: " + p;
    }
    if (!reserved_sink("", p).empty()) {
        return "destination is reserved by the store: " + p;
    }
    if (into_wanted(p)) {
        return "destination is the drop folder itself: " + p;
    }
    return "";
}

Spec wanted_line(const std::string& line) {
    std::istringstream words(line);
    std::vector<std::string> f;
    for (std::string w; words >> w;) {
        f.push_back(w);
    }
    const std::string& locator = f[0];
    if (locator.find("://") == std::string::npos) {
        throw RequestRefused("not a URL: " + locator);
    }
    std::string digest, dest;
    for (std::size_t i = 1; i < f.size(); ++i) {
        const std::string& x = f[i];
        if (!normal_digest(x).empty() && digest.empty()) {
            digest = normal_digest(x);
        } else if (starts(lower(x), "sha256:")) {
            throw RequestRefused("digest is not sha256:<64 hex>: " + x);
        } else if (dest.empty()) {
            dest = x;
        } else {
            throw RequestRefused("one destination per line: " + x);
        }
    }
    if (dest.empty()) {
        dest = std::string(kFilesDir) + "/";
    }
    if (dest.back() == '/' || dest.back() == '\\') {
        dest += name_from(locator);
    }
    Spec s;
    s.artifact.digest = digest;
    Source src;
    src.scheme = scheme_from(locator);
    src.locator = locator;
    s.sources.push_back(src);
    s.sink.final_path = portable(dest);
    if (const std::string why = refused_at_the_door(s); !why.empty()) {
        throw RequestRefused(why);
    }
    return s;
}

std::string stamp() {
    const std::time_t t = std::time(nullptr);
    std::tm local{};
#ifdef _WIN32
    localtime_s(&local, &t);
#else
    localtime_r(&t, &local);
#endif
    char buf[40];
    const std::size_t n = std::strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%S%z", &local);
    std::string s(buf, n);
    if (n >= 5 && (s[n - 5] == '+' || s[n - 5] == '-')) {
        s.insert(n - 2, ":");
    }
    return s;
}

std::string read_all(const std::string& p) {
    std::ifstream in(path_of(p), std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

bool write(const std::string& p, const std::vector<std::string>& lines) {
    const fs::path target = path_of(p);
    const fs::path tmp = target.parent_path() / ("." + utf8_of(target.filename()) + ".tmp");
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        for (const std::string& line : lines) {
            out << line << "\n";
        }
        if (!out) {
            return false;
        }
    }
    std::error_code ec;
    fs::rename(tmp, target, ec);
    return !ec;
}

// The new name is written before the old is removed, so a crash between the
// two leaves the request visible twice rather than gone.
void reply(const std::string& from, const std::string& to, const std::vector<std::string>& lines) {
    if (write(to, lines)) {
        std::error_code ec;
        fs::remove(path_of(from), ec);
    }
}

bool is_answered(const std::string& name) {
    for (const char* s : {answered::kAccepted, answered::kDone, answered::kFailed, answered::kRefused}) {
        if (ends(name, s)) {
            return true;
        }
    }
    return false;
}

// What editors, Finder and Explorer write into any folder they are shown.
bool ignored(const std::string& name) {
    const std::string l = lower(name);
    return starts(name, ".") || ends(name, "~") || l == "desktop.ini" || l == "thumbs.db";
}

// What the person wrote: everything before the first answer.
std::vector<std::string> request_lines(const std::string& text) {
    std::vector<std::string> out;
    std::string body = text;
    while (!body.empty() && body.back() == '\n') {
        body.pop_back();
    }
    for (std::string line : split_lines(body)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (starts(line, "# accepted ") || starts(line, "# refused ")) {
            break;
        }
        out.push_back(line);
    }
    return out;
}

std::vector<std::string> listing(const std::string& dir) {
    std::vector<std::string> names;
    for (const fs::directory_entry& e : fs::directory_iterator(path_of(dir))) {
        if (!e.is_directory()) {
            names.push_back(utf8_of(e.path().filename()));
        }
    }
    std::sort(names.begin(), names.end());
    return names;
}

}  // namespace

std::string refused_at_the_door(const Spec& s) {
    for (const std::string& p : {s.sink.final_path, s.sink.partial}) {
        if (const std::string why = refused_sink(p); !why.empty()) {
            return why;
        }
    }
    for (std::size_t i = 0; i < s.sources.size(); ++i) {
        const Source& src = s.sources[i];
        const std::string at = "source " + std::to_string(i + 1) + ": ";
        if (src.scheme != "http" && src.scheme != "https") {
            return at + src.scheme + " is not fetched for a dropped request, only http and https";
        }
        if (!src.attrs.empty() || !src.headers.empty()) {
            return at + "a dropped request names no credential and sets no header";
        }
    }
    return invalid(s, "");
}

std::vector<Spec> parse_wanted(const std::string& text) {
    std::vector<std::pair<int, std::string>> lines;
    int n = 0;
    for (const std::string& raw : split_lines(text)) {
        ++n;
        const std::string line = trim(raw);
        if (!line.empty() && line[0] != '#') {
            lines.emplace_back(n, line);
        }
    }
    if (lines.empty()) {
        throw RequestRefused("nothing to fetch");
    }
    if (lines[0].second[0] == '{') {
        std::string joined;
        for (const auto& l : lines) {
            joined += l.second + "\n";
        }
        bool ok = false;
        const job::Json d = job::Json::parse(joined, &ok);
        if (!ok || !d.is_object()) {
            throw RequestRefused("not a spec");
        }
        const Spec s = spec_from(d);
        if (const std::string why = refused_at_the_door(s); !why.empty()) {
            throw RequestRefused("spec: " + why);
        }
        return {s};
    }
    std::vector<Spec> specs;
    for (const auto& l : lines) {
        try {
            specs.push_back(wanted_line(l.second));
        } catch (const RequestRefused& e) {
            const std::string why = e.what();
            throw RequestRefused("line " + std::to_string(l.first) + ": " +
                                 why.substr(std::string("download: request refused: ").size()));
        }
    }
    return specs;
}

Wanted::Wanted(job::Store& store, std::function<std::string(const Spec&)> submit)
    : store_(store), submit_(std::move(submit)) {}

std::string Wanted::dir() const {
    const auto* local = dynamic_cast<const job::LocalStore*>(&store_);
    if (local == nullptr || local->root().empty()) {
        throw job::Invalid("download: this store has no local area");
    }
    const std::string d = local->root() + "/" + kWantedDir;
    fs::create_directories(path_of(d));
    return d;
}

std::vector<std::string> Wanted::take_in() {
    const std::string d = dir();
    std::vector<std::string> ids;
    for (const std::string& name : listing(d)) {
        if (is_answered(name) || ignored(name)) {
            continue;
        }
        const std::vector<std::string> taken = take(d + "/" + name);
        ids.insert(ids.end(), taken.begin(), taken.end());
    }
    return ids;
}

std::vector<std::string> Wanted::take(const std::string& p) {
    std::error_code ec;
    const auto size = fs::file_size(path_of(p), ec);
    if (ec) {
        return {};
    }
    if (static_cast<std::int64_t>(size) > kRequestLimit) {
        fs::rename(path_of(p), path_of(p + answered::kRefused), ec);
        return {};
    }
    const std::string text = read_all(p);
    std::vector<std::string> lines = request_lines(text);
    std::vector<Spec> specs;
    try {
        specs = parse_wanted(text);
    } catch (const RequestRefused& e) {
        const std::string why = e.what();
        lines.push_back("# refused " + stamp() + ": " +
                        why.substr(std::string("download: request refused: ").size()));
        reply(p, p + answered::kRefused, lines);
        return {};
    }
    fs::rename(path_of(p), path_of(p + answered::kAccepted), ec);
    if (ec) {
        return {};
    }
    lines.push_back("# accepted " + stamp());
    std::vector<std::string> ids;
    for (const Spec& s : specs) {
        try {
            const std::string id = submit_(s);
            ids.push_back(id);
            lines.push_back("# job " + id + " -> " + s.sink.final_path);
        } catch (const std::exception& e) {
            lines.push_back(std::string("# refused: ") + e.what());
        }
    }
    if (ids.empty()) {
        reply(p + answered::kAccepted, p + answered::kRefused, lines);
        return {};
    }
    write(p + answered::kAccepted, lines);
    return ids;
}

void Wanted::answer() {
    const std::string d = dir();
    for (const std::string& name : listing(d)) {
        if (ends(name, answered::kAccepted)) {
            follow(d + "/" + name);
        }
    }
}

// Everything through the last `# job` line is kept and what follows is
// regenerated, so the request and its ids survive every rewrite.
void Wanted::follow(const std::string& p) {
    const std::string text = read_all(p);
    std::string body = text;
    while (!body.empty() && body.back() == '\n') {
        body.pop_back();
    }
    std::vector<std::string> lines = split_lines(body);
    const std::string base = p.substr(0, p.size() - std::string(answered::kAccepted).size());
    std::size_t kept = 0;
    for (std::size_t i = 0; i < lines.size(); ++i) {
        if (starts(lines[i], "# job ")) {
            kept = i + 1;
        }
    }
    if (kept == 0) {
        lines.push_back("# refused " + stamp() + ": no job was recorded");
        reply(p, base + answered::kRefused, lines);
        return;
    }
    lines.resize(kept);
    std::size_t jobs = 0, done = 0, failed = 0;
    std::vector<std::string> status;
    for (const std::string& line : lines) {
        if (!starts(line, "# job ")) {
            continue;
        }
        const std::string rest = line.substr(6);
        const auto arrow = rest.find(" -> ");
        const auto [s, end] = progress(rest.substr(0, arrow),
                                       arrow == std::string::npos ? "" : rest.substr(arrow + 4));
        status.push_back(s);
        ++jobs;
        done += end == answered::kDone;
        failed += end == answered::kFailed;
    }
    lines.insert(lines.end(), status.begin(), status.end());
    if (done == jobs) {
        reply(p, base + answered::kDone, lines);
    } else if (done + failed == jobs) {
        reply(p, base + answered::kFailed, lines);
    } else {
        std::string now;
        for (const std::string& line : lines) {
            now += line + "\n";
        }
        if (now != text) {
            write(p, lines);
        }
    }
}

std::pair<std::string, std::string> Wanted::progress(const std::string& id,
                                                     const std::string& final_path) {
    job::Record rec;
    try {
        rec = store_.load(id);
    } catch (const job::JobError&) {
        return {"# failed: " + final_path + " " + kDash + " its job is gone from the store",
                answered::kFailed};
    }
    if (rec.state == job::state::kComplete || rec.state == job::state::kTransferred) {
        std::string line = "# done " + stamp() + ": " + final_path + ", " +
                           std::to_string(rec.progress.done) + " bytes";
        try {
            if (const std::string digest = spec_of(rec).artifact.digest; !digest.empty()) {
                line += ", " + digest + " verified";
            }
        } catch (const job::JobError&) {
        }
        return {line, answered::kDone};
    }
    if (rec.state == job::state::kFailed || rec.state == job::state::kCancelled) {
        return {"# failed " + stamp() + ": " + final_path + " " + kDash + " " +
                    (rec.error.empty() ? rec.state : rec.error),
                answered::kFailed};
    }
    std::string line = "# " + rec.state;
    if (rec.progress.total > 0) {
        line += " " + std::to_string(100 * rec.progress.done / rec.progress.total) + "%";
    }
    if (!rec.error.empty()) {
        line += std::string(" ") + kDash + " last attempt: " + rec.error;
    }
    return {line, ""};
}

}  // namespace download
}  // namespace abstraction
