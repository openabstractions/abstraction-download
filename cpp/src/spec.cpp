#include <abstraction/download/runner.h>
#include <abstraction/download/sink.h>
#include <abstraction/job/store.h>

#include <algorithm>
#include <utility>

namespace abstraction {
namespace download {
namespace {

using job::Json;

std::string str_at(const Json& obj, const char* key) {
    if (!obj.is_object() || !obj.contains(key) || !obj.at(key).is_string()) {
        return "";
    }
    return obj.at(key).get<std::string>();
}

std::int64_t int_at(const Json& obj, const char* key) {
    if (!obj.is_object() || !obj.contains(key) || !obj.at(key).is_number_integer()) {
        return 0;
    }
    return obj.at(key).get<std::int64_t>();
}

std::map<std::string, std::string> map_at(const Json& obj, const char* key) {
    std::map<std::string, std::string> out;
    if (!obj.is_object() || !obj.contains(key) || !obj.at(key).is_object()) {
        return out;
    }
    for (auto it = obj.at(key).begin(); it != obj.at(key).end(); ++it) {
        if (it.value().is_string()) {
            out[it.key()] = it.value().get<std::string>();
        }
    }
    return out;
}

}  // namespace

Spec spec_of(const job::Record& r) {
    if (r.kind != kKind) {
        throw job::Invalid("job " + r.id + " is kind \"" + r.kind + "\", not \"" + kKind + "\"");
    }
    return spec_from(r.spec);
}

Spec spec_from(const Json& d) {
    Spec s;
    if (d.is_object() && d.contains("artifact")) {
        s.artifact.digest = str_at(d.at("artifact"), "digest");
        s.artifact.size = int_at(d.at("artifact"), "size");
    }
    if (d.is_object() && d.contains("sink")) {
        s.sink.partial = str_at(d.at("sink"), "partial");
        s.sink.final_path = str_at(d.at("sink"), "final");
    }
    if (d.is_object() && d.contains("sources") && d.at("sources").is_array()) {
        for (const Json& one : d.at("sources")) {
            Source src;
            src.scheme = str_at(one, "scheme");
            src.locator = str_at(one, "locator");
            src.priority = static_cast<int>(int_at(one, "priority"));
            src.attrs = map_at(one, "attrs");
            src.headers = map_at(one, "headers");
            s.sources.push_back(std::move(src));
        }
    }
    // Lower priority first, and stable so equal priorities keep the order the
    // spec gave them. The order is behaviour: a local copy at -100 is what turns
    // a download into a copy.
    std::stable_sort(s.sources.begin(), s.sources.end(),
                     [](const Source& a, const Source& b) { return a.priority < b.priority; });
    return s;
}

namespace {

Json strings(const std::map<std::string, std::string>& m) {
    Json o = Json::object();
    for (const auto& kv : m) {
        o[kv.first] = kv.second;
    }
    return o;
}

bool blank(const std::string& s) { return s.find_first_not_of(" \t\r\n") == std::string::npos; }

}  // namespace

Json spec_json(const Spec& s) {
    Json artifact = Json::object();
    if (!s.artifact.digest.empty()) {
        artifact["digest"] = s.artifact.digest;
    }
    if (s.artifact.size != 0) {
        artifact["size"] = s.artifact.size;
    }
    Json sources = Json::array();
    for (const Source& src : s.sources) {
        Json one = {{"scheme", src.scheme}, {"locator", src.locator}};
        if (!src.attrs.empty()) {
            one["attrs"] = strings(src.attrs);
        }
        if (!src.headers.empty()) {
            one["headers"] = strings(src.headers);
        }
        if (src.priority != 0) {
            one["priority"] = src.priority;
        }
        sources.push_back(one);
    }
    return {{"artifact", artifact},
            {"sources", sources},
            {"sink", Json{{"partial", s.sink.partial}, {"final", s.sink.final_path}}}};
}

std::string invalid(const Spec& s, const std::string& owner) {
    if (s.sources.empty()) {
        return "download: at least one source is required";
    }
    for (std::size_t i = 0; i < s.sources.size(); ++i) {
        const std::string at = "download: source " + std::to_string(i) + ": ";
        if (blank(s.sources[i].scheme)) {
            return at + "scheme is required";
        }
        if (blank(s.sources[i].locator)) {
            return at + "locator is required";
        }
        for (const auto& h : s.sources[i].headers) {
            if (blank(h.first)) {
                return at + "a header needs a name";
            }
        }
    }
    if (blank(s.sink.final_path)) {
        return "download: sink final path is required";
    }
    for (const std::string& p : {s.sink.final_path, s.sink.partial}) {
        if (const std::string why = escapes_root(p); !why.empty()) {
            return why;
        }
        if (const std::string why = reserved_sink(owner, p); !why.empty()) {
            return why;
        }
    }
    if (!s.artifact.digest.empty() && s.artifact.digest.rfind("sha256:", 0) != 0) {
        return "download: digest \"" + s.artifact.digest + "\" is not sha256:<hex>";
    }
    return "";
}

std::string submit(job::Store& store, Spec spec) {
    const std::string id = job::new_id();
    if (const std::string why = invalid(spec, id); !why.empty()) {
        throw job::Invalid(why);
    }
    spec.sink.partial = portable(spec.sink.partial);
    spec.sink.final_path = portable(spec.sink.final_path);
    if (spec.sink.partial.empty()) {
        spec.sink.partial = partial_for(spec.sink.final_path, id);
    }
    job::Record r;
    r.id = id;
    r.kind = kKind;
    r.progress.total = spec.artifact.size;
    r.spec = spec_json(spec);
    return store.submit(std::move(r));
}

Fetcher* Fetchers::pick(const Source& src, const std::vector<std::string>& required) const {
    for (const auto& f : fetchers_) {
        const std::vector<std::string> schemes = f->schemes();
        if (std::find(schemes.begin(), schemes.end(), src.scheme) == schemes.end()) {
            continue;
        }
        const std::vector<std::string> have = f->capabilities();
        bool all = true;
        for (const std::string& want : required) {
            if (std::find(have.begin(), have.end(), want) == have.end()) {
                all = false;
                break;
            }
        }
        if (all) {
            return f.get();
        }
    }
    return nullptr;
}

}  // namespace download
}  // namespace abstraction
