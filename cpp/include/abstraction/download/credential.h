#ifndef ABSTRACTION_DOWNLOAD_CREDENTIAL_H
#define ABSTRACTION_DOWNLOAD_CREDENTIAL_H

#include <cctype>
#include <cstdlib>
#include <map>
#include <string>

namespace abstraction {
namespace download {

// A record names a credential -- attrs.credential=hf -- and never a secret.
// The secret lives on the fetching machine as $ABSTRACTION_CRED_HF, and the
// hosts it may be sent to live beside it as $ABSTRACTION_CRED_HF_HOSTS, a
// comma-separated list each covering its subdomains. Neither is read from the
// record or the store, because a shared store is written by whoever can write
// it, and a record that chose the host would otherwise choose where the
// owner's token goes. Same rule as download/go/credentials.go.

inline const char* const kCredentialAttr = "credential";
inline const char* const kCredentialHeaderAttr = "credential_header";

inline std::string credential_env(const std::string& name) {
    std::string out = "ABSTRACTION_CRED_";
    for (char c : name) {
        out += (c == '-' || c == '.') ? '_'
                                      : static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return out;
}

inline std::string env_value(const std::string& key) {
    const char* v = std::getenv(key.c_str());
    return v == nullptr ? std::string() : std::string(v);
}

inline std::string lowered(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

inline bool credential_bound(const std::string& name, const std::string& host) {
    const std::string h = lowered(host);
    if (h.empty()) {
        return false;
    }
    const std::string list = env_value(credential_env(name) + "_HOSTS");
    std::size_t at = 0;
    while (at <= list.size()) {
        std::size_t comma = list.find(',', at);
        if (comma == std::string::npos) {
            comma = list.size();
        }
        std::string pat = lowered(list.substr(at, comma - at));
        while (!pat.empty() && pat.front() == ' ') pat.erase(0, 1);
        while (!pat.empty() && pat.back() == ' ') pat.pop_back();
        if (!pat.empty() &&
            (h == pat || (h.size() > pat.size() && h.compare(h.size() - pat.size() - 1, pat.size() + 1, "." + pat) == 0))) {
            return true;
        }
        at = comma + 1;
    }
    return false;
}

// Adds what the record's credential resolves to for host, or returns the
// reason nothing may be sent there. An empty reason means go ahead. The
// refusal is this machine's binding, never the record's fault, so a caller
// leaves the job adoptable.
inline std::string attach_credential(const std::map<std::string, std::string>& attrs,
                                     const std::string& host,
                                     std::map<std::string, std::string>& headers) {
    const auto named = attrs.find(kCredentialAttr);
    if (named == attrs.end() || named->second.empty()) {
        return "";
    }
    const std::string& name = named->second;
    const std::string token = env_value(credential_env(name));
    if (token.empty() || !credential_bound(name, host)) {
        return "download: credential \"" + name + "\" is not configured for host \"" + host +
               "\" -- bind it on the fetching machine with " + credential_env(name) + " and " +
               credential_env(name) + "_HOSTS, never in the record";
    }
    const auto into = attrs.find(kCredentialHeaderAttr);
    headers[into == attrs.end() || into->second.empty() ? "Authorization" : into->second] =
        "Bearer " + token;
    return "";
}

}  // namespace download
}  // namespace abstraction

#endif  // ABSTRACTION_DOWNLOAD_CREDENTIAL_H
