// A digest reduced to the part that carries the meaning.
//
// One implementation writes "sha256:<hex>", another the bare hex, and the
// contract is that they name the same artifact. Comparing the SPELLING instead
// is what deleted a correct 1.5 GB download: the error read "got sha256:1fc70f…
// want 1fc70f…", the same digest twice.
//
// Anything unrecognised becomes empty rather than itself, so a comparison can
// never succeed on two things neither side understood.

#ifndef ABSTRACTION_DOWNLOAD_DIGEST_H
#define ABSTRACTION_DOWNLOAD_DIGEST_H

#include <cctype>
#include <string>

namespace abstraction {
namespace download {

inline std::string normal_digest(const std::string& raw) {
    std::string s;
    for (char c : raw) {
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
            s.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
    }
    for (const char* prefix : {"sha256:", "sha256-"}) {
        const std::string p(prefix);
        if (s.rfind(p, 0) == 0) {
            s = s.substr(p.size());
            break;
        }
    }
    if (s.size() != 64) {
        return "";
    }
    for (char c : s) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return "";
        }
    }
    return "sha256:" + s;
}

}  // namespace download
}  // namespace abstraction

#endif  // ABSTRACTION_DOWNLOAD_DIGEST_H
