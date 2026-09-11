// The two questions about a path that neither the bytes nor the OS name can
// answer: how to open it, and whether two of them are one file.
//
// Public rather than private to src/, because both are rules an adopter has to
// obey to read our records correctly.

#ifndef ABSTRACTION_DOWNLOAD_PATHS_H
#define ABSTRACTION_DOWNLOAD_PATHS_H

#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <system_error>

namespace abstraction {
namespace download {

// A path that a record spells in UTF-8, opened as the filesystem spells it.
//
// Not decoration on Windows: fs::path built from a narrow string is read in the
// active code page, so a record naming a model in any non-ASCII script opens the
// wrong file or none at all — and the record is UTF-8 because three languages
// have to read it.
inline std::filesystem::path path_of(const std::string& utf8) {
#if defined(_WIN32) && defined(__cpp_lib_char8_t)
    return std::filesystem::path(std::u8string(utf8.begin(), utf8.end()));
#elif defined(_WIN32)
    return std::filesystem::u8path(utf8);
#else
    return std::filesystem::path(utf8);
#endif
}

inline std::string utf8_of(const std::filesystem::path& p) {
    const auto s = p.u8string();
    return std::string(s.begin(), s.end());
}

namespace paths_detail {

// The filter that keeps the probe off the common path. Two names holding no byte
// above 0x7F and still differing after A–Z is folded cannot be one file
// anywhere: case mapping and Unicode normalisation are both the identity on
// ASCII apart from the letters. So "llama.gguf" against "mistral.gguf" costs
// nothing, and only a real candidate reaches the volume.
inline bool may_fold(const std::string& x, const std::string& y) {
    for (const std::string* s : {&x, &y}) {
        for (unsigned char c : *s) {
            if (c >= 0x80) {
                return true;
            }
        }
    }
    if (x.size() != y.size()) {
        return false;
    }
    for (std::size_t i = 0; i < x.size(); ++i) {
        unsigned char a = static_cast<unsigned char>(x[i]);
        unsigned char b = static_cast<unsigned char>(y[i]);
        if (a >= 'A' && a <= 'Z') a = static_cast<unsigned char>(a + 32);
        if (b >= 'A' && b <= 'Z') b = static_cast<unsigned char>(b + 32);
        if (a != b) {
            return false;
        }
    }
    return true;
}

// The closest ancestor that exists. On Windows a directory's case sensitivity is
// a per-directory flag inherited from its parent at creation, so the nearest
// existing ancestor is the one that will decide for the directories still to be
// made under it.
inline std::filesystem::path nearest_dir(std::filesystem::path d) {
    std::error_code ec;
    while (!d.empty()) {
        if (std::filesystem::is_directory(d, ec)) {
            return d;
        }
        const std::filesystem::path up = d.parent_path();
        if (up == d) {
            return {};
        }
        d = up;
    }
    return {};
}

inline std::string probe_tag() {
    std::random_device rd;
    static const char* kHex = "0123456789abcdef";
    std::string out = ".abstraction-samepath-";
    for (int i = 0; i < 8; ++i) {
        const unsigned v = rd() & 0xFFu;
        out.push_back(kHex[(v >> 4) & 0xF]);
        out.push_back(kHex[v & 0xF]);
    }
    return out + "-";
}

// Asks dir whether it keeps x and y as two names, by making one and looking for
// the other. Both are carried on a name of our own so that neither destination
// is created as a side effect of being asked about.
inline bool folds_together(const std::filesystem::path& dir, std::string x, std::string y) {
    namespace fs = std::filesystem;
    if (dir.empty()) {
        return false;
    }
    if (x > y) {
        x.swap(y);
    }
    const std::string key = utf8_of(dir) + '\0' + x + '\0' + y;

    static std::mutex lock;
    static std::map<std::string, bool> answered;
    {
        std::lock_guard<std::mutex> held(lock);
        const auto it = answered.find(key);
        if (it != answered.end()) {
            return it->second;
        }
    }

    const std::string tag = probe_tag();
    std::error_code ec;
    const fs::path mine = dir / path_of(tag + x);
    if (fs::exists(mine, ec)) {
        return false;
    }
    {
        std::ofstream out(mine, std::ios::binary);
        if (!out) {
            return false;
        }
    }
    const fs::path other = dir / path_of(tag + y);
    const bool same = fs::exists(other, ec) && fs::equivalent(mine, other, ec) && !ec;
    fs::remove(mine, ec);

    std::lock_guard<std::mutex> held(lock);
    answered[key] = same;
    return same;
}

}  // namespace paths_detail

// Whether a and b name one file.
//
// The question belongs to the filesystem holding them, never to their bytes:
// NTFS folds "café" against its uppercase and keeps U+212A apart, APFS folds
// both, ext4 folds neither, and one volume can be mounted differently from the
// next. std::filesystem::equivalent is the standard library's answer and it
// compares device and inode, or volume serial and file id, which is where the
// answer actually lives — Go spells it os.SameFile, Python spells it
// os.path.samefile, Java spells it Files.isSameFile. See abstraction-download/CONTRACT.md,
// "Two paths, one file".
//
// A destination usually does not exist yet, which those four cannot answer. Its
// PARENT does, so the parent is settled the same way and the final component is
// settled by asking the parent directory to demonstrate its own rule.
inline bool same_path(const std::string& a, const std::string& b) {
    namespace fs = std::filesystem;
    if (a.empty() || b.empty()) {
        return a == b;
    }
    std::error_code ec;
    fs::path pa = fs::absolute(path_of(a), ec);
    if (ec) pa = path_of(a);
    ec.clear();
    fs::path pb = fs::absolute(path_of(b), ec);
    if (ec) pb = path_of(b);
    pa = pa.lexically_normal();
    pb = pb.lexically_normal();
    if (pa == pb) {
        return true;
    }

    ec.clear();
    const bool has_a = fs::exists(pa, ec);
    ec.clear();
    const bool has_b = fs::exists(pb, ec);
    if (has_a && has_b) {
        ec.clear();
        const bool same = fs::equivalent(pa, pb, ec);
        return same && !ec;
    }
    // One spelling resolves and the other does not, so the volume holding them
    // does not treat them as one name. This is an answer, not a failure.
    if (has_a || has_b) {
        return false;
    }

    const std::string name_a = utf8_of(pa.filename());
    const std::string name_b = utf8_of(pb.filename());
    if (name_a.empty() || name_b.empty()) {
        return false;
    }
    // A path whose parent is itself has nowhere left to climb, and recursing on
    // it would not terminate.
    if (pa.parent_path() == pa || pb.parent_path() == pb) {
        return false;
    }
    if (!same_path(utf8_of(pa.parent_path()), utf8_of(pb.parent_path()))) {
        return false;
    }
    if (name_a == name_b) {
        return true;
    }
    if (!paths_detail::may_fold(name_a, name_b)) {
        return false;
    }
    return paths_detail::folds_together(paths_detail::nearest_dir(pa.parent_path()), name_a, name_b);
}

}  // namespace download
}  // namespace abstraction

#endif  // ABSTRACTION_DOWNLOAD_PATHS_H
