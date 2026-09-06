// A path that a record spells in UTF-8, opened as the filesystem spells it.
//
// Not decoration on Windows: fs::path built from a narrow string is read in the
// active code page, so a record naming a model in any non-ASCII script opens the
// wrong file or none at all — and the record is UTF-8 because three languages
// have to read it.

#ifndef ABSTRACTION_DOWNLOAD_PATHS_H
#define ABSTRACTION_DOWNLOAD_PATHS_H

#include <filesystem>
#include <string>

namespace abstraction {
namespace download {

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

}  // namespace download
}  // namespace abstraction

#endif  // ABSTRACTION_DOWNLOAD_PATHS_H
