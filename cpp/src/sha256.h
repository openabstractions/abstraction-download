// SHA-256, written out rather than taken from anywhere.
//
// The two obvious alternatives both fail a rule this project holds. A package
// from a registry is an attack surface handed to every adopter; a platform
// crypto library (BCrypt, CommonCrypto) is free but is three code paths for a
// function whose definition was frozen in 2001 and cannot go stale. The rule
// against vendoring what the platform maintains is about data that rots — CA
// bundles, timezones — and a hash function is neither.

#ifndef ABSTRACTION_DOWNLOAD_SHA256_H
#define ABSTRACTION_DOWNLOAD_SHA256_H

#include <cstddef>
#include <cstdint>
#include <string>

namespace abstraction {
namespace download {

class Sha256 {
public:
    void write(const void* data, std::size_t n);
    // Finalises. "sha256:" + 64 lowercase hex, the form the record carries.
    std::string digest();

private:
    void block(const unsigned char* p);

    std::uint32_t h_[8] = {0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au,
                           0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u};
    unsigned char buf_[64] = {};
    std::size_t used_ = 0;
    std::uint64_t bytes_ = 0;
};

}  // namespace download
}  // namespace abstraction

#endif  // ABSTRACTION_DOWNLOAD_SHA256_H
