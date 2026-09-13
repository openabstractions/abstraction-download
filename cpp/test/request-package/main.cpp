#include <abstraction/download/request/rec.h>
#include <iostream>
int main() {
    namespace request = abstraction::download::request;
    request::Request original;
    original.artifact.digest = "sha256:0123456789abcdef";
    original.artifact.size = 70000;
    original.sources.push_back({"http", "http://127.0.0.1/artifact?name=\"example\""});
    const auto encoded = request::encode(original);
    const auto decoded = request::decode(encoded);
    if (decoded.artifact.digest != original.artifact.digest ||
        decoded.artifact.size != original.artifact.size ||
        decoded.sources.size() != 1 || decoded.sources[0].scheme != "http" ||
        decoded.sources[0].locator != original.sources[0].locator) return 1;
    try { request::decode(encoded + "{}"); return 2; }
    catch (const std::exception&) {}
    std::cout << "request vocabulary roundtrip and extra-document refusal passed\n";
}
