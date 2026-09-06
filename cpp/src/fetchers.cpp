#include <abstraction/download/fetcher.h>

#include "paths.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cwchar>
#include <fstream>
#include <string>
#include <vector>

#ifdef _WIN32
// windows.h defines min and max as macros, which breaks any use of std::min in
// this file that follows it.
#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
// After windows.h, which winhttp.h requires.
#include <winhttp.h>
#endif

namespace abstraction {
namespace download {
namespace {

namespace fs = std::filesystem;

class FileFetcher : public Fetcher {
public:
    std::vector<std::string> schemes() const override { return {"file", "smb"}; }
    std::vector<std::string> capabilities() const override { return {capability::kResume}; }

    Result fetch(const Request& req) override {
        const fs::path p = path_of(req.source.locator);
        std::ifstream in(p, std::ios::binary);
        if (!in) {
            // Not permanent. A share that was not mounted this minute is the
            // case this project exists for, and ending the job would mean a NAS
            // rebooting costs somebody their 40 GB.
            throw Error("download: cannot open " + req.source.locator, false);
        }
        std::error_code ec;
        const std::int64_t size = static_cast<std::int64_t>(fs::file_size(p, ec));
        if (req.from > 0) {
            in.seekg(static_cast<std::streamoff>(req.from));
        }
        std::vector<char> buf(1 << 20);
        std::int64_t written = 0;
        while (in) {
            std::int64_t want = static_cast<std::int64_t>(buf.size());
            if (req.to > 0) {
                want = std::min(want, req.to - req.from - written);
            }
            if (want <= 0) {
                break;
            }
            in.read(buf.data(), static_cast<std::streamsize>(want));
            const std::streamsize got = in.gcount();
            if (got <= 0) {
                break;
            }
            req.out(buf.data(), static_cast<std::size_t>(got));
            written += got;
            if (req.report) {
                req.report(written, size);
            }
        }
        return Result{written, size};
    }
};

#ifdef _WIN32

// https over WinHTTP.
//
// A platform facility, already installed and already trusted, so it costs no
// dependency — and its certificate trust is the machine's own store, which the
// OS keeps current. A library that shipped its own root bundle would go stale
// inside our binary the day after we released it.
class WinHttpFetcher : public Fetcher {
public:
    std::vector<std::string> schemes() const override { return {"http", "https"}; }
    // Not survives_process_exit: this runs inside the caller's process and dies
    // with it. That gap is what the service tier exists to fill, and claiming
    // otherwise would lie to a job that asked for durability.
    std::vector<std::string> capabilities() const override { return {capability::kResume}; }

    Result fetch(const Request& req) override;
};

bool same_word(const std::string& a, const char* b) {
    std::size_t i = 0;
    for (; i < a.size() && b[i] != '\0'; ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return i == a.size() && b[i] == '\0';
}

std::string trimmed(const std::string& s) {
    const std::size_t first = s.find_first_not_of(" \t");
    if (first == std::string::npos) {
        return std::string();
    }
    return s.substr(first, s.find_last_not_of(" \t") - first + 1);
}

// The first byte position of a `Content-Range`, or -1 when it says nothing this
// layer can act on. `bytes 1000-40959/40960` gives 1000.
std::int64_t content_range_start(const std::string& header) {
    const std::string s = trimmed(header);
    const std::size_t space = s.find(' ');
    if (space == std::string::npos || !same_word(s.substr(0, space), "bytes")) {
        return -1;
    }
    const std::size_t dash = s.find('-', space + 1);
    const std::size_t slash = s.find('/', space + 1);
    if (dash == std::string::npos || slash == std::string::npos || dash > slash) {
        return -1;
    }
    const std::string first = trimmed(s.substr(space + 1, dash - space - 1));
    if (first.empty() || first.find_first_not_of("0123456789") != std::string::npos) {
        return -1;
    }
    return std::stoll(first);
}

// The artifact's full length out of a `Content-Range`, and 0 when the server
// wrote `*`, which it is entitled to do. `bytes 40-63/*` once threw out of
// std::stoll through a path nothing was catching; a one-word header crashed a
// downloader. See download/testdata/scenarios/wire-unknown-total.txt.
std::int64_t content_range_total(const std::string& header) {
    const std::size_t slash = header.find('/');
    if (slash == std::string::npos) {
        return 0;
    }
    const std::string total = trimmed(header.substr(slash + 1));
    if (total.empty() || total.find_first_not_of("0123456789") != std::string::npos) {
        return 0;
    }
    return std::stoll(total);
}

// Why a 206 is not an answer to the request that was sent, or "".
//
// A single range beginning where the next byte will be written is the only 206
// this layer can use. Both ways of failing put real artifact bytes at an offset
// nobody asked about, and neither is visible to a length check or a transport
// error — only Content-Range says so, and only if somebody reads it. A
// `multipart/byteranges` body is worse than misplaced: its boundary line and
// per-part headers are content this layer would author into the artifact.
// RFC 9110 lets a server answer a single range that way and a coalescing proxy
// does, but we never send a multi-range request, so it is never an answer to
// ours.
std::string answers_from(const std::string& content_type, const std::string& content_range,
                         std::int64_t from) {
    const std::string kind = trimmed(content_type);
    if (kind.size() >= 10 && same_word(kind.substr(0, 10), "multipart/")) {
        return "download: one range was answered with " + kind;
    }
    const std::int64_t got = content_range_start(content_range);
    if (got < 0) {
        return "download: a 206 arrived with no usable Content-Range";
    }
    if (got != from) {
        return "download: asked for bytes from " + std::to_string(from) +
               ", got a range starting at " + std::to_string(got);
    }
    return std::string();
}

// shaped matches a fixed layout: `9` a digit, `a` a letter, `#` either a digit
// or the space asctime pads a one-digit day with, anything else itself.
bool shaped(const std::string& s, const std::string& pattern) {
    if (s.size() != pattern.size()) {
        return false;
    }
    for (std::size_t i = 0; i < s.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        switch (pattern[i]) {
            case '9': if (std::isdigit(c) == 0) return false; break;
            case 'a': if (std::isalpha(c) == 0) return false; break;
            case '#': if (std::isdigit(c) == 0 && c != ' ') return false; break;
            default: if (s[i] != pattern[i]) return false; break;
        }
    }
    return true;
}

bool month_at(const std::string& s, std::size_t at) {
    const std::size_t i = std::string("JanFebMarAprMayJunJulAugSepOctNovDec").find(s.substr(at, 3));
    return i != std::string::npos && i % 3 == 0;
}

// The three spellings of an HTTP-date RFC 9110 requires a RECIPIENT to accept:
// IMF-fixdate, which is the only one a sender may use, and the two obsolete
// forms a recipient still meets.
//
// This recognises a shape and does not parse a time, because the value is never
// interpreted: it is echoed back verbatim as `If-Range`, which the origin server
// evaluates by exact match. Written out rather than handed to a date parser
// because the three languages' parsers accept three different sets, so "an HTTP
// date" implemented as "whatever the standard library takes" is not one rule —
// it is three, and until 2026-09-06 this file's was the strictest of them.
bool http_date(const std::string& s) {
    if (shaped(s, "aaa, 99 aaa 9999 99:99:99 GMT")) {
        return month_at(s, 8);
    }
    if (shaped(s, "aaa aaa #9 99:99:99 9999")) {
        return month_at(s, 4);
    }
    const std::size_t comma = s.find(", ");
    if (comma == std::string::npos || comma == 0) {
        return false;
    }
    const std::string tail = s.substr(comma + 2);
    return shaped(s.substr(0, comma), std::string(comma, 'a')) &&
           shaped(tail, "99-aaa-99 99:99:99 GMT") && month_at(tail, 3);
}

Validators strong_validators(const std::string& etag, const std::string& last_modified) {
    const std::string tag = trimmed(etag);
    Validators v;
    if (tag.size() >= 2 && tag.front() == '"' && tag.back() == '"') {
        v.etag = tag;
        return v;
    }
    const std::string when = trimmed(last_modified);
    if (http_date(when)) {
        v.last_modified = when;
    }
    return v;
}

std::wstring wide(const std::string& s) {
    if (s.empty()) {
        return std::wstring();
    }
    const int n = ::MultiByteToWideChar(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), nullptr, 0);
    std::wstring out(static_cast<std::size_t>(n), L'\0');
    ::MultiByteToWideChar(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), &out[0], n);
    return out;
}

struct Handle {
    HINTERNET h = nullptr;
    ~Handle() {
        if (h != nullptr) {
            ::WinHttpCloseHandle(h);
        }
    }
    Handle& operator=(HINTERNET v) {
        if (h != nullptr) {
            ::WinHttpCloseHandle(h);
        }
        h = v;
        return *this;
    }
};

Error winhttp_error(const std::string& what) {
    const DWORD code = ::GetLastError();
    // A name that will not resolve is as permanent as a 404, and everything else
    // a socket can say is "not now".
    const bool permanent = code == ERROR_WINHTTP_NAME_NOT_RESOLVED ||
                           code == ERROR_WINHTTP_UNRECOGNIZED_SCHEME;
    return Error("download: " + what + ": windows error " + std::to_string(code), permanent);
}

std::wstring header_line(const std::map<std::string, std::string>& headers) {
    std::wstring out;
    for (const auto& kv : headers) {
        out += wide(kv.first) + L": " + wide(kv.second) + L"\r\n";
    }
    return out;
}

// Is this status the source saying no, as against the transport having a bad
// moment? download/README.md § Two endings, and it is LISTED, not ranged: this
// was `status >= 400 && status < 500` with two holes cut in it, so 409, 423 and
// 425 — somebody else's lock, and a request that arrived too early — ended jobs
// that would have worked on the next sweep. An unrecognised 4xx is "not now",
// because being wrong that way costs a retry and being wrong the other way
// costs the download. Found by
// download/testdata/scenarios/wire-notnow-status.txt.
bool refused(DWORD status) {
    switch (status) {
        case 400: case 401: case 402: case 403: case 404:
        case 405: case 406: case 410: case 414: case 451:
            return true;
        default:
            return false;
    }
}

std::string narrow(const std::wstring& s) {
    if (s.empty()) {
        return std::string();
    }
    const int n = ::WideCharToMultiByte(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), nullptr, 0,
                                        nullptr, nullptr);
    std::string out(static_cast<std::size_t>(n), '\0');
    ::WideCharToMultiByte(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), &out[0], n, nullptr,
                          nullptr);
    return out;
}

std::string query_header(HINTERNET request, DWORD info) {
    DWORD size = 0;
    DWORD index = WINHTTP_NO_HEADER_INDEX;
    ::WinHttpQueryHeaders(request, info, WINHTTP_HEADER_NAME_BY_INDEX, nullptr, &size, &index);
    if (size == 0) {
        return std::string();
    }
    std::wstring value(size / sizeof(wchar_t) + 1, L'\0');
    index = WINHTTP_NO_HEADER_INDEX;
    if (!::WinHttpQueryHeaders(request, info, WINHTTP_HEADER_NAME_BY_INDEX, &value[0], &size,
                               &index)) {
        return std::string();
    }
    value.resize(std::wcslen(value.c_str()));
    return narrow(value);
}

DWORD query_number(HINTERNET request, DWORD info) {
    DWORD value = 0;
    DWORD size = sizeof(value);
    DWORD index = WINHTTP_NO_HEADER_INDEX;
    if (!::WinHttpQueryHeaders(request, info | WINHTTP_QUERY_FLAG_NUMBER, WINHTTP_HEADER_NAME_BY_INDEX,
                               &value, &size, &index)) {
        return 0;
    }
    return value;
}

Result WinHttpFetcher::fetch(const Request& req) {
    Handle session;
    session = ::WinHttpOpen(L"abstraction-download/0.1", WINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY,
                            WINHTTP_NO_PROXY_NAME, WINHTTP_NO_PROXY_BYPASS, 0);
    if (session.h == nullptr) {
        throw winhttp_error("cannot open an http session");
    }

    const std::wstring url = wide(req.source.locator);
    URL_COMPONENTS parts{};
    parts.dwStructSize = sizeof(parts);
    parts.dwSchemeLength = parts.dwHostNameLength = parts.dwUrlPathLength = parts.dwExtraInfoLength =
        static_cast<DWORD>(-1);
    if (!::WinHttpCrackUrl(url.c_str(), static_cast<DWORD>(url.size()), 0, &parts)) {
        throw Error("download: the source refused: " + req.source.locator + " is not a URL", true);
    }
    const std::wstring host(parts.lpszHostName, parts.dwHostNameLength);
    std::wstring target(parts.lpszUrlPath, parts.dwUrlPathLength);
    target.append(parts.lpszExtraInfo, parts.dwExtraInfoLength);

    Handle connection;
    connection = ::WinHttpConnect(session.h, host.c_str(), parts.nPort, 0);
    if (connection.h == nullptr) {
        throw winhttp_error("cannot reach " + req.source.locator);
    }

    std::int64_t from = req.from;
    std::int64_t to = req.to;
    std::int64_t written = 0;
    std::int64_t total = 0;
    Validators sending = req.validators;

    // Two attempts at most: the second exists only for a source that answered a
    // ranged request with the whole artifact, which means it is serving a
    // different file than the one on disk.
    for (int attempt = 0; attempt < 2; ++attempt) {
        Handle request;
        request = ::WinHttpOpenRequest(connection.h, L"GET", target.c_str(), nullptr,
                                       WINHTTP_NO_REFERER, WINHTTP_DEFAULT_ACCEPT_TYPES,
                                       parts.nScheme == INTERNET_SCHEME_HTTPS ? WINHTTP_FLAG_SECURE
                                                                              : 0);
        if (request.h == nullptr) {
            throw winhttp_error("cannot build a request for " + req.source.locator);
        }

        std::wstring headers = header_line(req.headers);
        // On every request, not only the ranged ones. Offsets on this side are
        // counted after decoding and the server's before it, so a range over a
        // compressed body names a different byte at each end — but the digest is
        // over the artifact, and a coding applied to a whole body changes what
        // "the bytes" are just as much.
        headers += L"Accept-Encoding: identity\r\n";
        if (from > 0 || to > 0) {
            // The last byte position in HTTP is INCLUSIVE and `to` is exclusive,
            // so it goes out as to-1. An off-by-one here fetches one byte too
            // few and leaves a hole only a digest would ever catch.
            headers += L"Range: bytes=" + std::to_wstring(from) + L"-";
            if (to > from) {
                headers += std::to_wstring(to - 1);
            }
            headers += L"\r\n";
            // Which version the bytes on disk came from. Without it a source
            // whose file has changed answers the range honestly, with a valid
            // range of a DIFFERENT file, and nothing in the response says so.
            if (!sending.if_range().empty()) {
                headers += L"If-Range: " + wide(sending.if_range()) + L"\r\n";
            }
        }

        if (!::WinHttpSendRequest(request.h, headers.empty() ? WINHTTP_NO_ADDITIONAL_HEADERS
                                                             : headers.c_str(),
                                  headers.empty() ? 0 : static_cast<DWORD>(-1),
                                  WINHTTP_NO_REQUEST_DATA, 0, 0, 0) ||
            !::WinHttpReceiveResponse(request.h, nullptr)) {
            throw winhttp_error("cannot fetch " + req.source.locator);
        }

        const DWORD status = query_number(request.h, WINHTTP_QUERY_STATUS_CODE);
        const bool ranged = from > 0 || to > 0;

        const std::string coding = trimmed(query_header(request.h, WINHTTP_QUERY_CONTENT_ENCODING));
        if (!coding.empty() && !same_word(coding, "identity")) {
            // A digest is over the artifact and a coding changes what "the
            // bytes" are. Not permanent: a mirror, or the same server tomorrow,
            // may answer the identity this request asked for.
            throw Error("download: " + req.source.locator + " applied Content-Encoding " + coding +
                            " to a request that asked for identity",
                        false);
        }

        const std::string content_range = query_header(request.h, WINHTTP_QUERY_CONTENT_RANGE);

        // Every 206, ranged or not. A first fetch sends no Range and a CDN may
        // still answer 206, which is acceptable exactly when it names the
        // offset being written at — zero. One rule, not two.
        if (status == 206) {
            const std::string why =
                answers_from(query_header(request.h, WINHTTP_QUERY_CONTENT_TYPE), content_range,
                             from);
            if (!why.empty()) {
                if (!ranged || attempt == 1 || !req.restart) {
                    throw Error(why, false);
                }
                req.restart();
                from = 0;
                to = 0;
                written = 0;
                sending = Validators();
                continue;
            }
        } else if (ranged && (status == 200 || status == 416)) {
            // A server that ignores Range answers 200 and sends the whole file
            // from zero. Appending that to a partial gives a file of plausible
            // length and impossible content, which is exactly what `curl -C -`
            // will do. The only honest answers are to start again from zero or
            // to refuse.
            if (attempt == 1 || !req.restart) {
                throw Error("download: the stream restarts at zero and this request cannot rewind",
                            false);
            }
            req.restart();
            from = 0;
            to = 0;
            written = 0;
            sending = Validators();
            if (status != 200) {
                continue;
            }
            // Already holding the whole artifact from byte zero: read it.
        } else if (status != 200) {
            // A refusal the request as written will never satisfy ends the job;
            // a server that is busy or broken does not.
            throw Error("download: the source refused: HTTP " + std::to_string(status),
                        refused(status));
        }

        if (req.observed) {
            req.observed(strong_validators(query_header(request.h, WINHTTP_QUERY_ETAG),
                                           query_header(request.h, WINHTTP_QUERY_LAST_MODIFIED)));
        }

        total = static_cast<std::int64_t>(query_number(request.h, WINHTTP_QUERY_CONTENT_LENGTH));
        if (status == 206) {
            // Content-Length on a 206 is the span, not the artifact. The size
            // after the slash in Content-Range is the artifact.
            total = content_range_total(content_range);
        }

        std::vector<char> buf(1 << 20);
        for (;;) {
            DWORD got = 0;
            if (!::WinHttpReadData(request.h, buf.data(), static_cast<DWORD>(buf.size()), &got)) {
                throw winhttp_error("the connection dropped fetching " + req.source.locator);
            }
            if (got == 0) {
                break;
            }
            req.out(buf.data(), got);
            written += got;
            if (req.report) {
                req.report(written, total);
            }
        }
        return Result{written, total};
    }
    return Result{written, total};
}

#endif  // _WIN32

}  // namespace

Fetchers default_fetchers() {
    Fetchers r;
    r.add(std::make_shared<FileFetcher>());
#ifdef _WIN32
    r.add(std::make_shared<WinHttpFetcher>());
#endif
    return r;
}

bool https_available() {
#ifdef _WIN32
    return true;
#else
    return false;
#endif
}

}  // namespace download
}  // namespace abstraction
