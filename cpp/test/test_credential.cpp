// The same cases as download/go/credentials_test.go and
// download/python/test_abstraction_download.py, deliberately: a record naming
// credential "hf" with a locator the owner never chose must not receive the
// owner's token, and the bound host must.

#include <abstraction/download/credential.h>

#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>

using abstraction::download::attach_credential;
using abstraction::download::credential_bound;

static int g_failures = 0;

static void check(const char* name, bool ok) {
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", name);
    if (!ok) ++g_failures;
}

static void set_env(const char* key, const char* value) {
#ifdef _WIN32
    _putenv_s(key, value);
#else
    setenv(key, value, 1);
#endif
}

static const char* const kCanary = "hf_thisMustNeverAppearOnDisk_EXAMPLE";
static const std::map<std::string, std::string> kNamesHf = {{"credential", "hf"}};

static void test_credential_is_bound_to_its_hosts() {
    set_env("ABSTRACTION_CRED_HF", kCanary);
    set_env("ABSTRACTION_CRED_HF_HOSTS", "huggingface.co");

    std::map<std::string, std::string> headers;
    check("a host the owner never chose is refused",
          !attach_credential(kNamesHf, "attacker.example", headers).empty());
    check("nothing was attached for it", headers.empty());

    for (const char* host : {"huggingface.co", "cdn-lfs.huggingface.co", "HuggingFace.co"}) {
        headers.clear();
        check((std::string("the bound host is not refused: ") + host).c_str(),
              attach_credential(kNamesHf, host, headers).empty());
        check((std::string("the bound host receives the token: ") + host).c_str(),
              headers["Authorization"] == std::string("Bearer ") + kCanary);
    }
    check("a suffix that is not a subdomain is refused",
          !credential_bound("hf", "nothuggingface.co"));
}

static void test_unbound_credential_reaches_nobody() {
    set_env("ABSTRACTION_CRED_HF", kCanary);
    set_env("ABSTRACTION_CRED_HF_HOSTS", "");
    std::map<std::string, std::string> headers;
    check("no host list means no host", !attach_credential(kNamesHf, "huggingface.co", headers).empty());
    check("an empty host is refused", !credential_bound("hf", ""));
}

static void test_missing_credential_is_refused_not_sent() {
    set_env("ABSTRACTION_CRED_HF", "");
    set_env("ABSTRACTION_CRED_HF_HOSTS", "huggingface.co");
    std::map<std::string, std::string> headers;
    check("an unset token is a refusal, not an anonymous fetch",
          !attach_credential(kNamesHf, "huggingface.co", headers).empty());
    check("a source naming no credential needs none",
          attach_credential({}, "anywhere.example", headers).empty());
}

static void test_credential_header_names_where_the_secret_goes() {
    set_env("ABSTRACTION_CRED_HF", kCanary);
    set_env("ABSTRACTION_CRED_HF_HOSTS", "huggingface.co");
    std::map<std::string, std::string> headers;
    const std::map<std::string, std::string> attrs = {{"credential", "hf"},
                                                      {"credential_header", "X-Token"}};
    check("attached", attach_credential(attrs, "huggingface.co", headers).empty());
    check("into the named header", headers.count("X-Token") == 1 && headers.count("Authorization") == 0);
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    test_credential_is_bound_to_its_hosts();
    test_unbound_credential_reaches_nobody();
    test_missing_credential_is_refused_not_sent();
    test_credential_header_names_where_the_secret_goes();
    std::printf("%d failure(s)\n", g_failures);
    return g_failures == 0 ? 0 : 1;
}
