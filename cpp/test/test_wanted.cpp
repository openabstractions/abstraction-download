// The drop folder, from the contract: what it refuses, what it takes, and how
// it answers. The refusals are the security half — anyone who can write to a
// share can write here.

#include <abstraction/download/wanted.h>
#include <abstraction/job/store.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;

using abstraction::download::parse_wanted;
using abstraction::download::RequestRefused;
using abstraction::download::Spec;
using abstraction::download::Wanted;
using abstraction::job::FileStore;
using abstraction::job::Record;

static int g_failures = 0;

static void check(const std::string& name, bool ok) {
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", name.c_str());
    if (!ok) {
        ++g_failures;
    }
}

namespace {

const std::string kHex(64, 'a');

bool refuses(const std::string& text) {
    try {
        parse_wanted(text);
    } catch (const RequestRefused&) {
        return true;
    }
    return false;
}

Spec one(const std::string& text) {
    const auto specs = parse_wanted(text);
    return specs.empty() ? Spec{} : specs[0];
}

class Scratch {
public:
    Scratch() {
        dir_ = fs::temp_directory_path() /
               ("abstraction-wanted-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(dir_ / "store" / "wanted");
    }
    ~Scratch() {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }
    std::string root() const { return (dir_ / "store").string(); }
    std::string wanted(const std::string& name) const {
        return (dir_ / "store" / "wanted" / name).string();
    }
    void put(const std::string& name, const std::string& text) const {
        std::ofstream out(wanted(name), std::ios::binary);
        out << text;
    }
    std::string read(const std::string& name) const {
        std::ifstream in(wanted(name), std::ios::binary);
        return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    }
    bool has(const std::string& name) const { return fs::exists(wanted(name)); }

private:
    fs::path dir_;
};

void test_the_door_refuses_what_a_share_could_aim_at_the_supervisor() {
    const std::string url = "https://example.com/m.gguf ";
    check("absolute posix destination", refuses(url + "/etc/cron.d/evil"));
    check("absolute windows destination", refuses(url + "C:\\Windows\\evil"));
    check("unc destination", refuses(url + "\\\\nas\\share\\evil"));
    check("traversal", refuses(url + "../../etc/cron.d/evil"));
    check("traversal spelled with a detour", refuses(url + "models/../../evil"));
    check("job record", refuses(url + "jobs/x.json"));
    check("another job's scratch", refuses(url + "work/other"));
    check("the registry", refuses(url + "services.json"));
    check("the heartbeat", refuses(url + "supervisor.json"));
    check("the drop folder itself", refuses(url + "wanted/again.txt"));
    check("the drop folder, any case", refuses(url + "Wanted/again.txt"));
    check("file source", refuses("file:///C:/secrets.txt"));
    check("ftp source", refuses("ftp://example.com/x"));
    check("not a url", refuses("not-a-url"));
    check("malformed digest", refuses(url + "sha256:abc"));
    check("two destinations", refuses(url + "a/ b/"));
    check("nothing to fetch", refuses("# only a comment\n\n"));
    check("a header in spec form",
          refuses("{\"sources\":[{\"scheme\":\"https\",\"locator\":\"https://x/y\","
                  "\"headers\":{\"Authorization\":\"Bearer t\"}}],\"sink\":{\"final\":\"models/y\"}}"));
    check("a credential in spec form",
          refuses("{\"sources\":[{\"scheme\":\"https\",\"locator\":\"https://x/y\","
                  "\"attrs\":{\"credential\":\"hf\"}}],\"sink\":{\"final\":\"models/y\"}}"));
    check("a file scheme in spec form",
          refuses("{\"sources\":[{\"scheme\":\"file\",\"locator\":\"/etc/passwd\"}],"
                  "\"sink\":{\"final\":\"models/y\"}}"));
    check("an escaping partial in spec form",
          refuses("{\"sources\":[{\"scheme\":\"https\",\"locator\":\"https://x/y\"}],"
                  "\"sink\":{\"final\":\"models/y\",\"partial\":\"../y.part\"}}"));
    check("a spec that is not json", refuses("{not json"));
    check("one bad line refuses the file", refuses(url + "\nnot-a-url\n"));
}

void test_the_line_form_is_the_spec_form() {
    const Spec bare = one("https://example.com/dir/m.gguf?x=1\n");
    check("default destination is files/<name>", bare.sink.final_path == "files/m.gguf");
    check("scheme comes from the url", bare.sources.size() == 1 && bare.sources[0].scheme == "https");
    check("no digest unless given", bare.artifact.digest.empty());

    const Spec into = one("http://example.com/m.gguf models/\n");
    check("a directory takes the url's name", into.sink.final_path == "models/m.gguf");

    const Spec named = one("https://example.com/m.gguf sha256:" + kHex + " models/x.bin\n");
    check("digest then destination", named.artifact.digest == "sha256:" + kHex &&
                                         named.sink.final_path == "models/x.bin");
    const Spec swapped = one("https://example.com/m.gguf models/x.bin " + kHex + "\n");
    check("destination then bare digest", swapped.artifact.digest == "sha256:" + kHex &&
                                              swapped.sink.final_path == "models/x.bin");
    const Spec backslashed = one("https://example.com/m.gguf models\\sub\\\n");
    check("a backslash destination is respelled", backslashed.sink.final_path == "models/sub/m.gguf");

    const auto two = parse_wanted("# two\nhttps://a/x\n\nhttps://b/y isos/\n");
    check("one download per line", two.size() == 2 && two[1].sink.final_path == "isos/y");

    const Spec spec = one("{\"artifact\":{\"digest\":\"sha256:" + kHex + "\"},"
                          "\"sources\":[{\"scheme\":\"https\",\"locator\":\"https://x/y\"}],"
                          "\"sink\":{\"final\":\"models/y\"}}\n");
    check("a spec is taken as written", spec.sink.final_path == "models/y" &&
                                            spec.artifact.digest == "sha256:" + kHex);
}

void test_the_folder_answers_in_place() {
    Scratch s;
    FileStore store(s.root());
    Wanted w(store, [&](const Spec& spec) { return abstraction::download::submit(store, spec); });

    s.put("good.txt", "https://example.com/m.gguf sha256:" + kHex + " models/\n");
    s.put("bad.txt", "https://example.com/m.gguf ../out\n");
    s.put(".hidden", "https://example.com/ignored\n");
    s.put("editor.txt~", "https://example.com/ignored\n");
    s.put("huge.txt", std::string(65 << 10, 'x'));
    const auto ids = w.take_in();

    check("one job taken", ids.size() == 1);
    check("accepted is renamed", s.has("good.txt.accepted") && !s.has("good.txt"));
    const std::string accepted = s.read("good.txt.accepted");
    check("accepted keeps the request",
          accepted.rfind("https://example.com/m.gguf sha256:" + kHex + " models/\n", 0) == 0);
    check("accepted names the job", !ids.empty() &&
                                        accepted.find("# job " + ids[0] + " -> models/m.gguf\n") !=
                                            std::string::npos);
    check("refused is renamed", s.has("bad.txt.refused") && !s.has("bad.txt"));
    const std::string refused = s.read("bad.txt.refused");
    check("refused keeps its text", refused.rfind("https://example.com/m.gguf ../out\n", 0) == 0);
    check("refused says which line", refused.find("# refused ") != std::string::npos &&
                                         refused.find("line 1:") != std::string::npos);
    check("editor droppings are left alone", s.has(".hidden") && s.has("editor.txt~"));
    check("an oversized file is renamed unread",
          s.has("huge.txt.refused") && s.read("huge.txt.refused").find('#') == std::string::npos);
    check("nothing but LF", accepted.find('\r') == std::string::npos);

    w.answer();
    check("still accepted while pending", s.has("good.txt.accepted"));
    check("progress is the state", s.read("good.txt.accepted").find("\n# pending\n") != std::string::npos);

    const std::string id = ids.empty() ? "" : ids[0];
    Record claimed = store.claim(id, "test", std::chrono::seconds(30));
    store.update(id, claimed.lease.epoch, [](Record& r) {
        r.state = abstraction::job::state::kTransferred;
        r.progress.done = 64;
    });
    w.answer();
    check("done once delivered", s.has("good.txt.done") && !s.has("good.txt.accepted"));
    const std::string done = s.read("good.txt.done");
    check("done names the path, the size and the verified digest",
          done.find("# done ") != std::string::npos &&
              done.find("models/m.gguf, 64 bytes, sha256:" + kHex + " verified") != std::string::npos);
    check("done still names the job", done.find("# job " + id) != std::string::npos);

    s.put("lost.txt", "https://example.com/other.bin\n");
    const auto more = w.take_in();
    check("a second request is taken", more.size() == 1);
    Record again = store.claim(more[0], "test", std::chrono::seconds(30));
    store.update(more[0], again.lease.epoch, [](Record& r) {
        r.state = abstraction::job::state::kFailed;
        r.error = "404";
    });
    w.answer();
    check("failed once a job ends without delivering", s.has("lost.txt.failed"));
    check("failed says why", s.read("lost.txt.failed").find("files/other.bin") != std::string::npos &&
                                 s.read("lost.txt.failed").find("404") != std::string::npos);

    s.put("retry.txt", s.read("bad.txt.refused"));
    w.take_in();
    check("renaming a refusal back tries again and keeps only the request",
          s.has("retry.txt.refused") &&
              s.read("retry.txt.refused").find("# refused") ==
                  s.read("retry.txt.refused").rfind("# refused"));
}

}  // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    test_the_door_refuses_what_a_share_could_aim_at_the_supervisor();
    test_the_line_form_is_the_spec_form();
    test_the_folder_answers_in_place();
    std::printf("%d failure(s)\n", g_failures);
    return g_failures == 0 ? 0 : 1;
}
