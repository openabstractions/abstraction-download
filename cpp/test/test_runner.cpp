// What a runner decides, which is the half of this contract C++ could not reach
// before. Every case here is a branch the store tier can never observe: the
// ending a failed transfer leaves, the resume point, and whether an adopted job
// honours what somebody asked before it moves a byte.

#include <abstraction/download/runner.h>
#include <abstraction/download/sink.h>
#include <abstraction/job/ranges.h>
#include <abstraction/job/store.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;

using abstraction::download::Runner;
using abstraction::download::sha256_of;
using abstraction::job::FileStore;
using abstraction::job::Json;
using abstraction::job::Record;

static int g_failures = 0;

static void check(const std::string& name, bool ok) {
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", name.c_str());
    if (!ok) {
        ++g_failures;
    }
}

namespace {

std::string artifact(std::size_t n) {
    std::string body(n, '\0');
    for (std::size_t i = 0; i < n; ++i) {
        body[i] = static_cast<char>(i % 251);
    }
    return body;
}

void put(const fs::path& p, const std::string& body) {
    fs::create_directories(p.parent_path());
    std::ofstream out(p, std::ios::binary);
    out.write(body.data(), static_cast<std::streamsize>(body.size()));
}

std::string slurp(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

// A store in its own directory, gone when the test ends.
class Scratch {
public:
    Scratch() {
        dir_ = fs::temp_directory_path() /
               ("abstraction-runner-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(dir_ / "store");
    }
    ~Scratch() {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }
    fs::path dir() const { return dir_; }
    std::string root() const { return (dir_ / "store").string(); }

private:
    fs::path dir_;
};

struct Submitted {
    std::string id;
    std::string partial;
    std::string final_path;
};

Submitted submit(FileStore& store, const std::string& scheme, const std::string& locator,
                 const std::string& digest, std::int64_t size, const std::string& name) {
    Record r;
    r.id = abstraction::job::new_id();
    r.kind = abstraction::download::kKind;
    Json spec;
    spec["artifact"] = Json::object();
    if (size > 0) {
        spec["artifact"]["size"] = size;
    }
    if (!digest.empty()) {
        spec["artifact"]["digest"] = digest;
    }
    spec["sources"] = Json::array({Json{{"scheme", scheme}, {"locator", locator}}});
    const std::string final_path = "models/" + name;
    spec["sink"] = {{"final", final_path},
                    {"partial", abstraction::download::partial_for(final_path, r.id)}};
    r.spec = spec;
    r.progress.total = size;
    const std::string id = store.submit(std::move(r));
    return Submitted{id, "work/" + id, final_path};
}

bool ran(Runner& runner, const std::string& id) {
    try {
        runner.run(id);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

void test_sha256_matches_the_published_vectors() {
    check("sha256 of nothing",
          sha256_of("", 0) ==
              "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    check("sha256 of abc",
          sha256_of("abc", 3) ==
              "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    const std::string long_one(1000000, 'a');
    check("sha256 of a million a",
          sha256_of(long_one.data(), long_one.size()) ==
              "sha256:cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

void test_a_transfer_ends_transferred_and_proven() {
    Scratch s;
    FileStore store(s.root());
    const std::string body = artifact(4096);
    put(s.dir() / "source.bin", body);

    const Submitted job = submit(store, "file", (s.dir() / "source.bin").string(),
                                 sha256_of(body.data(), body.size()), 4096, "a.bin");
    Runner runner(store, "test");
    check("transfer succeeds", ran(runner, job.id));

    const Record after = store.load(job.id);
    check("state is transferred", after.state == abstraction::job::state::kTransferred);
    check("delivered bytes are the artifact", slurp(fs::path(s.root()) / job.final_path) == body);
    check("partial is gone", !fs::exists(fs::path(s.root()) / job.partial));
    check("progress is the whole artifact", after.progress.done == 4096);
    check("checkpoint spells the proven prefix",
          after.checkpoint && after.checkpoint->dump() == "{\"verified_prefix\":4096,\"validators\":{}}");
    check("nobody holds it", !after.lease.held(abstraction::job::Clock::now()));
}

// The bytes arrived and were not what was asked for. A refusal, never a warning
// — and the partial goes, or the next runner resumes onto a prefix already
// known to be wrong.
void test_wrong_bytes_are_refused_and_not_kept() {
    Scratch s;
    FileStore store(s.root());
    put(s.dir() / "source.bin", artifact(512));

    const Submitted job = submit(store, "file", (s.dir() / "source.bin").string(),
                                 "sha256:" + std::string(64, '0'), 512, "b.bin");
    Runner runner(store, "test");
    check("wrong digest fails", !ran(runner, job.id));

    const Record after = store.load(job.id);
    check("bad bytes are deleted", !fs::exists(fs::path(s.root()) / job.partial));
    check("nothing was delivered", !fs::exists(fs::path(s.root()) / job.final_path));
    check("the reason is recorded", !after.error.empty());
    // Not terminal. A mismatch may be a corrupt mirror, and the next sweep is
    // entitled to try the job again against a different source.
    check("still adoptable", !after.terminal());
}

// The whole of the difference between the two endings a failed transfer can
// have: nobody will ever fetch a scheme that does not exist, and a job left
// adoptable is fetched again on every sweep forever.
void test_a_scheme_nobody_serves_ends_the_job() {
    Scratch s;
    FileStore store(s.root());
    const Submitted job = submit(store, "gopher", "gopher://example.invalid/x", "", 0, "c.bin");
    Runner runner(store, "test");
    check("no fetcher fails", !ran(runner, job.id));

    const Record after = store.load(job.id);
    check("permanently", after.state == abstraction::job::state::kFailed);
    check("with a reason", !after.error.empty());
}

// A source that merely was not there is the case this project exists for. A NAS
// that rebooted must not cost anybody their 40 GB.
void test_a_source_that_was_not_there_stays_adoptable() {
    Scratch s;
    FileStore store(s.root());
    const Submitted job = submit(store, "file", (s.dir() / "absent.bin").string(), "", 0, "d.bin");
    Runner runner(store, "test");
    check("a missing source fails", !ran(runner, job.id));

    const Record after = store.load(job.id);
    check("but not terminally", !after.terminal());
    check("and the record says why", !after.error.empty());
}

// Resume, proven by making the two answers differ. The partial holds the true
// first 40 bytes; the SOURCE's first 40 bytes are wrong. A runner that resumes
// delivers the artifact; one that starts again delivers something whose digest
// does not match.
void test_resume_continues_from_what_was_proven() {
    Scratch s;
    FileStore store(s.root());
    const std::string body = artifact(256);
    std::string poisoned = body;
    for (int i = 0; i < 40; ++i) {
        poisoned[i] = 'X';
    }
    put(s.dir() / "source.bin", poisoned);

    const Submitted job = submit(store, "file", (s.dir() / "source.bin").string(),
                                 sha256_of(body.data(), body.size()), 256, "e.bin");
    put(fs::path(s.root()) / job.partial, body.substr(0, 40));
    const Record claimed = store.claim(job.id, "predecessor", std::chrono::seconds(30));
    store.update(job.id, claimed.lease.epoch, [](Record& r) {
        r.progress.done = 40;
        r.checkpoint = Json::parse("{\"verified_prefix\":40}");
    });
    store.release(job.id, claimed.lease.epoch);

    Runner runner(store, "successor");
    check("resumed transfer succeeds", ran(runner, job.id));
    check("the delivered file is the artifact", slurp(fs::path(s.root()) / job.final_path) == body);
}

// The file is the floor: a checkpoint claiming more than the partial holds is
// cut down to what is there, the hash is rebuilt over it, and the transfer
// carries on. The partial is never deleted.
void test_a_partial_shorter_than_its_checkpoint_is_clamped() {
    Scratch s;
    FileStore store(s.root());
    const std::string body = artifact(256);
    put(s.dir() / "source.bin", body);

    const Submitted job = submit(store, "file", (s.dir() / "source.bin").string(),
                                 sha256_of(body.data(), body.size()), 256, "f.bin");
    put(fs::path(s.root()) / job.partial, body.substr(0, 10));
    const Record claimed = store.claim(job.id, "predecessor", std::chrono::seconds(30));
    store.update(job.id, claimed.lease.epoch, [](Record& r) {
        r.progress.done = 40;
        r.checkpoint = Json::parse("{\"verified_prefix\":40}");
    });
    store.release(job.id, claimed.lease.epoch);

    Runner runner(store, "successor");
    check("a short partial resumes", ran(runner, job.id));
    check("the delivered file is the artifact", slurp(fs::path(s.root()) / job.final_path) == body);
}

// An owner died between a pause being asked for and the pause being carried
// out. The next owner's first act is to honour what was asked, not to move
// bytes.
void test_an_adopted_job_honours_a_pause_before_it_fetches() {
    Scratch s;
    FileStore store(s.root());
    const std::string body = artifact(256);
    put(s.dir() / "source.bin", body);

    const Submitted job = submit(store, "file", (s.dir() / "source.bin").string(),
                                 sha256_of(body.data(), body.size()), 256, "g.bin");
    store.claim(job.id, "predecessor", std::chrono::milliseconds(1));
    store.set_intent(job.id, abstraction::job::want::kPause, "a person");

    Runner runner(store, "successor");
    check("honouring a pause is not a failure", ran(runner, job.id));

    const Record after = store.load(job.id);
    check("nothing was fetched", after.progress.done == 0);
    check("nothing was delivered", !fs::exists(fs::path(s.root()) / job.final_path));
    check("and nobody holds it", !after.lease.held(abstraction::job::Clock::now()));
    check("a paused job is not an orphan", store.orphans().empty());
}

void test_cancel_ends_the_job() {
    Scratch s;
    FileStore store(s.root());
    const std::string body = artifact(256);
    put(s.dir() / "source.bin", body);

    const Submitted job = submit(store, "file", (s.dir() / "source.bin").string(),
                                 sha256_of(body.data(), body.size()), 256, "h.bin");
    store.claim(job.id, "predecessor", std::chrono::milliseconds(1));
    store.set_intent(job.id, abstraction::job::want::kCancel, "a person");

    Runner runner(store, "successor");
    check("honouring a cancel is not a failure", ran(runner, job.id));

    const Record after = store.load(job.id);
    check("the job is cancelled", after.state == abstraction::job::state::kCancelled);
    check("nothing was delivered", !fs::exists(fs::path(s.root()) / job.final_path));
    check("and nothing reopens it", after.terminal());
}

// A sink that names the store's own layout overwrites a job record or another
// job's partial. Refused, and refused permanently: no successor will ever make
// that record acceptable.
void test_a_sink_naming_the_store_is_refused() {
    Scratch s;
    FileStore store(s.root());
    Record r;
    r.id = abstraction::job::new_id();
    r.kind = abstraction::download::kKind;
    Json spec;
    spec["artifact"] = Json::object();
    spec["sources"] =
        Json::array({Json{{"scheme", "file"}, {"locator", (s.dir() / "source.bin").string()}}});
    spec["sink"] = {{"final", "jobs/" + r.id + ".json"}, {"partial", "work/" + r.id}};
    r.spec = spec;
    const std::string id = store.submit(std::move(r));

    Runner runner(store, "test");
    check("a reserved sink fails", !ran(runner, id));
    check("permanently", store.load(id).state == abstraction::job::state::kFailed);
}

// The code must not claim a platform it does not serve. On Windows and macOS
// the facility is there; on Linux it is not, and a job with an https source is
// refused by name rather than served by something that skipped a certificate.
void test_https_availability_is_told_truthfully() {
    abstraction::download::Fetchers fetchers = abstraction::download::default_fetchers();
    abstraction::download::Source https;
    https.scheme = "https";
    check("https_available agrees with the fetchers",
          abstraction::download::https_available() == (fetchers.pick(https, {}) != nullptr));
}

// Only when somebody points it at a real URL. A test that reaches the network on
// every run is a test everyone learns to ignore when the network is down.
void test_a_live_https_fetch() {
    const char* url = std::getenv("ABSTRACTION_LIVE_URL");
    const char* want = std::getenv("ABSTRACTION_LIVE_DIGEST");
    if (url == nullptr || want == nullptr) {
        std::printf("[skip] live https — set ABSTRACTION_LIVE_URL and ABSTRACTION_LIVE_DIGEST\n");
        return;
    }
    Scratch s;
    FileStore store(s.root());
    const Submitted job = submit(store, "https", url, want, 0, "live.bin");
    Runner runner(store, "test");
    check("a real https transfer succeeds", ran(runner, job.id));
    check("and is proven",
          store.load(job.id).state == abstraction::job::state::kTransferred);

    // Was the Range honoured, or did the server send the whole file and this
    // side quietly start again? Seeding the partial with a hundred bytes that
    // are NOT the artifact's tells the two apart: a ranged response appended at
    // offset 100 leaves a file whose digest is wrong, and a restart from zero
    // leaves one whose digest is right. Only the refusal proves the range.
    const Submitted ranged = submit(store, "https", url, want, 0, "live-ranged.bin");
    put(fs::path(s.root()) / ranged.partial, std::string(100, 'X'));
    const Record claimed = store.claim(ranged.id, "predecessor", std::chrono::seconds(30));
    store.update(ranged.id, claimed.lease.epoch, [](Record& r) {
        r.progress.done = 100;
        r.checkpoint = Json::parse("{\"verified_prefix\":100}");
    });
    store.release(ranged.id, claimed.lease.epoch);
    check("a ranged resume appends rather than starting again", !ran(runner, ranged.id));
}

}  // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    test_sha256_matches_the_published_vectors();
    test_a_transfer_ends_transferred_and_proven();
    test_wrong_bytes_are_refused_and_not_kept();
    test_a_scheme_nobody_serves_ends_the_job();
    test_a_source_that_was_not_there_stays_adoptable();
    test_resume_continues_from_what_was_proven();
    test_a_partial_shorter_than_its_checkpoint_is_clamped();
    test_an_adopted_job_honours_a_pause_before_it_fetches();
    test_cancel_ends_the_job();
    test_a_sink_naming_the_store_is_refused();
    test_https_availability_is_told_truthfully();
    test_a_live_https_fetch();

    std::printf("\n%s: %d failure(s)\n", g_failures == 0 ? "OK" : "FAILED", g_failures);
    return g_failures == 0 ? 0 : 1;
}
