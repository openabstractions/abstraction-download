#include <abstraction/download/runner.h>

#include <abstraction/download/credential.h>
#include <abstraction/download/digest.h>
#include <abstraction/download/sink.h>
#include <abstraction/job/awake.h>
#include <abstraction/job/ranges.h>

#include "paths.h"
#include "sha256.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <vector>

namespace abstraction {
namespace download {
namespace {

namespace fs = std::filesystem;

// The checkpoint a single stream writes, in the one spelling every
// implementation must produce for this state.
//
// `validators` is written even when it says nothing. Go's encoder emits it
// unconditionally, the field is compared byte for byte across languages, and one
// state spelled two ways is a record that churns against itself. Two of the
// three implementations therefore carry an empty object forever; see
// feedback/2026-09-06-cpp-runner.md.
void set_prefix_checkpoint(job::Record& r, std::int64_t prefix, const Validators& v) {
    job::Json seen = job::Json::object();
    if (!v.etag.empty()) {
        seen["etag"] = v.etag;
    }
    if (!v.last_modified.empty()) {
        seen["last_modified"] = v.last_modified;
    }
    job::Json cp = job::Json::object();
    cp[job::kVerifiedPrefixKey] = prefix;
    cp["validators"] = std::move(seen);
    r.checkpoint = std::move(cp);
    r.content.erase(
        std::remove(r.content.begin(), r.content.end(), std::string(job::feature::kRanges)),
        r.content.end());
}

Validators validators_of(const job::Record& r) {
    Validators v;
    if (!r.checkpoint || !r.checkpoint->is_object() || !r.checkpoint->contains("validators")) {
        return v;
    }
    const job::Json& seen = r.checkpoint->at("validators");
    if (!seen.is_object()) {
        return v;
    }
    if (seen.contains("etag") && seen.at("etag").is_string()) {
        v.etag = seen.at("etag").get<std::string>();
    }
    if (seen.contains("last_modified") && seen.at("last_modified").is_string()) {
        v.last_modified = seen.at("last_modified").get<std::string>();
    }
    return v;
}

std::string local_root(const job::Store& store) {
    const auto* local = dynamic_cast<const job::LocalStore*>(&store);
    return local == nullptr ? std::string() : local->root();
}

std::string resolve_one(const std::string& root, const std::string& owner, const std::string& p) {
    // In this order because a record's own faults are the same everywhere and a
    // machine's objection is only this machine's.
    if (const std::string refusal = escapes_root(p); !refusal.empty()) {
        throw Error(refusal, true);
    }
    if (const std::string refusal = foreign_path(p); !refusal.empty()) {
        // Not permanent: a sink written in the other platform's convention is
        // unusable here and perfectly usable on the machine whose convention it
        // is, which is a reason to leave the job alone rather than end it.
        throw Error(refusal, false);
    }
    if (const std::string refusal = reserved_sink(owner, p); !refusal.empty()) {
        throw Error(refusal, true);
    }
    if (p.empty() || !relative_everywhere(p)) {
        return p;
    }
    std::string slashed = p;
    std::replace(slashed.begin(), slashed.end(), '\\', '/');
    if (root.empty()) {
        return slashed;
    }
    return utf8_of((path_of(root) / path_of(slashed)).lexically_normal());
}

// Every file this store has already verified against digest: the final of a
// record that transferred these exact bytes, still there at the size it proved.
// The record is the proof, so nothing is re-hashed to offer it.
std::vector<Source> proven(const job::Store& store, const std::string& digest,
                           const std::string& except) {
    std::vector<Source> out;
    if (digest.empty()) {
        return out;
    }
    const std::string want = normal_digest(digest);
    const std::string root = local_root(store);
    std::vector<std::string> seen;
    for (const job::Record& rec : store.list()) {
        if (rec.id == except || rec.kind != kKind ||
            (rec.state != job::state::kTransferred && rec.state != job::state::kComplete)) {
            continue;
        }
        Spec spec;
        std::string final_path;
        try {
            spec = spec_of(rec);
            final_path = resolve_one(root, rec.id, spec.sink.final_path);
        } catch (const std::exception&) {
            continue;
        }
        if (want.empty() || normal_digest(spec.artifact.digest) != want) {
            continue;
        }
        const std::int64_t size = spec.artifact.size > 0 ? spec.artifact.size : rec.progress.done;
        std::error_code ec;
        const fs::path p = path_of(final_path);
        const std::string key = utf8_of(p.lexically_normal());
        if (size <= 0 || !fs::is_regular_file(p, ec) ||
            static_cast<std::int64_t>(fs::file_size(p, ec)) != size ||
            std::find(seen.begin(), seen.end(), key) != seen.end()) {
            continue;
        }
        seen.push_back(key);
        Source src;
        src.scheme = "file";
        src.locator = final_path;
        src.priority = -200 + static_cast<int>(out.size());
        src.attrs = {{"store", "job"}, {"job", rec.id}};
        out.push_back(src);
    }
    return out;
}

void deliver(const fs::path& partial, const fs::path& final_path) {
    std::error_code ec;
    fs::create_directories(final_path.parent_path(), ec);
    fs::rename(partial, final_path, ec);
    if (!ec) {
        return;
    }
    // Rename is atomic within a volume and unavailable across one, so a store on
    // C: delivering to D: falls back to a copy.
    ec.clear();
    fs::copy_file(partial, final_path, fs::copy_options::overwrite_existing, ec);
    if (ec) {
        throw Error("download: cannot deliver to " + utf8_of(final_path) + ": " + ec.message(),
                    false);
    }
    fs::remove(partial, ec);
}

bool is_permanent(const std::exception& e) {
    if (const auto* mine = dynamic_cast<const Error*>(&e)) {
        return mine->permanent();
    }
    return dynamic_cast<const job::Invalid*>(&e) != nullptr;
}

}  // namespace

std::string host_of(const std::string& locator) {
    std::string rest;
    if (const auto at = locator.find("://"); at != std::string::npos) {
        rest = locator.substr(at + 3);
    } else if (locator.rfind("\\\\", 0) == 0 || locator.rfind("//", 0) == 0) {
        rest = locator.substr(2);
    } else {
        return "";
    }
    rest = rest.substr(0, rest.find_first_of("/\\?#"));
    if (const auto at = rest.rfind('@'); at != std::string::npos) {
        rest = rest.substr(at + 1);
    }
    if (!rest.empty() && rest[0] == '[') {
        if (const auto close = rest.find(']'); close != std::string::npos) {
            rest = rest.substr(1, close - 1);
        }
    } else if (const auto colon = rest.rfind(':'); colon != std::string::npos) {
        rest = rest.substr(0, colon);
    }
    for (char& c : rest) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return rest;
}

Runner::Runner(job::Store& store, std::string owner) : store_(store), owner_(std::move(owner)) {}

void Runner::run(const std::string& id) {
    const job::Record claimed = store_.claim(id, owner_, lease_ttl);
    const std::int64_t epoch = claimed.lease.epoch;
    job::KeepAwake awake(store_, claimed);
    try {
        execute(claimed, epoch);
    } catch (const std::exception& e) {
        const bool permanent = is_permanent(e);
        try {
            store_.update(id, epoch, [&](job::Record& r) {
                r.error = e.what();
                // A refusal that stays adoptable is fetched again on every sweep
                // for as long as the store exists, and nothing waiting on the
                // record can ever stop waiting.
                if (permanent) {
                    r.state = job::state::kFailed;
                }
            });
        } catch (const job::JobError&) {
        }
        try {
            store_.release(id, epoch);
        } catch (const job::JobError&) {
        }
        throw;
    }
    // Let go. The bytes are proven and the only thing left is for whoever wanted
    // them to claim the job and say so, which a held lease blocks.
    try {
        store_.release(id, epoch);
    } catch (const job::JobError&) {
    }
}

void Runner::execute(const job::Record& rec, std::int64_t epoch) {
    // Before anything is fetched, because this owner may have just adopted a job
    // whose predecessor died between the pause being asked for and the pause
    // being carried out. That record is left running under a lapsed lease, and
    // the only way out of it is to honour what was asked rather than start
    // moving bytes and find out at the first checkpoint.
    if (rec.wants() != job::want::kRun) {
        honour(rec.wants(), rec.id, epoch);
        return;
    }

    Spec spec = spec_of(rec);
    // Whatever this store has already proven goes ahead of every source the
    // record names: on the machine that adopts a delegated job, that is the
    // delegate's own earlier delivery, which the record cannot name.
    std::vector<Source> held = proven(store_, spec.artifact.digest, rec.id);
    spec.sources.insert(spec.sources.begin(), held.begin(), held.end());
    std::string partial;
    std::string final_path;
    resolve(rec.id, spec.sink, partial, final_path);
    std::error_code ec;
    fs::create_directories(path_of(partial).parent_path(), ec);

    Transferred got;
    try {
        got = transfer(rec, spec, epoch, partial);
    } catch (const Stopped& stopped) {
        return honour(stopped.want, rec.id, epoch);
    }

    // Length before digest: a short transfer has a more useful error than "the
    // hash is wrong".
    if (spec.artifact.size > 0 && got.total != spec.artifact.size) {
        keep_proven(rec.id, epoch, got.total, got.validators);
        throw Error("download: source ended early: got " + std::to_string(got.total) +
                        " bytes, expected " + std::to_string(spec.artifact.size),
                    false);
    }

    // A digest nobody can read is not a digest that matches anything. Comparing
    // the normalised forms is what stops "sha256:1fc70f… want 1fc70f…" — the
    // same digest twice — deleting a correct 1.5 GB download; refusing an
    // unreadable one is what stops two unreadable digests comparing equal.
    const std::string want = normal_digest(spec.artifact.digest);
    if (!spec.artifact.digest.empty() && want != got.digest) {
        // Do not keep bytes that failed: leaving them means the next runner
        // resumes onto a prefix already known to be wrong.
        fs::remove(path_of(partial), ec);
        try {
            store_.update(rec.id, epoch, [](job::Record& r) {
                r.progress.done = 0;
                set_prefix_checkpoint(r, 0, Validators());
            });
        } catch (const job::JobError&) {
        }
        throw Error("download: digest mismatch: got " + got.digest + ", want " + want, false);
    }

    deliver(path_of(partial), path_of(final_path));

    // Transferred, not complete: the bytes are here and proven, and whoever
    // wanted them has not said so yet.
    store_.update(rec.id, epoch, [&](job::Record& r) {
        r.progress.done = got.total;
        r.progress.updated_at = job::Clock::now();
        r.state = job::state::kTransferred;
        r.error.clear();
        set_prefix_checkpoint(r, got.total, got.validators);
    });
}

// keep_proven writes down what a failed attempt reached, before the failure is
// recorded.
//
// The periodic checkpoint fires on a byte count or an interval, so a transfer
// that stops before the first one has proven nothing as far as the store is
// concerned — and the next owner re-fetches bytes sitting on the disk in front
// of it. That is a rounding error on a 64-byte scenario and the whole product
// on a 40 GB model over a link that drops every ten minutes.
//
// Never downwards: progress.done is the total of every proven range and this
// offers only a prefix, so a checkpoint that already knows more keeps it.
void Runner::keep_proven(const std::string& id, std::int64_t epoch, std::int64_t prefix,
                         const Validators& seen) {
    if (prefix <= 0) {
        return;
    }
    try {
        store_.update(id, epoch, [&](job::Record& r) {
            if (r.progress.done >= prefix) {
                return;
            }
            r.progress.done = prefix;
            r.progress.updated_at = job::Clock::now();
            set_prefix_checkpoint(r, prefix, seen);
        });
    } catch (const job::JobError&) {
    }
}

void Runner::resolve(const std::string& owner_id, const Sink& sink, std::string& partial,
                     std::string& final_path) const {
    if (shared_store) {
        for (const std::string& p : {sink.partial, sink.final_path}) {
            const std::string refusal = unportable_sink(p);
            if (!refusal.empty()) {
                // Not permanent: the record is valid on the machine whose
                // filesystem the path names, and this is this machine's policy
                // about a shared store.
                throw Error(refusal, false);
            }
        }
    }
    const std::string root = local_root(store_);
    partial = resolve_one(root, owner_id, sink.partial);
    final_path = resolve_one(root, owner_id, sink.final_path);
}

void Runner::honour(const std::string& want, const std::string& id, std::int64_t epoch) {
    if (want == job::want::kCancel) {
        store_.update(id, epoch, [](job::Record& r) {
            r.state = job::state::kCancelled;
            r.error.clear();
        });
        return;
    }
    if (want == job::want::kPause) {
        // Release rather than hold. Paused means nobody is working on it, and a
        // held lease tells every reader the opposite. Letting go is safe only
        // because a sweep excludes paused jobs; the two decisions work as a pair.
        store_.release(id, epoch);
    }
}

Runner::Transferred Runner::transfer(const job::Record& rec, const Spec& spec, std::int64_t epoch,
                                     const std::string& partial) {
    const fs::path p = path_of(partial);
    std::error_code ec;
    std::int64_t proven = job::verified_prefix(job::checkpoint_ranges(rec));
    const bool present = fs::exists(p, ec);
    Validators seen = validators_of(rec);

    const auto forget = [&](std::int64_t at, const Validators& v) {
        try {
            store_.update(rec.id, epoch, [&](job::Record& r) {
                r.progress.done = at;
                r.progress.updated_at = job::Clock::now();
                set_prefix_checkpoint(r, at, v);
            });
        } catch (const job::JobError&) {
        }
    };

    // The file is the floor. A partial that is missing holds nothing, so the
    // resume point is zero; one that is there and too short holds what it holds,
    // so the claim is cut down to that and the hash below is rebuilt over it.
    // Deleting a prefix that is about to be verified anyway is 40 GB fetched
    // twice, which is the complaint this layer exists to answer.
    if (!present) {
        proven = 0;
    } else {
        proven = std::min(proven, static_cast<std::int64_t>(fs::file_size(p, ec)));
    }
    if (present) {
        fs::resize_file(p, static_cast<std::uintmax_t>(proven), ec);
    }
    if (rec.progress.done != proven) {
        forget(proven, seen);
    }

    Sha256 hash;
    if (proven > 0) {
        // The cost of resuming honestly: a sequential read of what we already
        // have, at disk speed, instead of re-downloading it at network speed.
        std::ifstream in(p, std::ios::binary);
        std::vector<char> buf(1 << 20);
        std::int64_t left = proven;
        while (left > 0 && in) {
            in.read(buf.data(), static_cast<std::streamsize>(
                                    std::min<std::int64_t>(left, static_cast<std::int64_t>(buf.size()))));
            const std::streamsize got = in.gcount();
            if (got <= 0) {
                break;
            }
            hash.write(buf.data(), static_cast<std::size_t>(got));
            left -= got;
        }
        if (left > 0) {
            throw Error("download: the partial file is shorter than its checkpoint: " + partial,
                        false);
        }
    }

    if (!present) {
        std::ofstream create(p, std::ios::binary);
    }
    std::fstream out(p, std::ios::binary | std::ios::in | std::ios::out);
    if (!out) {
        throw Error("download: cannot write " + partial, false);
    }

    std::int64_t base = proven;
    std::int64_t landed = 0;
    std::int64_t last_persist = proven;
    auto last_at = std::chrono::steady_clock::now();

    const auto restart = [&]() {
        // A source has told us the artifact it holds is not the one these bytes
        // came from, so everything derived from them goes: the file back to
        // nothing, the hash back to empty, the recorded prefix to zero.
        out.close();
        fs::resize_file(p, 0, ec);
        out.open(p, std::ios::binary | std::ios::in | std::ios::out);
        hash = Sha256();
        base = 0;
        landed = 0;
        last_persist = 0;
        last_at = std::chrono::steady_clock::now();
        seen = Validators();
        forget(0, Validators());
    };

    const auto note = [&](std::int64_t at, std::int64_t total) {
        // Bytes OR time, whichever comes first. The byte threshold keeps a fast
        // link from writing the record constantly; the interval keeps a slow one
        // from never writing it at all.
        const auto now = std::chrono::steady_clock::now();
        const bool enough = at - last_persist >= persist_every;
        const bool overdue = persist_interval.count() > 0 && now - last_at >= persist_interval;
        if (!enough && !overdue) {
            return;
        }
        last_persist = at;
        last_at = now;
        // Durability before the claim: recording "the first N bytes are proven"
        // while N of them sit in a buffer would make a crash resume from bytes
        // that were never written.
        out.flush();
        const job::Record updated = store_.update(rec.id, epoch, [&](job::Record& r) {
            r.progress.done = at;
            // Only ever fill an unknown size in. A caller that supplied one has
            // better information than a response header, and a source that lies
            // about its length must not overwrite the number the digest was
            // chosen against.
            if (r.progress.total == 0 && total > 0) {
                r.progress.total = total;
            }
            r.progress.updated_at = job::Clock::now();
            set_prefix_checkpoint(r, at, seen);
        });
        // The record was just read and written, so what somebody wants is in
        // hand at no extra cost. Stopping here rather than at the end of the
        // transfer is the difference between a pause button that works and one
        // that takes effect in forty minutes.
        if (updated.wants() != job::want::kRun) {
            throw Stopped{updated.wants()};
        }
        store_.renew(rec.id, epoch, lease_ttl);
    };

    std::string last_error;
    bool last_permanent = false;
    bool served = false;

    for (const Source& src : spec.sources) {
        Fetcher* fetcher = fetchers.pick(src, rec.requires_capabilities);
        if (fetcher == nullptr) {
            const std::string why =
                "download: no fetcher for this job's sources: scheme \"" + src.scheme + "\"";
            // A source that says no does not speak for a mirror that merely
            // dropped the connection, so a retryable failure anywhere in the
            // list outranks a refusal that happened to come last.
            if (last_error.empty() || last_permanent) {
                last_error = why;
                last_permanent = true;
            }
            continue;
        }
        if (reach) {
            const std::string host = host_of(src.locator);
            const std::string why = host.empty() ? std::string() : reach(host);
            if (!why.empty()) {
                // Not permanent: the refusal is this machine's, not the job's.
                last_error = "download: this machine will not reach " + host + ": " + why;
                last_permanent = false;
                continue;
            }
        }

        Request req;
        req.source = src;
        req.headers = src.headers;
        const std::string unbound = attach_credential(src.attrs, host_of(src.locator), req.headers);
        if (!unbound.empty()) {
            last_error = unbound;
            last_permanent = false;
            continue;
        }
        req.from = base;
        req.validators = seen;
        req.restart = restart;
        req.observed = [&](const Validators& v) { seen = v; };
        req.out = [&](const char* data, std::size_t n) {
            out.write(data, static_cast<std::streamsize>(n));
            hash.write(data, n);
            landed += static_cast<std::int64_t>(n);
        };
        req.report = [&](std::int64_t, std::int64_t total) { note(base + landed, total); };

        out.seekp(static_cast<std::streamoff>(base));
        landed = 0;
        last_persist = base;
        last_at = std::chrono::steady_clock::now();
        try {
            fetcher->fetch(req);
            served = true;
            break;
        } catch (const Error& e) {
            if (landed > 0) {
                // This source contributed bytes before it failed. Stop rather
                // than letting the next one write over a hole it left mid-write.
                out.flush();
                keep_proven(rec.id, epoch, base + landed, seen);
                throw;
            }
            if (last_error.empty() || last_permanent || !e.permanent()) {
                last_error = e.what();
                last_permanent = e.permanent();
            }
        }
    }

    out.flush();
    if (!served) {
        throw Error(last_error.empty() ? "download: no fetcher for this job's sources" : last_error,
                    last_error.empty() ? true : last_permanent);
    }
    out.close();
    return Transferred{base + landed, hash.digest(), seen};
}

}  // namespace download
}  // namespace abstraction
