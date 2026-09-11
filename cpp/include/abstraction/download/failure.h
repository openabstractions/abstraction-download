// What a failed download means, and how a record carries it.
//
// download has exactly two endings and they are the whole retry model: a
// refusal ends the job, and anything else leaves it adoptable for the next
// runner to resume. Locally that is a dynamic_cast; across a process, a
// provider or a language it is nothing at all, because `error` is prose and a
// sentence is not a class. So the class travels beside the sentence, in the
// shape abstraction-download/download.thrift declares and the generated codec writes.

#ifndef ABSTRACTION_DOWNLOAD_FAILURE_H
#define ABSTRACTION_DOWNLOAD_FAILURE_H

#include <abstraction/download/cpp/rec.h>
#include <abstraction/download/fetcher.h>
#include <abstraction/job/record.h>

#include <exception>
#include <optional>
#include <string>

namespace abstraction {
namespace download {

// The name a record carries the payload under, in `extensions` and therefore in
// `content`. The definition's, not a second copy of it: the key IS the
// payload's version, so a change to the shape is a change to both.
inline const std::string& failure_extension() { return rec::kFailureNames.front(); }

// Whether trying this job again, unchanged, is pointless. Two names and not a
// membership list: this layer's own refusals say so where they are thrown, and
// the job layer's Invalid means the record itself will never be readable, which
// no successor can improve on.
inline bool is_permanent(const std::exception& e) {
    if (const auto* mine = dynamic_cast<const Error*>(&e)) {
        return mine->permanent();
    }
    return dynamic_cast<const job::Invalid*>(&e) != nullptr;
}

// Record why an attempt ended AND whether trying again could ever help. Both,
// together, because the caller reads them back as one thing.
//
// The bytes are the generated encoder's, so the field order and the escaping
// are the definition's in every language rather than this one's.
inline void set_failure(job::Record& r, const std::exception& e) {
    r.error = e.what();
    rec::Failure f;
    f.error = e.what();
    f.permanent = is_permanent(e);
    r.extensions[failure_extension()] = job::Json::parse(rec::encode(f));
}

// Take back both halves. Leaving the class behind when the sentence goes would
// answer "is this over" about an attempt nobody can read.
inline void clear_failure(job::Record& r) {
    r.error.clear();
    r.extensions.erase(failure_extension());
}

// The error a record's last attempt ended with, class intact, or nothing for a
// record that has not failed.
//
// A record written before this key existed, or by a writer that does not know
// it, yields a retryable error -- the same answer that record has always given.
// So does a payload this reader will not stand behind: the definition refuses
// an unknown field rather than granting it, so an unreadable class and an
// absent one are one answer and neither is a guess.
inline std::optional<Error> last_failure(const job::Record& r) {
    if (r.error.empty()) {
        return std::nullopt;
    }
    if (r.extensions.contains(failure_extension())) {
        try {
            const rec::Failure f = rec::decode(r.extensions.at(failure_extension()).dump());
            if (!f.error.empty()) {
                return Error(f.error, f.permanent);
            }
        } catch (const rec::Refusal&) {
        }
    }
    return Error(r.error, false);
}

}  // namespace download
}  // namespace abstraction

#endif
