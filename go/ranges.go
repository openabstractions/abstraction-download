package download

import (
	"encoding/json"

	job "github.com/openabstractions/abstraction-job/go"
)

// This file is how a download checkpoint says "these bytes are proven" when the
// bytes are not a prefix.
//
// The state itself belongs to the job layer, because three languages have to
// spell it identically and the record is the contract between them. Nothing
// here reimplements canonical form, merging or the wire encoding: every write
// goes through job.CheckpointWithRanges, and every read through
// job.RangesFromCheckpoint. What lives here is the part that is this layer's
// business — WHEN a download has something to say that a prefix cannot say, and
// what it does with the answer.

// Range is a half-open byte interval, and Ranges a canonical set of them. Both
// are the job layer's types, aliased so that a caller reading a checkpoint out
// of this package does not have to import two packages to name what it got.
type (
	Range  = job.Range
	Ranges = job.Ranges
)

// Proven is every byte this checkpoint says is proven, canonically.
//
// The prefix is folded in rather than checked against Verified, which is the
// rule the record format sets: both fields are claims that bytes ARE proven and
// neither is a claim that other bytes are not, so their union is the only
// reading that loses nothing. It is also what makes a hand-built
// Checkpoint{VerifiedPrefix: n} — the shape every caller wrote before this
// existed, and the shape the delegated path still writes — read as the one
// range [0, n) without anybody converting it.
func (c Checkpoint) Proven() (Ranges, error) {
	in := make([]Range, 0, len(c.Verified)+1)
	in = append(in, c.Verified...)
	if c.VerifiedPrefix > 0 {
		in = append(in, Range{Start: 0, End: c.VerifiedPrefix})
	}
	return job.CanonicalRanges(in)
}

// prefixShaped reports whether a set says nothing a single prefix could not.
//
// The empty set and one range starting at zero are both prefix-shaped; anything
// else has a hole in it. This is the test for whether a record needs the
// `verified` key at all — see setCheckpoint — and it is deliberately a question
// about the STATE rather than about how the state was reached. A parallel
// fetcher whose parts happen to have joined up is back to being describable by
// one integer, and should be described by one.
func prefixShaped(rs Ranges) bool {
	return len(rs) == 0 || (len(rs) == 1 && rs[0].Start == 0)
}

// setCheckpoint writes a download checkpoint into a record.
//
// Every checkpoint this package writes goes through here, and it decides one
// thing: whether to spend the `verified` key.
//
//   - Prefix-shaped state is written exactly as it was written before ranges
//     existed — `{"verified_prefix":N,...}`, no `verified`, no content model —
//     and the model is withdrawn if an earlier write had declared it. A
//     single-stream download therefore produces the same record bytes it has
//     always produced, which is not politeness: it is what lets this change go
//     out ahead of the other two bindings without any record they compare
//     against moving underneath them.
//
//   - A set with a hole in it cannot be said any other way, so it is written in
//     the job layer's canonical form and the model is declared.
//
// The declaration is carried, not derived — the job layer cannot rediscover it,
// because what it describes is inside a field that is opaque there — so a writer
// that stops having holes to report has to withdraw it, and that is the second
// half of this function rather than something a caller must remember.
func setCheckpoint(rr *job.Record, cp Checkpoint) error {
	rs, err := cp.Proven()
	if err != nil {
		return err
	}
	if prefixShaped(rs) {
		if err := rr.SetCheckpoint(Checkpoint{
			VerifiedPrefix: rs.VerifiedPrefix(),
			Validators:     cp.Validators,
		}); err != nil {
			return err
		}
		withdrawRanges(rr)
		return nil
	}

	// The keys that are not the job layer's go down first, so that
	// CheckpointWithRanges can put the two it owns in front of them and every
	// implementation emits one spelling of one state.
	//
	// Written even when there is nothing to say, because that is what the
	// prefix-shaped path above has always written: `omitempty` on a struct does
	// nothing in Go, so a checkpoint with no validators has carried
	// `"validators":{}` since the field existed. Leaving the key out HERE would
	// mean one record spelling the same absence two ways depending on whether
	// it happened to have holes in it — a difference nothing means, in the one
	// field this project compares byte for byte.
	raw, err := json.Marshal(struct {
		Validators Validators `json:"validators"`
	}{cp.Validators})
	if err != nil {
		return err
	}
	rr.Checkpoint = raw
	return rr.SetCheckpointRanges(rs)
}

// withdrawRanges takes the ranges model back off a record.
//
// Only the declaration has to be removed: the caller has just replaced the
// whole checkpoint with prefix-shaped JSON, so the `verified` key is already
// gone. Doing it this way rather than through job.ClearCheckpointRanges is what
// keeps the prefix-only record byte-for-byte what it used to be — clearing
// re-serialises the checkpoint from a map, which sorts `validators` in front of
// `verified_prefix` and changes bytes nothing asked to change.
func withdrawRanges(rr *job.Record) {
	if !containsModel(rr.Content, job.ModelRanges) {
		return
	}
	kept := make([]string, 0, len(rr.Content))
	for _, name := range rr.Content {
		if name != job.ModelRanges {
			kept = append(kept, name)
		}
	}
	rr.Content = kept
}

func containsModel(list []string, want string) bool {
	for _, name := range list {
		if name == want {
			return true
		}
	}
	return false
}
