package download

import (
	"errors"
	"fmt"
	"os"
)

// ErrFileTooShort means the partial file cannot hold the bytes the checkpoint
// says were proven.
//
// It is a refusal, not a warning, and it is the reason this file exists. The
// obvious reading of "the record and the file disagree" is to believe the
// smaller of the two and carry on from there — which is what this library did.
// That reading is wrong in a way that is hard to see: a file too short for its
// checkpoint is not a file with less proven in it, it is a file that something
// else has been editing. A temp cleaner truncating it, a copy that ran out of
// disk, a second process writing the same path: none of those leave a file
// whose remaining bytes are still the artifact's, and taking the smaller number
// silently promotes a corrupt file to a valid resume point.
//
// So the partial is discarded and the transfer starts again from zero. The cost
// is the bytes; the alternative was a file that looked finished and was not.
var ErrFileTooShort = errors.New("download: the partial file is shorter than its checkpoint")

// fetchRange is one request the runner still has to make.
//
// To is exclusive, and zero means "to the end of whatever the source holds".
// Zero is unambiguous because a range ending at byte zero is empty and would
// never be asked for, and it is the shape the last gap of a download of unknown
// size has to have — the only honest way to ask for "the rest".
type fetchRange struct {
	From int64
	To   int64
}

func (g fetchRange) open() bool { return g.To == 0 }

// resumePlan is what a resumed transfer may do: what is already proven, what is
// still to be fetched, and what it cost to work that out.
type resumePlan struct {
	// Have is the proven set, reconciled against the file on disk. Nothing in
	// here will be fetched again.
	Have Ranges

	// Gaps are the requests still to make, in order, covering everything in
	// [0, size) that Have does not. Empty means the artifact is already whole
	// on disk.
	Gaps []fetchRange

	// Trim is the length the partial must be cut to before anything is written:
	// the highest proven offset. See planResume.
	Trim int64

	// Discarded is how many bytes were cut off the tail of the partial because
	// nothing vouched for them. They were written by an owner that vanished
	// before checkpointing, so they are fetched again.
	Discarded int64

	// Validators identify the version the kept bytes came from, if the owner
	// that wrote them recorded any. Empty means the resume goes out without an
	// If-Range, which is safe but wastes everything kept if the source has
	// changed.
	Validators Validators
}

// From is the first byte the next request asks for: the start of the first gap,
// or the end of the artifact when there are none.
//
// This is the number a person is shown, and it is still the verified prefix —
// the first gap always begins where the leading run of proven bytes stops. That
// it survived the change intact is worth saying out loud, because it is the one
// prefix-shaped quantity that stayed true: "where does the next request start"
// is a question about a prefix by construction.
func (p resumePlan) From() int64 {
	if len(p.Gaps) == 0 {
		return p.Trim
	}
	return p.Gaps[0].From
}

// oneStream reports whether this plan is a single stream running to the end of
// the artifact, starting from a prefix or from nothing.
//
// It is asked for exactly one reason: sha256 is order-dependent. A plan that is
// one stream can be hashed as it is written, which is what this library has
// always done and costs nothing. A plan that fills holes writes bytes out of
// order, so no rolling hash over the write order means anything, and the digest
// has to be taken from the finished file instead. That is a fact about the hash
// function and not about the state, which is why it is a separate question from
// prefixShaped.
func (p resumePlan) oneStream() bool {
	return prefixShaped(p.Have) && len(p.Gaps) == 1 && p.Gaps[0].open()
}

// planResume answers "what may this transfer do, given what is on disk".
//
// # The rule, and why the old one had to go
//
// The old rule compared the file's LENGTH against the checkpoint and produced
// three answers: longer, cut the unproven tail off; equal, carry on; shorter,
// refuse. It worked because a prefix and a length are the same kind of number —
// with one contiguous run of proven bytes starting at zero, "how many bytes are
// proven" and "how far does the file reach" are the same question.
//
// Once bytes land at scattered offsets they stop being the same question, and
// nothing warns you. A sparse file is exactly as long as its furthest written
// byte, so a file holding [0,1MiB) and [99MiB,100MiB) is a hundred megabytes
// long and holds two. Length now says nothing whatever about the holes, and
// every use of it as a measure of progress is silently wrong: `min(checkpoint,
// size)` would report 100 MiB proven, and truncating "the unproven tail" at the
// resume point would delete the second range — a re-download that a length
// check cannot even notice afterwards, because the file would still be the
// right length when it finished.
//
// So length is demoted to the one thing it can still honestly do. **A file's
// length can refute a checkpoint but can never confirm one.** It refutes it
// when the file is too short to physically contain the highest proven byte;
// beyond that it is evidence of capacity, not of content. The checkpoint is
// believed for everything else, because the checkpoint is the only thing that
// ever knew which bytes were checked.
//
// That gives two cases where there were three:
//
//   - size < the highest proven offset. The file cannot hold what the record
//     says is in it. Something outside this library has edited it, so none of
//     what is left can be trusted either. Refuse: ErrFileTooShort.
//
//   - size >= the highest proven offset. The set stands exactly as written.
//     Trim to the highest proven offset — cutting off a tail nothing vouches
//     for, which is the only cut that is safe at any offset — and fetch the
//     gaps.
//
// "Longer" and "equal" collapsed into one because cutting is no longer how
// unproven bytes are dealt with. Under a prefix, deleting them was the only way
// to say "I do not believe these", since their presence WAS the claim. A range
// set says it directly, so unproven bytes below the highest proven offset are
// not deleted, they are overwritten in place by the gap that covers them. What
// survives of the old truncation is the tail above the highest proven offset,
// and it survives for a different reason than it used to have: not because it
// is unproven, but because nothing will be written over it, so leaving it would
// hand back a file longer than the artifact.
//
// # The two cases that are not about length at all
//
// A checkpoint proving nothing needs none of this: there is nothing to keep, so
// the transfer starts at zero whatever is on disk. And a partial that is missing
// entirely is not a short file — a file that is THERE and too short still has
// content, and something other than this library shortened it, so no part of
// that content can be believed; a file that is not there has no content at all,
// a temp cleaner took it, and starting from zero is not a compromise but the
// correct and complete answer.
//
// # size
//
// size is the artifact's length when the caller knows it, and 0 when nobody
// does. Without it there is no last gap to bound and no way to know whether a
// hole past the furthest proven byte exists at all, so the plan degenerates to
// a single open-ended request from the verified prefix — which is precisely
// what this library did before ranges, and remains right, because a source that
// will not tell you how long a file is cannot be asked for the middle of it
// either.
func planResume(partial string, cp Checkpoint, size int64) (resumePlan, error) {
	have, err := cp.Proven()
	if err != nil {
		return resumePlan{}, err
	}

	if len(have) == 0 {
		return resumePlan{Gaps: gapsFor(nil, size)}, nil
	}

	// The highest proven offset. This, and not the count of proven bytes, is
	// what a length can be compared against: a set of ranges occupies a file up
	// to here and says nothing about how much of that span it fills.
	reach := have[len(have)-1].End

	st, err := os.Stat(partial)
	switch {
	case os.IsNotExist(err):
		return resumePlan{Gaps: gapsFor(nil, size)}, nil
	case err != nil:
		return resumePlan{}, err
	case st.IsDir():
		return resumePlan{}, fmt.Errorf("download: %s is a directory, not a partial file", partial)
	}

	if size := st.Size(); size < reach {
		return resumePlan{}, fmt.Errorf("%w: %s holds %d bytes, the checkpoint says byte %d is proven",
			ErrFileTooShort, partial, size, reach-1)
	}

	return resumePlan{
		Have:       have,
		Gaps:       gapsFor(have, size),
		Trim:       reach,
		Discarded:  st.Size() - reach,
		Validators: cp.Validators,
	}, nil
}

// gapsFor turns a proven set into the requests still to make.
//
// The last gap is left OPEN when it runs to the artifact's end, and when the
// size is unknown there is only ever one gap and it is open. That is not an
// optimisation: it is what keeps a single-stream download sending the byte for
// byte identical `Range: bytes=N-` it has always sent, and what lets a source
// that never discloses a length still be resumed. Every gap with proven bytes
// after it is bounded, because asking open-endedly for one would re-fetch the
// proven bytes past it — the exact waste this whole change exists to stop.
func gapsFor(have Ranges, size int64) []fetchRange {
	// A declared size that the proven set already runs past is not a size this
	// can divide into gaps: subtracting the set from [0, size) would come out
	// empty and call a transfer finished that provably is not. The two numbers
	// disagree, the SOURCE is the one entitled to settle it, and the way to ask
	// is the open-ended request — which is what got a 416 and a clean restart
	// before ranges existed, and still does. Treated exactly like an unknown
	// size, because that is what a size contradicted by the record is worth.
	if size <= 0 || (len(have) > 0 && have[len(have)-1].End > size) {
		// Nothing to bound anything against. One request, from wherever the
		// leading proven run stops, running to whatever the source has.
		return []fetchRange{{From: have.VerifiedPrefix()}}
	}
	missing := have.Missing(0, size)
	out := make([]fetchRange, 0, len(missing))
	for _, m := range missing {
		g := fetchRange{From: m.Start, To: m.End}
		if m.End >= size {
			g.To = 0
		}
		out = append(out, g)
	}
	return out
}
