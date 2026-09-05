package download

import (
	"errors"
	"fmt"
	"os"
)

// ErrFileTooShort means the partial file holds fewer bytes than the checkpoint
// says were proven.
//
// It is a refusal, not a warning, and it is the reason this file exists. The
// obvious reading of "the record and the file disagree" is to believe the
// smaller of the two and carry on from there — which is what this library did.
// That reading is wrong in a way that is hard to see: a file SHORTER than its
// checkpoint is not a file with a shorter proven prefix, it is a file that
// something else has been editing. A temp cleaner truncating it, a copy that
// ran out of disk, a second process writing the same path: none of those leave
// a prefix whose first N bytes are still the artifact's, and taking the smaller
// number silently promotes a corrupt file to a valid resume point.
//
// So the partial is discarded and the transfer starts again from zero. The cost
// is the bytes; the alternative was a file that looked finished and was not.
var ErrFileTooShort = errors.New("download: the partial file is shorter than its checkpoint")

// resumePoint is where a resumed transfer may begin, and what it cost to get
// there.
type resumePoint struct {
	// From is the first byte the next request asks for.
	From int64

	// Discarded is how many bytes were truncated off the tail of the partial
	// because nothing vouched for them. They were written by an owner that
	// vanished before checkpointing, so they are re-downloaded.
	Discarded int64

	// Validators identify the version the kept prefix came from, if the owner
	// that wrote it recorded any. Empty means the resume goes out without an
	// If-Range, which is safe but wastes the whole prefix if the source has
	// changed.
	Validators Validators
}

// resumeAt answers "where may this transfer begin, given what is on disk".
//
// It is a three-way test rather than a minimum, and the three answers are
// genuinely different situations:
//
//   - The file is LONGER than the checkpoint. Normal: a checkpoint is written
//     periodically, so the file is usually ahead of it. The tail past the
//     checkpoint is unproven, so it is truncated away and counted.
//
//   - The file is exactly the checkpoint. Normal: carry on.
//
//   - The file is SHORTER than the checkpoint. Not normal, and not a resume
//     point. See ErrFileTooShort.
//
// A checkpoint of zero needs none of this: there is nothing to keep, so the
// transfer starts at zero whatever is on disk. A partial that is missing
// entirely is a fourth case and not the third one; see below.
func resumeAt(partial string, cp Checkpoint) (resumePoint, error) {
	if cp.VerifiedPrefix <= 0 {
		return resumePoint{}, nil
	}

	st, err := os.Stat(partial)
	switch {
	case os.IsNotExist(err):
		// Not the same as a short file, and the difference is what there is to
		// be wrong about. A file that is THERE and shorter than its checkpoint
		// still has a prefix, and something other than this library shortened
		// it, so no part of that prefix can be believed. A file that is not
		// there has no prefix at all: a temp cleaner took it, and starting from
		// zero is not a compromise, it is the correct and complete answer.
		return resumePoint{}, nil
	case err != nil:
		return resumePoint{}, err
	case st.IsDir():
		return resumePoint{}, fmt.Errorf("download: %s is a directory, not a partial file", partial)
	}

	switch size := st.Size(); {
	case size < cp.VerifiedPrefix:
		return resumePoint{}, fmt.Errorf("%w: %s holds %d bytes, the checkpoint says %d are proven",
			ErrFileTooShort, partial, size, cp.VerifiedPrefix)
	case size > cp.VerifiedPrefix:
		return resumePoint{
			From:       cp.VerifiedPrefix,
			Discarded:  size - cp.VerifiedPrefix,
			Validators: cp.Validators,
		}, nil
	default:
		return resumePoint{From: cp.VerifiedPrefix, Validators: cp.Validators}, nil
	}
}
