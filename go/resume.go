package download

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	job "github.com/openabstractions/abstraction-job/go"
)

// Resume is downloading keyed by DESTINATION rather than by job id.
//
// Submit answers "start this work and give me its id". That is the right
// question for a program that keeps the id — a UI, a service, a queue. It is
// the wrong question for a program that is run again from a shell, because such
// a program has no id to keep: it has a URL and a path, and what it means by
// "resume" is "the file at this path is half here, carry on with it".
//
// Both existing adopters wrote this on top of the library rather than getting it
// from it: the ComfyUI node's _resume_or_submit and the same loop in the
// Lemonade fork. A caller that does not write it gets a new record with a
// verified prefix of zero on every invocation, no Range request, and no resume.
type Resume interface {
	// ResumeOrSubmit returns the job for spec's destination, continuing the
	// existing one if there is one. Call it instead of Submit whenever the
	// caller identifies work by where the bytes land.
	//
	// It reports what it decided; see Continuation, and the rules below.
	ResumeOrSubmit(spec Spec, requires ...string) (job.Job, Continuation, error)

	// ResumeOrGet is ResumeOrSubmit for a caller holding a URL and a path, in
	// the shape of Get. A destination that names a directory takes its filename
	// from the source.
	ResumeOrGet(source, destination string) (job.Job, Continuation, error)
}

// Disposition is what ResumeOrSubmit did.
type Disposition string

const (
	// Submitted: nothing in the store was working on this destination, so a new
	// record was created. This is the only disposition that means no bytes
	// survive from an earlier attempt.
	Submitted Disposition = "submitted"

	// Resumed: an unfinished record for this destination was adopted.
	// Continuation.ResumeFrom says how many bytes of it survive.
	Resumed Disposition = "resumed"

	// Delivered: the artifact is already at the destination and no bytes need to
	// move. The returned job may still be waiting for TakeDelivery.
	Delivered Disposition = "delivered"

	// Busy: an unfinished record was adopted, and another owner holds its lease
	// right now. The lease was not taken and no work was started here. The
	// returned handle is that work — watch it. If the owner is dead its lease
	// lapses within the runner's LeaseTTL and the ordinary reclamation path
	// picks the job up.
	Busy Disposition = "busy"

	// Paused: an unfinished record was adopted, and somebody asked it to stop.
	// Nothing was started; resume it through the job handle (job.Pausable)
	// before expecting bytes to move.
	Paused Disposition = "paused"
)

// Continuation is what ResumeOrSubmit decided, in a form a caller can print.
//
// Nothing here is a branch a caller must take: the returned job.Job is correct
// whatever this says. It exists because every decision below is one a person
// running a command is entitled to see rather than have made silently.
type Continuation struct {
	// Disposition is which of the cases above applied.
	Disposition Disposition

	// ResumeFrom is the byte the next request will actually ask for, measured
	// against the filesystem and not against the record. A record claiming a
	// verified prefix of 900 whose partial file holds 100 bytes — or none —
	// yields 0, because two numbers that disagree do not average into a byte
	// anyone proved.
	//
	// It is the start of the first hole, which is still the verified prefix,
	// and it is no longer the same thing as how much of the artifact is here.
	// See Proven.
	ResumeFrom int64

	// Proven is how many bytes of the artifact are actually held, which is
	// ResumeFrom plus anything proven beyond the first hole.
	//
	// The two were one number while progress was a prefix. They stop being one
	// the moment a transfer has holes: a resume that begins at byte 0 because
	// the first megabyte is missing may still be holding thirty-nine gigabytes.
	// Reporting only ResumeFrom would tell that person their download had
	// started over.
	Proven int64

	// Discarded is how many bytes on disk are being thrown away to get to
	// ResumeFrom: the tail an owner wrote and never checkpointed, or the whole
	// partial when it disagreed with the record. They will be fetched again.
	//
	// Reported because it is the difference between a resume that saves nearly
	// everything and one that saves nothing, and a person watching a download
	// apparently start over is entitled to the number rather than a guess.
	Discarded int64

	// Source is the locator the adopted job will fetch from, which is not
	// necessarily the one just passed in. See SourceChanged.
	Source string

	// SourceChanged reports that an existing record was adopted whose first
	// source differs from the caller's.
	//
	// The destination is the identity, so this is the same work: a URL changes
	// when a mirror is configured (HF_ENDPOINT does exactly this), when a
	// signed link is reissued, or when a redirect resolves differently, and
	// none of those are a different file. The adopted record keeps its OWN
	// sources, so the partial on disk stays consistent with what wrote it — a
	// caller that meant a genuinely different artifact should cancel this job
	// and submit to a different path rather than have its bytes appended to
	// somebody else's prefix.
	SourceChanged bool

	// Note is one line of display text saying the same thing. Never parse it.
	Note string
}

// ResumeOrGet implements Resume.
func (s *service) ResumeOrGet(source, destination string) (job.Job, Continuation, error) {
	spec, err := specFor(source, destination)
	if err != nil {
		return nil, Continuation{}, err
	}
	return s.ResumeOrSubmit(spec)
}

// ResumeOrSubmit implements Resume.
//
// The rules, in the order they are applied:
//
//   - A destination is required. Identity is the destination, so there is no
//     meaning to this call without one; Submit is for a caller that has a digest
//     and lets the store choose a path.
//
//   - Two spellings of one destination are one record. The path is made
//     absolute, cleaned, and its parent directory resolved through symlinks;
//     on Windows it is also case-folded. What that cannot see: a file reached
//     through two mounts of one filesystem, a hardlink, a drive letter mapped
//     to a UNC share, an 8.3 short name, and — on a case-insensitive macOS or
//     Linux volume — two spellings differing only in case. Each of those
//     yields two records for one file, which is the failure this call exists
//     to remove, so a caller that can hand over one spelling should.
//
//   - An unfinished record for that destination is continued, whatever its
//     source. See Continuation.SourceChanged.
//
//   - COMPLETE means the file is there, and that is checked rather than
//     believed: a complete record whose file has been moved or deleted is
//     history, and a new job is created. FAILED and CANCELLED are history too —
//     running a command again is a new request, not an appeal against the last
//     one — and neither carries bytes forward, because nothing proved them.
//
//   - A lease held by another owner is left alone: nothing is claimed here, and
//     the record comes back with Disposition Busy. This process offers to take
//     the work over only once that lease lapses of its own accord, which is what
//     recovers a download whose owner was killed rather than stopped.
//
//   - The resume point is the checkpoint's proven ranges, checked against the
//     partial file only for whether the file is long enough to hold them: one
//     that is not has no resume point at all and the transfer starts again from
//     zero, and one that is has its tail above the highest proven byte
//     discarded. A vanished or truncated partial therefore cannot make a runner
//     ask a server to continue from bytes that are not there. The file's length
//     is never read as progress; see planResume.
//
//   - Two callers racing for one destination produce one record, because the
//     record id is derived from the destination and both store bindings refuse
//     to create an id twice. The loser of the race loads the winner's record
//     and continues it.
func (s *service) ResumeOrSubmit(spec Spec, requires ...string) (job.Job, Continuation, error) {
	if err := spec.Validate(); err != nil {
		return nil, Continuation{}, err
	}
	dest := s.destinationOf(spec.Sink)
	if dest == "" {
		return nil, Continuation{}, fmt.Errorf("download: ResumeOrSubmit needs a destination")
	}

	live, done := s.recordsFor(dest)
	if live != nil {
		c := s.continuation(live, spec)
		if c.Disposition == Resumed || c.Disposition == Busy {
			// begin never bypasses a lease: with a supervisor it only nudges,
			// and without one it retries Claim until the current lease lapses of
			// its own accord. See service.begin and service.runHere.
			s.begin(live.ID)
		}
		return s.Open(live.ID), c, nil
	}
	if done != nil {
		return s.Open(done.ID), Continuation{
			Disposition: Delivered,
			Source:      firstLocator(done),
			Note:        "already downloaded",
		}, nil
	}

	spec = s.alreadyHere(spec)
	id, created, err := s.claimDestination(dest, spec, requires)
	if err != nil {
		return nil, Continuation{}, err
	}
	if !created {
		// Somebody won the race between the scan above and the create. Their
		// record is the one record for this destination, so continue it.
		rec, lerr := s.runner.Store.Load(id)
		if lerr != nil {
			return nil, Continuation{}, lerr
		}
		c := s.continuation(rec, spec)
		if c.Disposition == Resumed || c.Disposition == Busy {
			s.begin(id)
		}
		return s.Open(id), c, nil
	}
	s.begin(id)
	return s.Open(id), Continuation{
		Disposition: Submitted,
		Source:      spec.Sources[0].Locator,
		Note:        "starting a new download",
	}, nil
}

// continuation decides what an existing unfinished record means for this call.
func (s *service) continuation(rec *job.Record, want Spec) Continuation {
	c := Continuation{Disposition: Resumed, Source: firstLocator(rec)}
	c.SourceChanged = len(want.Sources) > 0 && c.Source != "" && c.Source != want.Sources[0].Locator

	if rec.State == job.StateTransferred && s.destinationExists(rec) {
		c.Disposition = Delivered
		c.Note = "already downloaded, waiting to be taken delivery of"
		return c
	}

	c.ResumeFrom, c.Proven, c.Discarded = s.resumeFrom(rec)
	switch {
	case rec.Paused():
		c.Disposition = Paused
		c.Note = "an existing download to this path is paused"
	case !s.runner.Store.Claimable(rec) && rec.Lease.Owner != s.runner.Owner:
		c.Disposition = Busy
		c.Note = fmt.Sprintf("%s is already downloading to this path", rec.Lease.Owner)
	case c.ResumeFrom > 0:
		c.Note = fmt.Sprintf("continuing an existing download from byte %d", c.ResumeFrom)
	default:
		c.Note = "continuing an existing download from the beginning"
	}
	if c.Proven > c.ResumeFrom && c.Disposition == Resumed {
		// The number a person needs when their download resumes from a low
		// offset and is nonetheless nearly finished. Without it the display
		// says "continuing from byte 0" for a transfer holding all but its
		// first megabyte, which reads as starting over.
		c.Note += fmt.Sprintf("; %d bytes past that are already proven and will be skipped", c.Proven-c.ResumeFrom)
	}
	if c.Discarded > 0 && c.Disposition == Resumed {
		c.Note += fmt.Sprintf("; %d bytes on disk are unproven and will be fetched again", c.Discarded)
	}
	if c.SourceChanged {
		c.Note += "; it fetches from " + c.Source
	}
	return c
}

// resumeFrom is what the runner will actually do, worked out the same way the
// runner works it out — by planResume, so that what a person is told here and
// what happens next cannot drift apart.
//
// Three numbers rather than two, because the file's length stopped implying
// progress. `from` is where the next request begins; `proven` is how much of
// the artifact is held, which is a different number the moment there are holes
// past the first one; `discarded` is the tail above the highest proven byte,
// which is the only part of the file that is actually thrown away.
//
// A file too short to hold what its checkpoint claims is a different situation
// again: a temp cleaner, a half-finished copy onto a full disk, a user tidying
// up. The old answer was to believe the smaller of the two and carry on from
// there, which quietly turned "these two disagree" into a lower offset and a
// resume onto bytes nothing vouches for. That case has no resume point at all —
// the partial is discarded and the transfer starts again — so this reports zero
// proven and the whole file discarded.
func (s *service) resumeFrom(rec *job.Record) (from, proven, discarded int64) {
	spec, err := SpecOf(rec)
	if err != nil {
		return 0, 0, 0
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		return 0, 0, 0
	}
	partial, _ := LocalSink(s.runner.Store, spec.Sink)
	plan, err := planResume(partial, cp, spec.Artifact.Size)
	if err != nil {
		// No resume point. Whatever is on disk is going, and how much of it
		// there is is exactly what a person wants to be told.
		if st, serr := os.Stat(partial); serr == nil && !st.IsDir() {
			return 0, 0, st.Size()
		}
		return 0, 0, 0
	}
	return plan.From(), plan.Have.Total(), plan.Discarded
}

// destinationExists reports whether the record's final path holds a file.
func (s *service) destinationExists(rec *job.Record) bool {
	spec, err := SpecOf(rec)
	if err != nil {
		return false
	}
	_, final := LocalSink(s.runner.Store, spec.Sink)
	st, err := os.Stat(final)
	return err == nil && !st.IsDir()
}

// recordsFor finds this destination's record.
//
// It returns two things because the two are treated differently: an unfinished
// record is work to continue, and a COMPLETE one is only evidence that the file
// might already be there. The scan is over every record rather than over ids
// this package would have chosen, so a job submitted by an older version, by the
// CLI, or by another implementation is still found.
func (s *service) recordsFor(dest string) (live, complete *job.Record) {
	all, err := s.runner.Store.List()
	if err != nil {
		return nil, nil
	}
	for _, rec := range all {
		if rec.Kind != Kind {
			continue
		}
		spec, err := SpecOf(rec)
		if err != nil {
			continue
		}
		if s.destinationOf(spec.Sink) != dest {
			continue
		}
		switch {
		case !rec.State.Terminal():
			live = rec
		case rec.State == job.StateComplete && s.destinationExists(rec):
			complete = rec
		}
	}
	return live, complete
}

// claimDestination creates the one record for this destination, or returns the
// id of the record somebody else created first.
//
// The exclusion is the store's, not this package's: Submit refuses an id that
// already exists — O_EXCL in the file binding, a map check in the memory one —
// so deriving the id from the destination turns two concurrent creates into one
// winner and one loser that can read what the winner wrote.
//
// The generation suffix is what keeps that compatible with "download it again":
// a destination whose first record ended failed, cancelled, or complete-with-no
// file needs a second record, and it cannot have the same id as the first.
func (s *service) claimDestination(dest string, spec Spec, requires []string) (id string, created bool, err error) {
	base := destinationID(dest)
	for gen := 1; gen <= 512; gen++ {
		id = base
		if gen > 1 {
			id = fmt.Sprintf("%s-%d", base, gen)
		}
		got, serr := submitAs(s.runner.Store, id, spec, requires...)
		if serr == nil {
			return got, true, nil
		}
		// Taken. Either somebody just created it, in which case it is the record
		// for this destination and the caller continues it, or it is spent
		// history from an earlier run and the next generation is free.
		rec, lerr := s.runner.Store.Load(id)
		if lerr != nil {
			return "", false, serr
		}
		if !rec.State.Terminal() {
			return id, false, nil
		}
	}
	return "", false, fmt.Errorf("download: %s has too many spent records to start another", dest)
}

// destinationID is the record id for a destination: stable, and shaped so that
// it cannot collide with the timestamped ids job.NewID produces.
//
// It sorts after those ids rather than among them, so a listing that orders by
// id puts records made this way at the end. Anything that wants creation order
// has Record.CreatedAt.
func destinationID(dest string) string {
	sum := sha256.Sum256([]byte(dest))
	return "dest-" + hex.EncodeToString(sum[:8])
}

// destinationOf reduces a sink to the one string that identifies its file.
//
// A relative sink means "under the store's own area", so it is resolved the same
// way the runner will resolve it — otherwise a record written as `out/x.bin` and
// a call naming the absolute path of that same file would be two records.
func (s *service) destinationOf(sink Sink) string {
	_, final := LocalSink(s.runner.Store, sink)
	return canonicalPath(final)
}

// canonicalPath is this package's answer to "are these two strings the same
// file", and its limits are listed on ResumeOrSubmit.
//
// The parent directory is resolved through symlinks rather than the file itself,
// because the file is usually not there yet — that is the whole situation. A
// directory that cannot be resolved is used as written, which is right for a
// destination whose directory has still to be created.
func canonicalPath(p string) string {
	if strings.TrimSpace(p) == "" {
		return ""
	}
	p = filepath.FromSlash(p)
	if abs, err := filepath.Abs(p); err == nil {
		p = abs
	}
	p = filepath.Clean(p)
	dir, base := filepath.Split(p)
	if real, err := filepath.EvalSymlinks(filepath.Clean(dir)); err == nil {
		p = filepath.Join(real, base)
	}
	if runtime.GOOS == "windows" {
		// Windows filenames are case-insensitive, so folding here merges two
		// spellings of one file. Not done elsewhere: a case-sensitive Linux
		// volume holds `X.bin` and `x.bin` as two files, and folding them
		// together would point two downloads at one record.
		p = strings.ToLower(p)
	}
	return p
}

func firstLocator(rec *job.Record) string {
	spec, err := SpecOf(rec)
	if err != nil || len(spec.Sources) == 0 {
		return ""
	}
	return spec.Sources[0].Locator
}
