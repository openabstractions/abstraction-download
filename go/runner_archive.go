package download

import (
	"context"
	"strings"
	"sync"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// DownloadArchive is a folder of earlier downloads a runner checks before the
// sources of a job that names its SHA-256, and keeps a copy of what the
// sources delivered in [DL-R44]. The archive package implements it over
// the download archive the person sets in the Panel.
type DownloadArchive interface {
	// Fill writes the archived file whose digest is digest into partial,
	// hashing every byte, and reports the bytes written and whether partial
	// now holds exactly that file. size is zero when unknown. A missing,
	// asleep or differing archive answers false within ctx; partial is
	// then removed or left as it was.
	Fill(ctx context.Context, digest string, size int64, partial string, progress func(done, total int64)) (int64, bool)
	// Keep offers a delivered, verified file of the job operation for the
	// archive. It must return at once; the copy runs in the background.
	Keep(operation, final, digest string)
}

// fromArchive fills partial from the runner's archive when the job names a
// SHA-256 and has proven no byte yet. The lease is renewed and progress
// recorded while the archive copies. filled is false when the archive does
// not hold the file; the job then goes to its sources as before.
func (r *Runner) fromArchive(ctx context.Context, rec *job.Record, spec Spec, epoch int64, partial string) (total int64, filled bool, err error) {
	if r.Archive == nil || !strings.HasPrefix(spec.Artifact.Digest, "sha256:") {
		return 0, false, nil
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		return 0, false, nil
	}
	if have, err := cp.Proven(); err != nil || have.Total() > 0 {
		return 0, false, nil
	}
	kctx, keep := r.keep(ctx, rec.ID, epoch)
	var mu sync.Mutex
	var last time.Time
	var shown bool
	total, filled = r.Archive.Fill(kctx, spec.Artifact.Digest, spec.Artifact.Size, partial, func(done, size int64) {
		keep.beat()
		mu.Lock()
		due := time.Since(last) >= time.Second
		if due {
			last = time.Now()
		}
		mu.Unlock()
		if !due {
			return
		}
		shown = true
		_, err := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
			rr.Progress.Done = done
			if rr.Progress.Total == 0 {
				rr.Progress.Total = size
			}
			rr.Progress.UpdatedAt = job.At(time.Now())
			return nil
		})
		keep.refused(err)
	})
	if fence := keep.stop(); fence != nil {
		return 0, false, fence
	}
	if !filled && shown {
		// The archive copy's progress is no checkpoint: the sources start
		// from zero.
		//unchecked: a refused reset leaves the shown progress for the next checkpoint to replace.
		_, _ = r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
			rr.Progress.Done = 0
			rr.Progress.UpdatedAt = job.At(time.Now())
			return nil
		})
	}
	return total, filled, nil
}
