package download

import (
	"context"
	"errors"
	"sync"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// watchRunIntent covers source planning and quiet requests before any bytes
// arrive. It reads one owned operation; progress callbacks keep their existing
// checkpoint and renewal responsibilities. The returned function joins it.
func (r *Runner) watchRunIntent(ctx context.Context, id string, epoch int64, intent func(job.Want), cancel context.CancelFunc) func() error {
	done, exited := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var fence error // Read only after exited closes.
	go func() {
		defer close(exited)
		ticker := time.NewTicker(750 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			default:
			}
			record, err := r.Store.Load(id)
			switch {
			case errors.Is(err, job.ErrNotFound):
				fence = err
			case err != nil:
				// Transient read failures retain existing lease/fencing behavior.
			case record.Lease.Epoch != epoch:
				fence = job.ErrStaleEpoch
			case record.State.Terminal():
				fence = job.ErrTerminal
			case record.Lease.Owner != r.Owner || time.Now().After(record.Lease.ExpiresAt.Time):
				fence = job.ErrLeaseExpiry
			case record.Wants() != job.WantRun:
				intent(record.Wants())
				return
			}
			if fence != nil {
				cancel()
				return
			}
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() error {
		once.Do(func() { close(done) })
		<-exited
		return fence
	}
}
