// Package serve is the download layer's execution half inside the runtime: the
// `download-http-request-v1` profile, delegated execution and result reads that
// `openabstractions serve runtime` hosts.
package serve

import (
	"context"
	"fmt"

	download "github.com/openabstractions/abstraction-download/go"
)

// Pass is one sweep, and the order matters.
//
// Reconcile first: a delegated job that has finished needs finalising and
// verifying, and doing that before adopting means the orphan pass does not pick
// up work the delegate has in fact already completed.
//
// The counts, and what went wrong. Both matter: a sweep that reports only what
// it managed describes a store where nothing needs attention exactly as it
// describes one where a job fails on every single pass. `reconciled=2` once
// printed every five seconds for two hours while one transfer could not
// progress at all, its error thrown away by taking the count only when err was
// nil.
func Pass(ctx context.Context, r *download.Runner) (reconciled, delegated, adopted, delivered int, problems []error) {
	note := func(stage string, err error) {
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", stage, err))
		}
	}

	if r.Delegators != nil {
		n, err := r.ReconcileAll(ctx)
		reconciled, _ = n, err
		note("reconcile", err)

		// Then offer anything unclaimed to a delegate.
		n, err = r.DelegateAll(ctx)
		delegated = n
		note("delegate", err)
	}
	// Whatever nobody else wanted is still ours to finish.
	n, err := r.Adopt(ctx)
	adopted = n
	note("adopt", err)

	// Close out work that is demonstrably done. Without this a finished
	// download waits forever for an acknowledgement from a process that may
	// never come back.
	n, err = r.TakeDeliveryAll(ctx)
	delivered = n
	note("deliver", err)

	return reconciled, delegated, adopted, delivered, problems
}
