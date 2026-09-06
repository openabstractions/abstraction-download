package download

import (
	"context"

	job "github.com/openabstractions/abstraction-job/go"
)

// Handle is a submitted download: a job handle that also answers the three
// questions every caller of this layer has had to answer for itself.
//
// A job handle knows about work; it does not know about bytes, and it must not
// — the job layer never parses a spec. But the first two honest callers of the
// facade both had to know where their file landed, and both wrote the same
// anonymous struct to decode sink.final out of another layer's wire format.
// That is the abstraction leaking through the API that exists to seal it.
//
//	h, _ := a.Download().Get(url, "models/")
//	if _, err := h.Wait(ctx); err != nil { return err }
//	path, _ := h.Destination()
type Handle interface {
	job.Job

	// Destination is where the bytes are on this machine, resolved the way the
	// process that delivers them resolves it. Valid before the transfer starts:
	// the sink is decided at submission, not on arrival.
	Destination() (string, error)

	// Wait blocks until the bytes are here and takes delivery of them.
	//
	// Without it an application that submits and returns downloads nothing when
	// no supervisor is running: Submit hands the work to a goroutine in this
	// process, and the process exits. Three callers wrote three different loops
	// over a subscription, and two of them forgot the delivery half.
	Wait(ctx context.Context) (*job.Record, error)

	// TakeDelivery is the requester saying "I have it". Wait does this; a caller
	// that watched the collection itself still owes it.
	TakeDelivery() error
}

type handle struct {
	job.Job
	client *client
}

func (h handle) Destination() (string, error) {
	rec, err := h.Record()
	if err != nil {
		return "", err
	}
	spec, err := SpecOf(rec)
	if err != nil {
		return "", err
	}
	_, final, err := LocalSink(h.client.runner.Store, rec.ID, spec.Sink)
	return final, err
}

func (h handle) Wait(ctx context.Context) (*job.Record, error) {
	return h.client.Deliver(ctx, h.ID())
}

func (h handle) TakeDelivery() error { return h.client.TakeDelivery(h.ID()) }

// pausableHandle forwards an extended capability the wrapped handle advertises.
//
// Without it, wrapping is a silent downgrade: job.Pausable is discovered by type
// assertion, an assertion against a wrapper that does not declare Pause fails,
// and every application built on this layer loses its pause button without one
// compiler error anywhere.
type pausableHandle struct {
	handle
	inner job.Pausable
}

func (p pausableHandle) Pause() error { return p.inner.Pause() }

func (p pausableHandle) Resume() error { return p.inner.Resume() }
