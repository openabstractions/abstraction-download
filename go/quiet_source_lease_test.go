package download

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// pausingSource writes half the artifact, sends nothing for quiet, and then
// writes the rest.
type pausingSource struct {
	body  []byte
	quiet time.Duration
}

func (*pausingSource) Schemes() []string          { return []string{"http"} }
func (*pausingSource) Capabilities() []Capability { return []Capability{CapResume} }
func (s *pausingSource) Fetch(ctx context.Context, req Request) (Result, error) {
	half := int64(len(s.body)) / 2
	written := int64(0)
	if req.From < half {
		n, err := req.Out.Write(s.body[req.From:half])
		written += int64(n)
		if err != nil {
			return Result{Written: written}, err
		}
	}
	select {
	case <-ctx.Done():
		return Result{Written: written}, ctx.Err()
	case <-time.After(s.quiet):
	}
	from := max(req.From, half)
	n, err := req.Out.Write(s.body[from:])
	written += int64(n)
	return Result{Written: written, Total: int64(len(s.body))}, err
}

// A single-stream source that is silent for several leases, and well inside
// the silence budget, still completes. Renewal rode only the data path, so the
// lease lapsed after one TTL and the owner was fenced off a live transfer.
func TestASingleStreamHoldsItsLeaseThroughAQuietSource(t *testing.T) {
	body, digest := payload(t, 64<<10)
	r, store, root := newRunner(t)
	r.Connections = 1
	r.LeaseTTL = 400 * time.Millisecond
	r.SilenceBudget = 30 * time.Second
	r.PersistInterval = time.Hour
	r.Fetchers = NewFetchers(&pausingSource{body: body, quiet: 4 * r.LeaseTTL})
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: "http://127.0.0.1/fixture"})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := r.Run(ctx, id); err != nil {
		t.Fatalf("a source quiet for %s inside a %s budget was dropped: %v", 4*r.LeaseTTL, r.SilenceBudget, err)
	}
	got, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("the delivered artifact differs")
	}
	if rec, err := store.Load(id); err != nil || rec.State != job.StateTransferred {
		t.Fatalf("after the quiet source: %v, %v", rec, err)
	}
}
