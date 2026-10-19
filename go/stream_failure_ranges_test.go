package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// holeFetcher serves one artifact gap by gap and, while failAt is set, breaks
// the request that reaches failAt after writing up to it.
type holeFetcher struct {
	body   []byte
	mu     sync.Mutex
	failAt int64
	asked  []fetchRange
}

func (*holeFetcher) Schemes() []string          { return []string{"http"} }
func (*holeFetcher) Capabilities() []Capability { return []Capability{CapResume} }

func (f *holeFetcher) Fetch(_ context.Context, req Request) (Result, error) {
	f.mu.Lock()
	f.asked = append(f.asked, fetchRange{From: req.From, To: req.To})
	failAt := f.failAt
	f.mu.Unlock()
	end := req.To
	if end == 0 {
		end = int64(len(f.body))
	}
	broken := failAt > req.From && failAt < end
	if broken {
		end = failAt
	}
	n, err := req.Out.Write(f.body[req.From:end])
	res := Result{Written: int64(n), Total: int64(len(f.body))}
	if err != nil {
		return res, err
	}
	if broken {
		return res, errors.New("connection reset by fixture")
	}
	return res, nil
}

func (f *holeFetcher) requests() []fetchRange {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fetchRange(nil), f.asked...)
}

// A single stream that fills one hole and fails inside the next keeps every
// range past the failure. Writing the failed run's prefix in place of the set
// dropped [150,200), and the next plan trimmed the file to the prefix.
func TestAStreamFailureKeepsRangesPastTheFailedGap(t *testing.T) {
	body, digest := payload(t, 200)
	r, store, root := newRunner(t)
	r.Connections = 1
	r.PersistEvery = 1 << 20
	fetcher := &holeFetcher{body: body, failAt: 140}
	r.Fetchers = NewFetchers(fetcher)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: "http://127.0.0.1/fixture"})

	stageSparse(t, store, id, body,
		Range{Start: 0, End: 10},
		Range{Start: 50, End: 100},
		Range{Start: 150, End: 200},
	)

	if err := r.Run(context.Background(), id); err == nil {
		t.Fatal("a run whose source broke at byte 140 reported success")
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	have, err := cp.Proven()
	if err != nil {
		t.Fatal(err)
	}
	want := Ranges{{Start: 0, End: 140}, {Start: 150, End: 200}}
	if !have.Equal(want) {
		t.Fatalf("checkpoint after the failure holds %v, want %v", have, want)
	}
	if rec.Progress.Done != want.Total() {
		t.Fatalf("progress done = %d, want %d", rec.Progress.Done, want.Total())
	}
	if st, err := os.Stat(partialOf(t, store, id)); err != nil || st.Size() != 200 {
		t.Fatalf("partial after the failure: %v, %v; want 200 bytes", st, err)
	}

	fetcher.mu.Lock()
	fetcher.failAt = 0
	fetcher.asked = nil
	fetcher.mu.Unlock()
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := fmt.Sprint(fetcher.requests()); got != fmt.Sprint([]fetchRange{{From: 140, To: 150}}) {
		t.Fatalf("resume asked for %s; only [140,150) was missing", got)
	}
	delivered, err := os.ReadFile(finalOf(t, store, id))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(delivered, body) {
		t.Fatal("the resumed artifact differs")
	}
	if rec, err := store.Load(id); err != nil || rec.State != job.StateTransferred {
		t.Fatalf("after resume: %v, %v", rec, err)
	}
}

// keepProven adds its prefix to the ranges the record holds.
func TestKeepProvenAddsThePrefixToTheRecordedRanges(t *testing.T) {
	r, store, root := newRunner(t)
	id := submit(t, store, root, "", 200, Source{Scheme: "http", Locator: "http://127.0.0.1/fixture"})
	claimed, err := store.Claim(id, r.Owner, r.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	epoch := claimed.Lease.Epoch
	start := Ranges{{Start: 0, End: 10}, {Start: 50, End: 100}, {Start: 150, End: 200}}
	if _, err := store.Update(id, epoch, func(rr *job.Record) error {
		rr.Progress.Done = start.Total()
		return setCheckpoint(rr, Checkpoint{Verified: start, Validators: Validators{ETag: `"v1"`}})
	}); err != nil {
		t.Fatal(err)
	}

	r.keepProven(id, epoch, 140, Validators{})

	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	have, err := cp.Proven()
	if err != nil {
		t.Fatal(err)
	}
	want := Ranges{{Start: 0, End: 140}, {Start: 150, End: 200}}
	if !have.Equal(want) || rec.Progress.Done != want.Total() {
		t.Fatalf("checkpoint holds %v done %d, want %v done %d", have, rec.Progress.Done, want, want.Total())
	}
	if cp.Validators.ETag != `"v1"` {
		t.Fatalf("validators = %+v, want the recorded version kept", cp.Validators)
	}

	// A smaller prefix proves nothing new and changes nothing.
	r.keepProven(id, epoch, 120, Validators{})
	rec, err = store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	cp, _ = CheckpointOf(rec)
	if have, _ := cp.Proven(); !have.Equal(want) {
		t.Fatalf("a smaller prefix moved the checkpoint to %v", have)
	}
}
