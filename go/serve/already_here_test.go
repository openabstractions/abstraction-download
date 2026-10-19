package serve

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// A second request for a digest the service already proved copies the bytes
// from its own finished file and opens no source, through the service's own
// runner and fetchers [DL-R28, already-here].
func TestServiceReusesBytesItAlreadyProved(t *testing.T) {
	body := []byte("bytes the service proved once and keeps")
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(body))
	var first, second atomic.Int32
	firstSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.Add(1)
		w.Write(body)
	}))
	defer firstSource.Close()
	secondSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		http.Error(w, "this source is never needed", http.StatusServiceUnavailable)
	}))
	defer secondSource.Close()

	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Execute(ctx) }()
	defer func() { cancel(); <-done }()

	fetch := func(caller, key, locator string) {
		t.Helper()
		id := api.RequestIdentity{Key: key, HistoryEpoch: window.HistoryEpoch}
		spec := request.Encode(&request.Request{Artifact: request.Artifact{Digest: digest, Size: int64(len(body))},
			Sources: []request.Source{{Scheme: "http", Locator: locator}}})
		if v, err := p.Bind(caller).Submit(api.Submission{Identity: id, Kind: download.Kind, Spec: spec}); err != nil || v.Outcome != api.AcceptanceOutcomeAccepted {
			t.Fatalf("submit %s: %+v %v", key, v, err)
		}
		for {
			v, err := p.BindOperations(caller).ObserveWork(id)
			if err != nil || v.Snapshot == nil {
				t.Fatalf("observe %s: %+v %v", key, v, err)
			}
			if v.Snapshot.State == api.WorkStateComplete {
				read, err := p.BindOperations(caller).ReadResult(id, 0, int64(len(body)+1))
				if err != nil || read.Chunk == nil || string(read.Chunk.Data) != string(body) {
					t.Fatalf("result of %s: %+v %v", key, read, err)
				}
				return
			}
			if f := v.Snapshot.Failure; f != nil && v.Snapshot.State == api.WorkStateFailed {
				t.Fatalf("%s failed: %s %s", key, f.Cause, f.Message)
			}
			select {
			case <-ctx.Done():
				t.Fatalf("%s did not complete: %+v", key, v.Snapshot)
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	fetch("alice", "first", firstSource.URL+"/artifact")
	if first.Load() == 0 {
		t.Fatal("the first request fetched nothing")
	}
	// Another caller, another source, the same digest.
	fetch("bob", "second", secondSource.URL+"/artifact")
	if n := second.Load(); n != 0 {
		t.Fatalf("the second request opened its source %d times for bytes the store already proved", n)
	}
}
