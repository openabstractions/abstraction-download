package serve

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// HTTP execution declares the acceptance history window as result retention.
func TestHTTPExecutionDeclaresResultRetention(t *testing.T) {
	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	w, err := p.Bind("alice").GetHistoryWindow()
	if err != nil || w.ResultRetentionMs != acceptanceprovider.MinimumRetentionMs {
		t.Fatalf("history window: %+v %v", w, err)
	}
}

// Service-owned HTTP execution reports a typed cause, and ends exactly the
// failures its profile names as final [JOB-A8].
func TestHTTPExecutionClassifiesFailures(t *testing.T) {
	body := []byte("service-owned-bytes")
	good := fmt.Sprintf("sha256:%x", sha256.Sum256(body))
	for _, c := range []struct {
		name, state, class, cause string
		status                    int
		serve                     []byte
		digest                    string
		size                      int64
	}{
		{name: "digest-mismatch", state: "failed", class: "permanent", cause: "digest_mismatch", status: 200, serve: []byte("tampered-bytes-here"), digest: good},
		{name: "oversize", state: "failed", class: "permanent", cause: "oversize", status: 200, serve: append(body, 'x'), size: int64(len(body))},
		{name: "unauthorized", state: "failed", class: "permanent", cause: "unauthorized", status: 401},
		{name: "not-found", state: "failed", class: "permanent", cause: "not_found", status: 404},
		// Retryable work is observed pending or still running while its lease is released.
		{name: "server-error", state: "nonterminal", class: "retryable", cause: "server_error", status: 503},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if c.status != 200 {
					http.Error(w, "no", c.status)
					return
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(c.serve)))
				w.Write(c.serve)
			}))
			defer server.Close()
			p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{})
			if err != nil {
				t.Fatal(err)
			}
			window, err := p.Bind("alice").GetHistoryWindow()
			if err != nil {
				t.Fatal(err)
			}
			id := api.RequestIdentity{Key: c.name, HistoryEpoch: window.HistoryEpoch}
			spec := request.Encode(&request.Request{Artifact: request.Artifact{Digest: c.digest, Size: c.size}, Sources: []request.Source{{Scheme: "http", Locator: server.URL + "/artifact"}}})
			if v, err := p.Bind("alice").Submit(api.Submission{Identity: id, Kind: "download", Spec: spec}); err != nil || v.Outcome != "accepted" {
				t.Fatalf("submit: %+v %v", v, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- p.Execute(ctx) }()
			defer func() { cancel(); <-done }()
			for {
				v, err := p.BindOperations("alice").ObserveWork(id)
				if err != nil || v.Snapshot == nil {
					t.Fatalf("observe: %+v %v", v, err)
				}
				if f := v.Snapshot.Failure; f != nil {
					state := v.Snapshot.State
					if c.state == "nonterminal" && (state == "pending" || state == "running") {
						state = "nonterminal"
					}
					if state != c.state || f.Classification != c.class || f.Cause != c.cause {
						t.Fatalf("got state %s class %s cause %q; want %s %s %s", v.Snapshot.State, f.Classification, f.Cause, c.state, c.class, c.cause)
					}
					if read, _ := p.BindOperations("alice").ReadResult(id, 0, 16); read.Outcome == "data" {
						t.Fatal("failed work exposed a result")
					}
					return
				}
				select {
				case <-ctx.Done():
					t.Fatalf("no failure observed: %+v", v.Snapshot)
				case <-time.After(20 * time.Millisecond):
				}
			}
		})
	}
}
