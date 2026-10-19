package serve

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-download/go/internal/personfolders"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

const refusedWhy = "switched off by the person for this test"

// A submission whose every source host the person switched off is refused
// unavailable with the list's reason and journals nothing; one reachable
// source admits it, and allowing the host again admits the same identity
// [DL-E8].
func TestServiceAdmissionHonoursRefusedHosts(t *testing.T) {
	if err := personfolders.Under(t.TempDir(), t.Setenv); err != nil {
		t.Fatal(err)
	}
	list := download.DefaultRefusals()
	if err := list.Refuse("refused.example", refusedWhy); err != nil {
		t.Fatal(err)
	}
	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	submit := func(key string, locators ...string) api.AcceptanceResult {
		t.Helper()
		var sources []request.Source
		for _, l := range locators {
			sources = append(sources, request.Source{Scheme: "https", Locator: l})
		}
		result, err := p.Bind("alice").Submit(api.Submission{Identity: api.RequestIdentity{Key: key, HistoryEpoch: window.HistoryEpoch},
			Kind: download.Kind, Spec: request.Encode(&request.Request{Sources: sources})})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	result := submit("only-refused", "https://cdn.refused.example/model")
	if result.Outcome != api.AcceptanceOutcomeUnavailable || !strings.Contains(result.Reason, refusedWhy) || !strings.Contains(result.Reason, "cdn.refused.example") {
		t.Fatalf("a request to a refused host was admitted: %+v", result)
	}
	if result := submit("one-reachable", "https://cdn.refused.example/model", "https://mirror.invalid/model"); result.Outcome != api.AcceptanceOutcomeAccepted {
		t.Fatalf("a request with a reachable source was refused: %+v", result)
	}
	if err := list.Allow("refused.example"); err != nil {
		t.Fatal(err)
	}
	if result := submit("only-refused", "https://cdn.refused.example/model"); result.Outcome != api.AcceptanceOutcomeAccepted {
		t.Fatalf("the refusal outlived the person's switch: %+v", result)
	}
}

// The service runner asks the person's list before every connection: a
// redirect to a host switched off after admission is not followed, and the
// attempt ends not now with the list's reason [DL-E8].
func TestServiceRunnerRefusesARedirectToARefusedHost(t *testing.T) {
	if err := personfolders.Under(t.TempDir(), t.Setenv); err != nil {
		t.Fatal(err)
	}
	var reached atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Store(true)
		w.Write([]byte("bytes from a refused host"))
	}))
	defer target.Close()
	refusedTarget := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, refusedTarget+"/artifact", http.StatusFound)
	}))
	defer origin.Close()
	if err := download.DefaultRefusals().Refuse("localhost", refusedWhy); err != nil {
		t.Fatal(err)
	}

	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	id := api.RequestIdentity{Key: "redirected", HistoryEpoch: window.HistoryEpoch}
	spec := request.Encode(&request.Request{Sources: []request.Source{{Scheme: "http", Locator: origin.URL + "/artifact"}}})
	if v, err := p.Bind("alice").Submit(api.Submission{Identity: id, Kind: download.Kind, Spec: spec}); err != nil || v.Outcome != api.AcceptanceOutcomeAccepted {
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
			if f.Classification.String() != "retryable" || !strings.Contains(f.Message, refusedWhy) {
				t.Fatalf("got class %s message %q; want retryable naming the person's reason", f.Classification, f.Message)
			}
			if reached.Load() {
				t.Fatal("the refused redirect target was reached")
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no failure observed: %+v", v.Snapshot)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// A host name the list does not refuse, resolving to an address it does, is
// refused at the dial with the list's reason [DL-E8].
func TestServiceRunnerRefusesAResolvedAddress(t *testing.T) {
	if err := personfolders.Under(t.TempDir(), t.Setenv); err != nil {
		t.Fatal(err)
	}
	var reached atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Store(true)
		w.Write([]byte("bytes from a refused address"))
	}))
	defer server.Close()
	if err := download.DefaultRefusals().Refuse("127.0.0.1", refusedWhy); err != nil {
		t.Fatal(err)
	}
	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	id := api.RequestIdentity{Key: "by-address", HistoryEpoch: window.HistoryEpoch}
	locator := strings.Replace(server.URL, "127.0.0.1", "localhost", 1) + "/artifact"
	spec := request.Encode(&request.Request{Sources: []request.Source{{Scheme: "http", Locator: locator}}})
	if v, err := p.Bind("alice").Submit(api.Submission{Identity: id, Kind: download.Kind, Spec: spec}); err != nil || v.Outcome != api.AcceptanceOutcomeAccepted {
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
			if f.Classification.String() != "retryable" || !strings.Contains(f.Message, refusedWhy) {
				t.Fatalf("got class %s message %q; want retryable naming the person's reason", f.Classification, f.Message)
			}
			if reached.Load() {
				t.Fatal("the refused address was reached")
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no failure observed: %+v", v.Snapshot)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
