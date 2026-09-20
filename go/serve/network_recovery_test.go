package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-download/go/netcost"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// A service that restarts before its cost source answers, as a Linux runtime
// can before NetworkManager starts, opens its store with network-constrained
// work accepted. That work waits with network:unavailable, unconstrained work
// completes meanwhile, a new constrained submission is refused, and the waiting
// work completes once the source answers [DL-N8, JOB-A15].
func TestRestartBeforeTheCostSourceAnswersKeepsTheStore(t *testing.T) {
	previous := networkRetry
	networkRetry = 20 * time.Millisecond
	t.Cleanup(func() { networkRetry = previous })
	fastService(t)

	body := testBody(40 << 10)
	source := &rangeSource{body: body}
	server := httptest.NewServer(source)
	defer server.Close()
	fake := netcost.NewFake(netcost.Metered)
	var ready atomic.Bool
	ready.Store(true)
	var opens atomic.Int32
	e := HTTPExecution{NetworkCost: func() (netcost.Source, error) {
		opens.Add(1)
		if !ready.Load() {
			return nil, netcost.ErrUnavailable
		}
		return fake, nil
	}}
	root := t.TempDir()
	first, err := acceptanceprovider.OpenWithExecutor(root, "late-owner", e)
	if err != nil {
		t.Fatal(err)
	}
	id, operation := submitUnmetered(t, first, "constrained", body, server.URL+"/constrained")

	// The platform stops answering; the service restarts.
	ready.Store(false)
	p, err := acceptanceprovider.OpenWithExecutor(root, "late-owner", e)
	if err != nil {
		t.Fatalf("an unanswering cost source closed the store: %v", err)
	}
	if slices.Contains(p.SupportedGuarantees(), NetworkCostGuarantee) {
		t.Fatal("offered network-cost@1 with no source answering")
	}
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	plainID := api.RequestIdentity{Key: "plain", HistoryEpoch: window.HistoryEpoch}
	plainSpec := request.Encode(&request.Request{Artifact: request.Artifact{Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(body)), Size: int64(len(body))},
		Sources: []request.Source{{Scheme: "http", Locator: server.URL + "/plain"}}})
	if v, err := p.Bind("alice").Submit(api.Submission{Identity: plainID, Kind: "download", Spec: plainSpec}); err != nil || v.Outcome.String() != "accepted" {
		t.Fatalf("unconstrained submit: %+v %v", v, err)
	}
	freshID := api.RequestIdentity{Key: "fresh", HistoryEpoch: window.HistoryEpoch}
	if v, err := p.Bind("alice").Submit(api.Submission{Identity: freshID, Kind: "download", Spec: unmeteredRequest(body, server.URL+"/fresh"), RequiredGuarantees: []string{NetworkCostGuarantee}}); err != nil || v.Outcome.String() == "accepted" {
		t.Fatalf("new constrained work accepted with no source: %+v %v", v, err)
	}

	execute(t, p)
	waitFor(t, "unconstrained work completes", 20*time.Second, func() bool { return observe(t, p, plainID).State.String() == "complete" })
	waitFor(t, "network:unavailable recorded", 20*time.Second, func() bool {
		s := observe(t, p, id)
		return s.Waiting == download.WaitingNetworkUnavailable && s.State.String() == "pending"
	})
	if r := record(t, root, operation); r.Lease.Owner != "" || download.LastFailure(r) != nil || r.Error != "" {
		t.Fatalf("waiting record: lease %q error %q", r.Lease.Owner, r.Error)
	}
	// One request served the unconstrained work; the constrained work opened nothing.
	before := len(source.log())
	if before != 1 {
		t.Fatalf("source requests while unavailable: %q", source.log())
	}

	// The source answers, reporting a metered path: the word changes, nothing moves.
	ready.Store(true)
	waitFor(t, "network:metered once the source answers", 20*time.Second, func() bool {
		return observe(t, p, id).Waiting == download.WaitingNetworkMetered
	})
	if len(source.log()) != before {
		t.Fatalf("a source opened while metered: %q", source.log())
	}
	if !slices.Contains(p.SupportedGuarantees(), NetworkCostGuarantee) {
		t.Fatal("network-cost@1 not offered once the source answers")
	}
	fake.Set(netcost.Unmetered)
	waitFor(t, "constrained work completes", 30*time.Second, func() bool {
		s := observe(t, p, id)
		if s.State.String() == "failed" || s.State.String() == "cancelled" {
			t.Fatalf("ended %s: %+v", s.State, s.Failure)
		}
		return s.State.String() == "complete"
	})
	if !bytes.Equal(readAll(t, p, id), body) || observe(t, p, id).Waiting != "" {
		t.Fatal("constrained result differs or kept its word")
	}
	if opens.Load() < 3 {
		t.Fatalf("the source was asked %d times", opens.Load())
	}
}

// A late source reports unavailable, then the opened source's cost, and tells
// its watchers about both; closing it ends its watchers.
func TestLateNetworkReportsTheOpenedSource(t *testing.T) {
	previous := networkRetry
	networkRetry = 5 * time.Millisecond
	t.Cleanup(func() { networkRetry = previous })
	fake := netcost.NewFake(netcost.Unmetered)
	var ready atomic.Bool
	source, stop := servedNetwork(context.Background(), func() (netcost.Source, error) {
		if !ready.Load() {
			return nil, netcost.ErrUnavailable
		}
		return fake, nil
	})
	watcher := source.Watch()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if c, err := watcher.Next(ctx); err != nil || c != download.CostUnavailable || source.Cost() != download.CostUnavailable {
		t.Fatalf("first %q %v", c, err)
	}
	ready.Store(true)
	if c, err := watcher.Next(ctx); err != nil || c != netcost.Unmetered {
		t.Fatalf("opened %q %v", c, err)
	}
	fake.Set(netcost.Metered)
	if c, err := watcher.Next(ctx); err != nil || c != netcost.Metered || source.Cost() != netcost.Metered {
		t.Fatalf("metered %q %v", c, err)
	}
	stop()
	if _, err := watcher.Next(ctx); err == nil {
		t.Fatal("watcher outlived the late source")
	}
	if direct, stopDirect := servedNetwork(context.Background(), func() (netcost.Source, error) { return fake, nil }); direct != netcost.Source(fake) {
		t.Fatal("an answering source was wrapped")
	} else {
		stopDirect()
	}
}
