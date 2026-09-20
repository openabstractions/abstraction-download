package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-download/go/netcost"
	job "github.com/openabstractions/abstraction-job/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

func withFakeCost(fake *netcost.Fake) HTTPExecution {
	return HTTPExecution{NetworkCost: func() (netcost.Source, error) { return fake, nil }}
}

func unmeteredRequest(body []byte, url string) []byte {
	return request.Encode(&request.Request{
		Artifact:    request.Artifact{Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(body)), Size: int64(len(body))},
		Sources:     []request.Source{{Scheme: "http", Locator: url}},
		Constraints: &request.Constraints{Network: request.NetworkUnmetered},
	})
}

func submitUnmetered(t *testing.T, p *acceptanceprovider.Provider, key string, body []byte, url string) (api.RequestIdentity, string) {
	t.Helper()
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	id := api.RequestIdentity{Key: key, HistoryEpoch: window.HistoryEpoch}
	v, err := p.Bind("alice").Submit(api.Submission{Identity: id, Kind: "download", Spec: unmeteredRequest(body, url), RequiredGuarantees: []string{NetworkCostGuarantee}})
	if err != nil || v.Outcome.String() != "accepted" || !slices.Contains(v.Receipt.AcceptedGuarantees, NetworkCostGuarantee) {
		t.Fatalf("submit: %+v %v", v, err)
	}
	return id, v.Receipt.OperationID
}

// record reads the service's own job record, which carries the waiting word.
func record(t *testing.T, root, operation string) *job.Record {
	t.Helper()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Load(operation)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func fastService(t *testing.T) {
	configureRunner = fastRunner
	t.Cleanup(func() { configureRunner = func(*download.Runner) {} })
}

// Preparation keeps an unconstrained request's work byte for byte, puts an
// unmetered constraint and its guarantee on the work, and refuses the
// constraint without the guarantee [DL-N1, DL-N2].
func TestNetworkConstraintPreparation(t *testing.T) {
	e := withFakeCost(netcost.NewFake(netcost.Unmetered))
	plain := []byte(`{"artifact":{},"sources":[{"scheme":"https","locator":"https://example.com/file"}]}`)
	anyNetwork := []byte(`{"artifact":{},"sources":[{"scheme":"https","locator":"https://example.com/file"}],"constraints":{"network":"any"}}`)
	before, _, err := e.PrepareScoped("alice", "op1", "download", plain, nil)
	if err != nil {
		t.Fatal(err)
	}
	again, requires, err := e.PrepareScoped("alice", "op1", "download", anyNetwork, nil)
	if err != nil || !bytes.Equal(before, again) || requires != nil || strings.Contains(string(before), "constraints") {
		t.Fatalf("network any prepared differently: %s %v %v", again, requires, err)
	}
	unmetered := unmeteredRequest([]byte("x"), "http://example.com/file")
	if _, _, err := e.PrepareScoped("alice", "op1", "download", unmetered, nil); err == nil {
		t.Fatal("an unmetered constraint prepared without its guarantee")
	}
	work, requires, err := e.PrepareScoped("alice", "op1", "download", unmetered, []string{NetworkCostGuarantee})
	if err != nil || !slices.Equal(requires, []string{NetworkCostGuarantee}) || !strings.Contains(string(work), `"constraints":{"network":"unmetered"}`) {
		t.Fatalf("unmetered work %s %v %v", work, requires, err)
	}
	if _, _, err := (DelegatedExecution{HTTPExecution: e}).PrepareScoped("alice", "op1", "download", unmetered, []string{NetworkCostGuarantee}); err == nil {
		t.Fatal("delegated execution accepted a network constraint it cannot evaluate")
	}
}

// A metered notice mid-transfer stops the transfer at the proven boundary,
// records waiting and releases the lease. Nothing is opened while metered; the
// notice back resumes with one Range request and delivers exact bytes
// [DL-N3, DL-N4, DL-N5].
func TestMeteredMidTransferWaitsAndResumesWithOneRange(t *testing.T) {
	const size, hold = 256 << 10, 64 << 10
	body := testBody(size)
	source := &rangeSource{body: body, hold: hold, release: make(chan struct{})}
	server := httptest.NewServer(source)
	defer server.Close()
	defer close(source.release)
	fake := netcost.NewFake(netcost.Unmetered)
	root := t.TempDir()
	p, err := acceptanceprovider.OpenWithExecutor(root, "metered-owner", withFakeCost(fake))
	if err != nil {
		t.Fatal(err)
	}
	fastService(t)
	id, operation := submitUnmetered(t, p, "metered", body, server.URL+"/artifact")
	execute(t, p)
	waitFor(t, "the held prefix checkpointed", 20*time.Second, func() bool { return observe(t, p, id).Progress.Done >= hold })

	fake.Set(netcost.Metered)
	waitFor(t, "waiting recorded with the lease released", 20*time.Second, func() bool {
		r := record(t, root, operation)
		return download.Waiting(r) == download.WaitingNetworkMetered && r.State == job.StatePending && r.Lease.Owner == ""
	})
	r := record(t, root, operation)
	if r.Progress.Done != hold || download.LastFailure(r) != nil || r.Error != "" {
		t.Fatalf("waiting record: done %d error %q", r.Progress.Done, r.Error)
	}
	if s := observe(t, p, id); s.State.String() != "pending" || s.Failure != nil || s.Waiting != download.WaitingNetworkMetered {
		t.Fatalf("waiting snapshot: %+v", s)
	}
	time.Sleep(500 * time.Millisecond)
	if log := source.log(); len(log) != 1 {
		t.Fatalf("a source was opened while metered: %q", log)
	}

	fake.Set(netcost.Unmetered)
	waitFor(t, "completion after the path is unmetered", 30*time.Second, func() bool {
		s := observe(t, p, id)
		if s.State.String() == "failed" || s.State.String() == "cancelled" {
			t.Fatalf("ended %s: %+v", s.State, s.Failure)
		}
		return s.State.String() == "complete"
	})
	if got := readAll(t, p, id); !bytes.Equal(got, body) {
		t.Fatalf("result differs: %d bytes", len(got))
	}
	if log := source.log(); len(log) != 2 || log[0] != "" || log[1] != fmt.Sprintf("bytes=%d-", hold) {
		t.Fatalf("source requests %q", log)
	}
	if download.Waiting(record(t, root, operation)) != "" || observe(t, p, id).Waiting != "" {
		t.Fatal("waiting word left on delivered work")
	}
}

// Work submitted while the path is metered waits before anything is opened.
// Cancelling it ends the attempt cancelled with no source opened [DL-N6].
func TestCancelWhileWaitingOpensNothing(t *testing.T) {
	body := testBody(32 << 10)
	source := &rangeSource{body: body}
	server := httptest.NewServer(source)
	defer server.Close()
	fake := netcost.NewFake(netcost.Metered)
	root := t.TempDir()
	p, err := acceptanceprovider.OpenWithExecutor(root, "cancel-owner", withFakeCost(fake))
	if err != nil {
		t.Fatal(err)
	}
	fastService(t)
	id, operation := submitUnmetered(t, p, "cancel", body, server.URL+"/artifact")
	execute(t, p)
	waitFor(t, "waiting recorded", 20*time.Second, func() bool {
		return download.Waiting(record(t, root, operation)) == download.WaitingNetworkMetered
	})
	if v, err := p.Bind("alice").CancelWork(id); err != nil || v.Outcome.String() != "requested" {
		t.Fatalf("cancel: %+v %v", v, err)
	}
	waitFor(t, "cancelled", 20*time.Second, func() bool { return observe(t, p, id).State.String() == "cancelled" })
	if log := source.log(); len(log) != 0 {
		t.Fatalf("cancelling waiting work opened a source: %q", log)
	}
	if download.Waiting(record(t, root, operation)) != "" {
		t.Fatal("waiting word left on cancelled work")
	}
}

// The submitting caller going away leaves the waiting attempt accepted and
// waiting, through a service restart as well; a new caller reconciles the same
// receipt, and the attempt completes once the path is unmetered [DL-N6].
func TestCallerExitWhileWaitingKeepsTheAttempt(t *testing.T) {
	body := testBody(48 << 10)
	source := &rangeSource{body: body}
	server := httptest.NewServer(source)
	defer server.Close()
	fake := netcost.NewFake(netcost.Metered)
	root := t.TempDir()
	fastService(t)
	first, err := acceptanceprovider.OpenWithExecutor(root, "exit-owner", withFakeCost(fake))
	if err != nil {
		t.Fatal(err)
	}
	id, operation := submitUnmetered(t, first, "exit", body, server.URL+"/artifact")
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- first.Execute(ctx) }()
	waitFor(t, "waiting recorded", 20*time.Second, func() bool {
		return download.Waiting(record(t, root, operation)) == download.WaitingNetworkMetered
	})
	// The caller is gone and the service stops; nothing of either survives but
	// the provider's own state.
	stop()
	<-done

	p, err := acceptanceprovider.OpenWithExecutor(root, "exit-owner", withFakeCost(fake))
	if err != nil {
		t.Fatal(err)
	}
	v, err := p.Bind("alice").Reconcile(id)
	if err != nil || v.Outcome.String() != "accepted" || v.Receipt.OperationID != operation {
		t.Fatalf("reconcile after caller exit: %+v %v", v, err)
	}
	execute(t, p)
	time.Sleep(300 * time.Millisecond)
	if s := observe(t, p, id); s.State.String() != "pending" || s.Waiting != download.WaitingNetworkMetered || len(source.log()) != 0 || download.Waiting(record(t, root, operation)) == "" {
		t.Fatalf("restarted service did not keep waiting: %+v requests %q", s, source.log())
	}
	fake.Set(netcost.Unmetered)
	waitFor(t, "completion", 30*time.Second, func() bool { return observe(t, p, id).State.String() == "complete" })
	if !bytes.Equal(readAll(t, p, id), body) {
		t.Fatal("result differs")
	}
	if log := source.log(); len(log) != 1 || log[0] != "" {
		t.Fatalf("source requests %q", log)
	}
}

// A provider without a cost source does not offer network-cost@1, refuses a
// submission requiring it, and refuses the constraint without it [DL-N2].
func TestNoCostSourceRefusesTheGuarantee(t *testing.T) {
	e := HTTPExecution{NetworkCost: func() (netcost.Source, error) { return nil, netcost.ErrUnavailable }}
	if slices.Contains(e.ExecutionGuarantees(), NetworkCostGuarantee) {
		t.Fatal("advertised without a cost source")
	}
	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "none-owner", e)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(p.SupportedGuarantees(), NetworkCostGuarantee) {
		t.Fatalf("provider offers %v", p.SupportedGuarantees())
	}
	window, _ := p.Bind("alice").GetHistoryWindow()
	body := []byte("bytes")
	for i, required := range [][]string{{NetworkCostGuarantee}, nil} {
		id := api.RequestIdentity{Key: fmt.Sprintf("none-%d", i), HistoryEpoch: window.HistoryEpoch}
		v, err := p.Bind("alice").Submit(api.Submission{Identity: id, Kind: "download", Spec: unmeteredRequest(body, "http://127.0.0.1:9/x"), RequiredGuarantees: required})
		if err != nil || v.Outcome.String() == "accepted" {
			t.Fatalf("required %v accepted without a cost source: %+v %v", required, v, err)
		}
	}
	if !slices.Contains(withFakeCost(netcost.NewFake(netcost.Unknown)).ExecutionGuarantees(), NetworkCostGuarantee) {
		t.Fatal("a provider with a cost source does not advertise it")
	}
}
