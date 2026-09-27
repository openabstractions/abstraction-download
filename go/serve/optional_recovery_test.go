package serve

import (
	"slices"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-download/go/netcost"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// Expose the adapter's transfer methods while withholding reconciliation.
type adapterWithoutLocator struct{ download.Delegator }

func TestExecutorRecoveryWithOptionalSourcesAbsent(t *testing.T) {
	fastService(t)
	for _, profile := range []struct {
		name string
		wrap func(HTTPExecution) acceptanceprovider.Executor
	}{
		{"http", func(e HTTPExecution) acceptanceprovider.Executor { return e }},
		{"legacy-sink", func(e HTTPExecution) acceptanceprovider.Executor { return LegacySinkExecution{HTTPExecution: e} }},
	} {
		t.Run(profile.name, func(t *testing.T) {
			root := t.TempDir()
			applier := &switchApplier{}
			applier.set("applied", "applied")
			first, err := acceptanceprovider.OpenWithExecutor(root, "optional-owner", profile.wrap(HTTPExecution{
				Credentials: applier,
				NetworkCost: func() (netcost.Source, error) { return netcost.NewFake(netcost.Unmetered), nil },
			}))
			if err != nil {
				t.Fatal(err)
			}
			window, err := first.Bind("alice").GetHistoryWindow()
			if err != nil {
				t.Fatal(err)
			}
			submit := func(key string, constrained bool) api.RequestIdentity {
				t.Helper()
				input := &request.Request{Sources: []request.Source{{Scheme: "https", Locator: "https://example.invalid/fixture", Credential: "hf"}}}
				required := []string{CredentialGuarantee}
				if constrained {
					input.Constraints = &request.Constraints{Network: request.NetworkUnmetered}
					required = append(required, NetworkCostGuarantee)
				}
				id := api.RequestIdentity{Key: key, HistoryEpoch: window.HistoryEpoch}
				result, err := first.Bind("alice").Submit(api.Submission{Identity: id, Kind: download.Kind,
					Spec: request.Encode(input), RequiredGuarantees: required})
				if err != nil || result.Outcome != api.AcceptanceOutcomeAccepted {
					t.Fatalf("admit %s: %+v %v", key, result, err)
				}
				return id
			}
			waiting := submit("waiting-network", true)
			credential := submit("missing-credential", false)

			recovered, err := acceptanceprovider.OpenWithExecutor(root, "optional-owner", profile.wrap(HTTPExecution{
				NetworkCost: func() (netcost.Source, error) { return nil, netcost.ErrUnavailable },
			}))
			if err != nil {
				t.Fatalf("accepted work rejected on recovery: %v", err)
			}
			if offered := recovered.SupportedGuarantees(); slices.Contains(offered, CredentialGuarantee) || slices.Contains(offered, NetworkCostGuarantee) {
				t.Fatalf("offered absent dependencies: %v", offered)
			}
			fresh := api.Submission{Identity: api.RequestIdentity{Key: "new", HistoryEpoch: window.HistoryEpoch},
				Kind: download.Kind, Spec: request.Encode(&request.Request{Sources: []request.Source{{Scheme: "https", Locator: "https://example.invalid/new", Credential: "hf"}}}),
				RequiredGuarantees: []string{CredentialGuarantee}}
			if result, err := recovered.Bind("alice").Submit(fresh); err != nil || result.Outcome == api.AcceptanceOutcomeAccepted {
				t.Fatalf("new work used absent applier: %+v %v", result, err)
			}
			execute(t, recovered)
			waitFor(t, "constrained job waits for network source", 10*time.Second, func() bool {
				return observe(t, recovered, waiting).Waiting == download.WaitingNetworkUnavailable
			})
			waitFor(t, "missing applier gives a retryable credential failure", 10*time.Second, func() bool {
				return observe(t, recovered, credential).Failure != nil
			})
			failure := observe(t, recovered, credential).Failure
			if failure.Cause != api.FailureCauseCredential || failure.Classification != api.FailureClassRetryable {
				t.Fatalf("missing applier failure: %+v", failure)
			}
		})
	}
}

func TestDelegatedExecutorRecoversWithoutItsAdapter(t *testing.T) {
	root := t.TempDir()
	adapter := &RemoteJobDelegator{Name: "remote"} // Admission and recovery open no transport.
	if slices.Contains((DelegatedExecution{Delegators: []download.Delegator{adapterWithoutLocator{adapter}}}).ExecutionGuarantees(), string(download.CapRecoverableSubmission)) {
		t.Fatal("offered recoverable submission without Locator")
	}
	first, err := acceptanceprovider.OpenWithExecutor(root, "optional-delegate-owner", DelegatedExecution{Delegators: []download.Delegator{adapter}})
	if err != nil {
		t.Fatal(err)
	}
	window, err := first.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	input := request.Encode(&request.Request{Sources: []request.Source{{Scheme: "https", Locator: "https://example.invalid/model", Credential: "hf"}}})
	accepted, err := first.Bind("alice").Submit(api.Submission{Identity: api.RequestIdentity{Key: "retained", HistoryEpoch: window.HistoryEpoch},
		Kind: download.Kind, Spec: input, RequiredGuarantees: []string{string(download.CapRecoverableSubmission), CredentialGuarantee}})
	if err != nil || accepted.Outcome != api.AcceptanceOutcomeAccepted {
		t.Fatalf("first admission: %+v %v", accepted, err)
	}
	recovered, err := acceptanceprovider.OpenWithExecutor(root, "optional-delegate-owner", DelegatedExecution{})
	if err != nil {
		t.Fatalf("missing adapter rejected retained work: %v", err)
	}
	if offered := recovered.SupportedGuarantees(); slices.Contains(offered, string(download.CapRecoverableSubmission)) || slices.Contains(offered, CredentialGuarantee) {
		t.Fatalf("offered absent delegation or credential dependency: %v", offered)
	}
	fresh, err := recovered.Bind("alice").Submit(api.Submission{Identity: api.RequestIdentity{Key: "new", HistoryEpoch: window.HistoryEpoch},
		Kind: download.Kind, Spec: input, RequiredGuarantees: []string{string(download.CapRecoverableSubmission), CredentialGuarantee}})
	if err != nil || fresh.Outcome == api.AcceptanceOutcomeAccepted {
		t.Fatalf("accepted new work without adapter: %+v %v", fresh, err)
	}
	// Existing work remains accepted and can be resumed after an adapter returns.
	got, err := recovered.Bind("alice").Reconcile(api.RequestIdentity{Key: "retained", HistoryEpoch: window.HistoryEpoch})
	if err != nil || got.Outcome != api.AcceptanceOutcomeAccepted {
		t.Fatalf("retained work: %+v %v", got, err)
	}
	execute(t, recovered)
	waitFor(t, "accepted delegated work stays retryable", 10*time.Second, func() bool {
		return observe(t, recovered, api.RequestIdentity{Key: "retained", HistoryEpoch: window.HistoryEpoch}).Failure != nil
	})
	retained := observe(t, recovered, api.RequestIdentity{Key: "retained", HistoryEpoch: window.HistoryEpoch})
	if retained.State == api.WorkStateFailed || retained.Failure.Classification != api.FailureClassRetryable {
		t.Fatalf("absent delegate ended accepted work: %+v", retained)
	}
}
