package serve

import (
	"slices"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-download/go/netcost"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

func TestRemoteRequestPreservesNetworkConstraint(t *testing.T) {
	for _, test := range []struct {
		name        string
		constraints *download.Constraints
		wantNetwork request.Network
		wantCost    bool
	}{
		{name: "absent"},
		{name: "any", constraints: &download.Constraints{Network: "any"}},
		{name: "unmetered", constraints: &download.Constraints{Network: download.NetworkUnmetered},
			wantNetwork: request.NetworkUnmetered, wantCost: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, required, err := remoteRequest(download.Spec{
				Artifact:    download.Artifact{Digest: "sha256:abcd", Size: 4},
				Sources:     []download.Source{{Scheme: "https", Locator: "https://example.invalid/model"}},
				Constraints: test.constraints,
			})
			if err != nil {
				t.Fatal(err)
			}
			got, err := request.Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got.Artifact.Digest != "sha256:abcd" || got.Artifact.Size != 4 || len(got.Sources) != 1 {
				t.Fatalf("remote request lost artifact or source: %+v", got)
			}
			if got.Constraints == nil && test.wantNetwork != 0 || got.Constraints != nil && got.Constraints.Network != test.wantNetwork {
				t.Fatalf("remote constraint = %+v, want network %v", got.Constraints, test.wantNetwork)
			}
			if slices.Contains(required, NetworkCostGuarantee) != test.wantCost {
				t.Fatalf("required guarantees = %v, network-cost present = %v", required, slices.Contains(required, NetworkCostGuarantee))
			}
			if !slices.Contains(required, acceptanceprovider.GuaranteeReconciliation) {
				t.Fatalf("required guarantees lost reconciliation: %v", required)
			}
		})
	}
}

func TestRemoteAdmissionRefusesUnmeteredWithoutNetworkCostSource(t *testing.T) {
	raw, required, err := remoteRequest(download.Spec{
		Artifact:    download.Artifact{Size: 4},
		Sources:     []download.Source{{Scheme: "https", Locator: "https://example.invalid/model"}},
		Constraints: &download.Constraints{Network: download.NetworkUnmetered},
	})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	execution := HTTPExecution{NetworkCost: func() (netcost.Source, error) { return nil, netcost.ErrUnavailable }}
	provider, err := acceptanceprovider.OpenWithExecutor(root, "remote-without-network-cost", execution)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(execution.ExecutionGuarantees(), NetworkCostGuarantee) {
		t.Fatal("remote without a cost source offers network-cost@1 to new work")
	}
	window, err := provider.Bind("caller").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Bind("caller").Submit(api.Submission{
		Identity:           api.RequestIdentity{Key: "unmetered", HistoryEpoch: window.HistoryEpoch},
		Kind:               download.Kind,
		Spec:               raw,
		RequiredGuarantees: required,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != api.AcceptanceOutcomeDefinitelyNotAccepted || result.Receipt != nil || result.Reason == "" {
		t.Fatalf("remote admitted unmetered work without a cost source: %+v", result)
	}
}
