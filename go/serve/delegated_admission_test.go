package serve

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

type limitedRemoteAdapter struct {
	*RemoteJobDelegator
	caps    []download.Capability
	schemes []string
}

func (d limitedRemoteAdapter) Capabilities() []download.Capability { return d.caps }
func (d limitedRemoteAdapter) Schemes() []string                   { return d.schemes }

func TestDelegatedAdmissionNeedsOneAdapterForWholeRequest(t *testing.T) {
	recoverable := download.CapRecoverableSubmission
	credential := download.Capability(CredentialGuarantee)
	adapter := func(name string, caps []download.Capability, schemes ...string) limitedRemoteAdapter {
		return limitedRemoteAdapter{RemoteJobDelegator: &RemoteJobDelegator{Name: name}, caps: caps, schemes: schemes}
	}
	for _, test := range []struct {
		name     string
		adapters []download.Delegator
		accepted bool
	}{
		{"split-guarantees", []download.Delegator{
			adapter("recovery", []download.Capability{recoverable}, "https"),
			adapter("credential", []download.Capability{credential}, "https"),
		}, false},
		{"wrong-scheme", []download.Delegator{
			adapter("http-only", []download.Capability{recoverable, credential}, "http"),
		}, false},
		{"one-complete-adapter", []download.Delegator{
			adapter("complete", []download.Capability{recoverable, credential}, "https"),
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			provider, err := acceptanceprovider.OpenWithExecutor(root, "delegated-admission", DelegatedExecution{Delegators: test.adapters})
			if err != nil {
				t.Fatal(err)
			}
			window, err := provider.Bind("caller").GetHistoryWindow()
			if err != nil {
				t.Fatal(err)
			}
			required := []string{string(recoverable), CredentialGuarantee}
			if !slices.Contains(provider.SupportedGuarantees(), required[0]) || !slices.Contains(provider.SupportedGuarantees(), required[1]) {
				t.Fatalf("fixture must offer both guarantees: %v", provider.SupportedGuarantees())
			}
			input := request.Encode(&request.Request{Sources: []request.Source{{Scheme: "https", Locator: "https://example.invalid/model", Credential: "hf"}}})
			result, err := provider.Bind("caller").Submit(api.Submission{Identity: api.RequestIdentity{Key: "candidate", HistoryEpoch: window.HistoryEpoch},
				Kind: download.Kind, Spec: input, RequiredGuarantees: required})
			if err != nil {
				t.Fatal(err)
			}
			if test.accepted {
				if result.Outcome != api.AcceptanceOutcomeAccepted {
					t.Fatalf("complete adapter refused: %+v", result)
				}
				return
			}
			if result.Outcome != api.AcceptanceOutcomeInvalid || result.Receipt != nil || result.Reason == "" {
				t.Fatalf("split or wrong-scheme adapter admitted work: %+v", result)
			}
			entries, err := os.ReadDir(filepath.Join(root, "acceptance", "requests"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("refusal left a journal: %v %v", entries, err)
			}
		})
	}
}

func TestBoundDelegatedAdmissionKeepsAdapterEligibility(t *testing.T) {
	recoverable := download.CapRecoverableSubmission
	credential := download.Capability(CredentialGuarantee)
	providers := []download.Delegator{
		limitedRemoteAdapter{RemoteJobDelegator: &RemoteJobDelegator{Name: "recovery"}, caps: []download.Capability{recoverable}, schemes: []string{"https"}},
		limitedRemoteAdapter{RemoteJobDelegator: &RemoteJobDelegator{Name: "credential"}, caps: []download.Capability{credential}, schemes: []string{"https"}},
	}
	root := t.TempDir()
	p, err := acceptanceprovider.OpenWithExecutor(root, "bound-delegated-admission", DelegatedExecution{Delegators: providers})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "caller")
	subject := acceptanceprovider.AuthenticatedSubject{AccountKind: "posix", Account: "1000", Executable: path, Program: path}
	scope, err := acceptanceprovider.LocalSubjectScope(subject.AccountKind, subject.Account, path)
	if err != nil {
		t.Fatal(err)
	}
	binding := acceptanceprovider.Binding{Scope: scope, Subject: &subject, Origin: "local"}
	window, err := p.BindBinding(binding).GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	input := request.Encode(&request.Request{Sources: []request.Source{{Scheme: "https", Locator: "https://example.invalid/model", Credential: "hf"}}})
	result, err := p.BindBinding(binding).Submit(api.Submission{Identity: api.RequestIdentity{Key: "split", HistoryEpoch: window.HistoryEpoch},
		Kind: download.Kind, Spec: input, RequiredGuarantees: []string{string(recoverable), CredentialGuarantee}})
	if err != nil || result.Outcome != api.AcceptanceOutcomeInvalid || result.Receipt != nil || result.Reason != "no configured delegate can serve the requested source and guarantees" {
		t.Fatalf("bound delegated admission %+v %v", result, err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "acceptance", "requests"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("bound refusal journaled: %v %v", entries, err)
	}
}
