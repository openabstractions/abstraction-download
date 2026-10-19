package download

import (
	"encoding/json"
	"errors"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

func TestRetainedCredentialRoutesRefuseUnknownMalformedAndMixed(t *testing.T) {
	for _, test := range []struct {
		name    string
		sources []Source
	}{
		{"unknown", []Source{{Attrs: map[string]string{CredentialAttr: "hf", CredentialScopeAttr: "scope", CredentialRouteAttr: "abstraction.download/credentials@3"}}}},
		{"empty", []Source{{Attrs: map[string]string{CredentialAttr: "hf", CredentialRouteAttr: ""}}}},
		{"missing name", []Source{{Attrs: map[string]string{CredentialScopeAttr: "scope", CredentialRouteAttr: CredentialRouteScoped}}}},
		{"missing scope", []Source{{Attrs: map[string]string{CredentialAttr: "hf", CredentialRouteAttr: CredentialRouteScoped}}}},
		{"mixed", []Source{{Attrs: map[string]string{CredentialAttr: "hf"}}, {Attrs: map[string]string{CredentialAttr: "other", CredentialScopeAttr: "scope", CredentialRouteAttr: CredentialRouteScoped}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for i := range test.sources {
				test.sources[i].Scheme, test.sources[i].Locator = "https", "https://example.invalid/file"
			}
			spec := Spec{Sources: test.sources, Sink: Sink{Final: "results/job"}}
			if err := spec.ValidateFor("job"); err == nil {
				t.Fatal("invalid retained route accepted at submission")
			}
			raw, err := json.Marshal(spec)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := SpecOf(&job.Record{ID: "job", Kind: Kind, Spec: raw}); err == nil {
				t.Fatal("invalid retained route accepted for execution")
			}
		})
	}
	old := Source{Attrs: map[string]string{CredentialAttr: "hf"}}
	if route, err := CredentialRoute(old); err != nil || route != CredentialRouteOriginal {
		t.Fatalf("old source changed route: %q %v", route, err)
	}
}

func TestScopedCredentialRouteCannotUseEnvironmentBridge(t *testing.T) {
	t.Setenv("ABSTRACTION_CRED_HF", "environment-value")
	t.Setenv("ABSTRACTION_CRED_HF_HOSTS", "example.invalid")
	src := Source{Scheme: "https", Locator: "https://example.invalid/file", Attrs: map[string]string{CredentialAttr: "hf", CredentialScopeAttr: "scope", CredentialRouteAttr: CredentialRouteScoped}}
	headers, err := headersFor(src, EnvCredentials{})
	var refusal *CredentialError
	if headers != nil || !errors.As(err, &refusal) || refusal.Outcome != "unavailable" {
		t.Fatalf("scoped route used original environment bridge: %v %v", headers, err)
	}
}
