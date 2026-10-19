package serve

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
)

func TestHTTPPreparationOwnsDestinations(t *testing.T) {
	e := HTTPExecution{}
	raw := []byte(`{"artifact":{},"sources":[{"scheme":"https","locator":"https://example.com/file"}]}`)
	prepared, err := e.Prepare("operation123", "download", raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := e.Prepare("operation123", "download", raw)
	if err != nil || !bytes.Equal(prepared, again) {
		t.Fatal("unstable preparation")
	}
	var spec download.Spec
	if err := json.Unmarshal(prepared, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Sink.Partial != "work/operation123" || spec.Sink.Final != "results/operation123" {
		t.Fatalf("destinations: %+v", spec.Sink)
	}
	if _, err := e.Prepare("../outside", "download", raw); err == nil {
		t.Fatal("unsafe identity accepted")
	}
	if _, err := e.Prepare("id", "other", raw); err == nil {
		t.Fatal("unsupported kind accepted")
	}
}

func TestHTTPPreparationRefusesUnsupportedRequests(t *testing.T) {
	for _, raw := range []string{
		`{"artifact":{},"sources":[],"sink":{"final":"elsewhere"}}`,
		`{"artifact":{},"sources":[]}`,
		`{"artifact":{"size":-1},"sources":[{"scheme":"https","locator":"https://example.com"}]}`,
		`{"artifact":{"digest":"sha256:bad"},"sources":[{"scheme":"https","locator":"https://example.com"}]}`,
		`{"artifact":{"digest":"sha256:../../x"},"sources":[{"scheme":"https","locator":"https://example.com"}]}`,
		// W0-06(p): uppercase hex passed hex.DecodeString before the shared grammar.
		`{"artifact":{"digest":"sha256:ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB"},"sources":[{"scheme":"https","locator":"https://example.com"}]}`,
		`{"artifact":{"digest":"abababababababababababababababababababababababababababababababab"},"sources":[{"scheme":"https","locator":"https://example.com"}]}`,
		`{"artifact":{},"sources":[{"scheme":"https","locator":"https://user:secret@example.com"}]}`,
		`{"artifact":{},"sources":[{"scheme":"file","locator":"file:///private"}]}`,
		`{"artifact":{},"sources":[{"scheme":"http","locator":"https://example.com"}]}`,
	} {
		if _, err := (HTTPExecution{}).Prepare("id", "download", []byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestHTTPPreparationNamesInputRefusalAndKeepsNamedCredentials(t *testing.T) {
	for _, test := range []struct{ name, scheme, locator, want string }{
		{"file", "file", "file:///owned/input", `unsupported source scheme "file"`},
		{"ftp", "ftp", "ftp://example.com/input", `unsupported source scheme "ftp"`},
		{"userinfo", "https", "https://user:fixture-private-value@example.com/input", "userinfo"},
		{"malformed", "https", "https://example.com/%invalid", "locator"},
		{"hostless", "http", "http:///input", "host"},
		{"mismatch", "http", "https://example.com/input", "scheme does not match"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]any{"artifact": map[string]any{}, "sources": []map[string]string{{"scheme": test.scheme, "locator": test.locator}}})
			_, err := (HTTPExecution{}).Prepare("operation", download.Kind, raw)
			if err == nil || !strings.Contains(err.Error(), test.want) || strings.Contains(err.Error(), "anonymous") || strings.Contains(err.Error(), "fixture-private-value") {
				t.Fatalf("wrong refusal cause: %v", err)
			}
		})
	}
	for _, scheme := range []string{"http", "https"} {
		t.Run("named-"+scheme, func(t *testing.T) {
			applier := &switchApplier{check: "applied"}
			e := HTTPExecution{Credentials: applier}
			raw, _ := json.Marshal(map[string]any{"artifact": map[string]any{}, "sources": []map[string]string{{"scheme": scheme, "locator": scheme + "://example.com/input", "credential": "fixture-name"}}})
			work, _, err := e.PrepareScoped("fixture-scope", "operation", download.Kind, raw, []string{CredentialGuarantee})
			if err != nil {
				t.Fatal(err)
			}
			var spec download.Spec
			if err := json.Unmarshal(work, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.Sources[0].Attrs[download.CredentialAttr] != "fixture-name" || spec.Sources[0].Attrs[download.CredentialScopeAttr] != "fixture-scope" {
				t.Fatal("named credential scope lost")
			}
			if outcome, reason := e.CheckAdmission("fixture-scope", download.Kind, raw, []string{CredentialGuarantee}); outcome != 0 || reason != "" {
				t.Fatalf("named policy positive: %v %q", outcome, reason)
			}
			if len(applier.checked) != 1 || applier.checked[0] != "fixture-scope|fixture-name|example.com" || len(applier.appliedScopes) != 0 {
				t.Fatal("admission skipped policy or applied a secret")
			}
			applier.set("not_permitted", "")
			if outcome, _ := e.CheckAdmission("fixture-scope", download.Kind, raw, []string{CredentialGuarantee}); outcome == 0 {
				t.Fatal("named credential policy refusal was lost")
			}
		})
	}
}
