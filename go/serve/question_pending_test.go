package serve

import (
	"context"
	"strings"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// questionsChecker refuses every subject check not_permitted, carrying the
// pending first-use questions the decision raised.
type questionsChecker struct {
	*scopedTestApplier
	pending []string
}

type pendingRefusal struct {
	error
	questions []string
}

func (e pendingRefusal) Unwrap() error              { return e.error }
func (e pendingRefusal) PendingQuestions() []string { return e.questions }

func (c questionsChecker) refuse(name string) error {
	return pendingRefusal{error: download.CredentialRefusal(name, "not_permitted"), questions: c.pending}
}

func (c questionsChecker) CheckSubjectCredential(_ context.Context, _ acceptanceprovider.AuthenticatedSubject, name, _ string) error {
	return c.refuse(name)
}

func (c questionsChecker) CheckScopedSubjectCredential(_ context.Context, _ acceptanceprovider.AuthenticatedSubject, name, _ string) error {
	return c.refuse(name)
}

// TestCredentialAdmissionNamesPendingQuestions: a bound submission refused
// because the caller may not apply its credential names, after its grant
// words and in order, each first-use question the check raised that is still
// pending, on the original and the scoped route alike (INF-W11). A refusal
// carrying none reads as before.
func TestCredentialAdmissionNamesPendingQuestions(t *testing.T) {
	binding := scopedBinding(t)
	spec := request.Encode(&request.Request{Sources: []request.Source{{Scheme: "http", Locator: "https://huggingface.co/gated", Credential: "hf"}}})
	for _, route := range []string{CredentialGuarantee, ScopedCredentialGuarantee} {
		for _, row := range []struct {
			pending []string
			want    string
		}{
			{[]string{"q-apply", "q-apply", "bad id", ""}, "credential:not_permitted:hf rights:question_pending:q-apply"},
			{[]string{"q-1", "q-2"}, "credential:not_permitted:hf rights:question_pending:q-1 rights:question_pending:q-2"},
			{nil, "credential:not_permitted:hf"},
			// Naming stops before the 1024-byte acceptance reason bound.
			{[]string{"q-1", strings.Repeat("x", 1000), "q-3"}, "credential:not_permitted:hf rights:question_pending:q-1"},
		} {
			e := HTTPExecution{Credentials: questionsChecker{scopedTestApplier: newScopedApplier(binding), pending: row.pending}}
			outcome, reason := e.CheckSubjectAdmission(binding, download.Kind, spec, []string{route})
			if outcome.String() != "forbidden" || reason != row.want {
				t.Fatalf("%s %v: %s %q, want forbidden %q", route, row.pending, outcome, reason, row.want)
			}
		}
	}
}
