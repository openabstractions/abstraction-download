package serve

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// switchApplier answers Check and Apply with the outcomes a test sets.
type switchApplier struct {
	mu             sync.Mutex
	check, apply   string
	checked        []string
	appliedScopes  []string
	subjectChecked []string
	subjectApplied []string
}

func (a *switchApplier) set(check, apply string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.check, a.apply = check, apply
}

func (a *switchApplier) CheckCredential(_ context.Context, scope, name, host string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.checked = append(a.checked, scope+"|"+name+"|"+host)
	if a.check != "applied" {
		return download.CredentialRefusal(name, a.check)
	}
	return nil
}

func (a *switchApplier) ApplyCredential(_ context.Context, scope, name, host string) (map[string]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appliedScopes = append(a.appliedScopes, scope)
	if a.apply != "applied" {
		return nil, download.CredentialRefusal(name, a.apply)
	}
	return map[string]string{"Authorization": "Bearer admission-test-secret"}, nil
}

func (a *switchApplier) CheckSubjectCredential(_ context.Context, subject acceptanceprovider.AuthenticatedSubject, name, host string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.subjectChecked = append(a.subjectChecked, subject.Account+"|"+subject.Program+"|"+name+"|"+host)
	if a.check != "applied" {
		return download.CredentialRefusal(name, a.check)
	}
	return nil
}

func (a *switchApplier) ApplySubjectCredential(_ context.Context, subject acceptanceprovider.AuthenticatedSubject, name, host string) (map[string]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.subjectApplied = append(a.subjectApplied, subject.Account+"|"+subject.Program+"|"+name+"|"+host)
	if a.apply != "applied" {
		return nil, download.CredentialRefusal(name, a.apply)
	}
	return map[string]string{"Authorization": "Bearer subject-test-secret"}, nil
}

func credentialSubmission(window api.HistoryWindow, key, locator string) api.Submission {
	spec := request.Encode(&request.Request{Sources: []request.Source{{Scheme: "http", Locator: locator, Credential: "hf"}}})
	return api.Submission{Identity: api.RequestIdentity{Key: key, HistoryEpoch: window.HistoryEpoch}, Kind: "download", Spec: spec, RequiredGuarantees: []string{CredentialGuarantee}}
}

// A submission naming a credential the caller may not apply is refused at
// Submit: invalid with credential:<outcome>:<name>, or unavailable, with no
// journal, seal or work. The same identity is accepted once the check admits
// it. A credential refused at execution ends the attempt with the typed cause
// credential and the applier's outcome in the message [JOB-A16, DL-K1].
func TestCredentialRefusedAtAdmissionAndTypedAtExecution(t *testing.T) {
	var mu sync.Mutex
	seen := []string{}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Write([]byte("gated"))
	}))
	defer origin.Close()
	applier := &switchApplier{}
	root := t.TempDir()
	p, err := acceptanceprovider.OpenWithExecutor(root, "admission-owner", HTTPExecution{Credentials: applier})
	if err != nil {
		t.Fatal(err)
	}
	fastService(t)
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	s := credentialSubmission(window, "gated", origin.URL+"/gated")
	for outcome, want := range map[string][2]string{
		"not_permitted": {"invalid", "credential:not_permitted:hf"},
		"unknown":       {"invalid", "credential:unknown:hf"},
		"unavailable":   {"unavailable", "credential:unavailable:hf"},
	} {
		applier.set(outcome, "applied")
		v, err := p.Bind("alice").Submit(s)
		if err != nil || v.Outcome.String() != want[0] || v.Reason != want[1] || v.Receipt != nil {
			t.Fatalf("%s: %+v %v", outcome, v, err)
		}
		entries, err := os.ReadDir(filepath.Join(root, "acceptance", "requests"))
		if err != nil || len(entries) != 0 {
			t.Fatalf("%s left a journal: %v %v", outcome, entries, err)
		}
	}
	if len(applier.checked) != 3 || applier.checked[0] != "alice|hf|127.0.0.1" {
		t.Fatalf("checked %v", applier.checked)
	}
	// Admitted, then revoked before execution: the attempt ends typed.
	applier.set("applied", "revoked")
	v, err := p.Bind("alice").Submit(s)
	if err != nil || v.Outcome.String() != "accepted" {
		t.Fatalf("admitted: %+v %v", v, err)
	}
	execute(t, p)
	waitFor(t, "a typed credential failure", 20*time.Second, func() bool { return observe(t, p, s.Identity).State.String() == "failed" })
	snapshot := observe(t, p, s.Identity)
	if f := snapshot.Failure; f == nil || f.Classification.String() != "permanent" || f.Cause != api.FailureCauseCredential || f.Message != "download attempt failed: credential:revoked:hf" {
		t.Fatalf("failure %+v", snapshot.Failure)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 0 {
		t.Fatalf("a refused credential reached the origin: %q", seen)
	}
}

func TestReceiverSubjectSurvivesRestartAndRevocation(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("revoked credential reached origin")
	}))
	defer origin.Close()
	fastService(t)
	applier := &switchApplier{check: "applied", apply: "revoked"}
	root := t.TempDir()
	execution := HTTPExecution{Credentials: applier}
	p, err := acceptanceprovider.OpenWithExecutor(root, "subject-owner", execution)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "packaged-caller.exe")
	bound := acceptanceprovider.AuthenticatedSubject{AccountKind: "posix", Account: "1000", Executable: path, Program: "msix:example.family"}
	scope, err := acceptanceprovider.LocalSubjectScope(bound.AccountKind, bound.Account, bound.Executable)
	if err != nil {
		t.Fatal(err)
	}
	binding := acceptanceprovider.Binding{Scope: scope, Origin: "local", Subject: &bound}
	window, err := p.BindBinding(binding).GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	submission := credentialSubmission(window, "subject-restart", origin.URL+"/gated")
	admitted, err := p.BindBinding(binding).Submit(submission)
	if err != nil || admitted.Outcome != api.AcceptanceOutcomeAccepted || admitted.Receipt == nil {
		t.Fatalf("admission %+v %v", admitted, err)
	}
	if len(applier.subjectChecked) != 1 || len(applier.checked) != 0 {
		t.Fatalf("admission inverted scope: subject=%v legacy=%v", applier.subjectChecked, applier.checked)
	}
	p, err = acceptanceprovider.OpenWithExecutor(root, "subject-owner", execution)
	if err != nil {
		t.Fatal(err)
	}
	execute(t, p)
	observed := func() *api.OperationSnapshot {
		t.Helper()
		result, err := p.BindOperations(scope).ObserveWork(submission.Identity)
		if err != nil || result.Outcome != api.ObservationOutcomeObserved || result.Snapshot == nil {
			t.Fatalf("observe %+v %v", result, err)
		}
		return result.Snapshot
	}
	waitFor(t, "revoked subject credential", 20*time.Second, func() bool {
		return observed().State.String() == "failed"
	})
	if len(applier.subjectApplied) == 0 || len(applier.appliedScopes) != 0 {
		t.Fatalf("execution inverted scope: subject=%v legacy=%v", applier.subjectApplied, applier.appliedScopes)
	}
	if got := observed().Failure; got == nil || got.Cause != api.FailureCauseCredential || got.Message != "download attempt failed: credential:revoked:hf" {
		t.Fatalf("revocation %+v", got)
	}
}
