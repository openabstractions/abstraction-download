package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	job "github.com/openabstractions/abstraction-job/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

type scopedTestApplier struct {
	*switchApplier
	available                    bool
	subject                      acceptanceprovider.AuthenticatedSubject
	scopedChecked, scopedApplied []string
}

type scopedApplierWithoutChecker struct {
	CredentialApplier
	ScopedSubjectCredentialApplier
	ScopedCredentialAvailability
}

type scopedCheckerWithoutApplier struct {
	CredentialApplier
	ScopedSubjectCredentialChecker
	ScopedCredentialAvailability
}

func (a *scopedTestApplier) ScopedCredentialsAvailable() bool { return a.available }

func (a *scopedTestApplier) CheckScopedSubjectCredential(_ context.Context, subject acceptanceprovider.AuthenticatedSubject, name, host string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.scopedChecked = append(a.scopedChecked, subject.Account+"|"+subject.Program+"|"+name+"|"+host)
	if subject != a.subject {
		return download.CredentialRefusal(name, "not_permitted")
	}
	if a.check != "applied" {
		return download.CredentialRefusal(name, a.check)
	}
	return nil
}

func (a *scopedTestApplier) ApplyScopedSubjectCredential(_ context.Context, subject acceptanceprovider.AuthenticatedSubject, name, host string) (map[string]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.scopedApplied = append(a.scopedApplied, subject.Account+"|"+subject.Program+"|"+name+"|"+host)
	if subject != a.subject {
		return nil, download.CredentialRefusal(name, "not_permitted")
	}
	if a.apply != "applied" {
		return nil, download.CredentialRefusal(name, a.apply)
	}
	return map[string]string{"Authorization": fmt.Sprintf("Bearer scoped-%d", len(a.scopedApplied))}, nil
}

func scopedBinding(t *testing.T) acceptanceprovider.Binding {
	t.Helper()
	subject := &acceptanceprovider.AuthenticatedSubject{AccountKind: "posix", Account: "1000", Executable: filepath.Join(t.TempDir(), "caller.exe"), Program: "package:caller"}
	scope, err := acceptanceprovider.LocalSubjectScope(subject.AccountKind, subject.Account, subject.Executable)
	if err != nil {
		t.Fatal(err)
	}
	return acceptanceprovider.Binding{Scope: scope, Subject: subject, Origin: "local"}
}

func newScopedApplier(binding acceptanceprovider.Binding) *scopedTestApplier {
	return &scopedTestApplier{switchApplier: &switchApplier{check: "applied", apply: "applied"}, available: true, subject: *binding.Subject}
}

func observeScoped(t *testing.T, p *acceptanceprovider.Provider, scope string, id api.RequestIdentity) *api.OperationSnapshot {
	t.Helper()
	v, err := p.BindOperations(scope).ObserveWork(id)
	if err != nil || v.Outcome != api.ObservationOutcomeObserved || v.Snapshot == nil {
		t.Fatalf("observe %+v %v", v, err)
	}
	return v.Snapshot
}

func TestExplicitScopedCredentialPreparationAndAdvertisement(t *testing.T) {
	binding := scopedBinding(t)
	applier := newScopedApplier(binding)
	e := HTTPExecution{Credentials: applier}
	raw := request.Encode(&request.Request{Sources: []request.Source{
		{Scheme: "https", Locator: "https://first.invalid/file", Credential: "first"},
		{Scheme: "https", Locator: "https://second.invalid/file", Credential: "second"},
		{Scheme: "https", Locator: "https://public.invalid/file"},
	}})
	for _, route := range []string{CredentialGuarantee, ScopedCredentialGuarantee} {
		work, _, err := e.PrepareScoped(binding.Scope, "operation", download.Kind, raw, []string{route})
		if err != nil {
			t.Fatal(err)
		}
		var spec download.Spec
		if err := json.Unmarshal(work, &spec); err != nil {
			t.Fatal(err)
		}
		for i, source := range spec.Sources {
			if i == 2 {
				if len(source.Attrs) != 0 {
					t.Fatalf("anonymous source gained authority attrs: %+v", source)
				}
				continue
			}
			got, err := download.CredentialRoute(source)
			if err != nil || got != route || source.Attrs[download.CredentialScopeAttr] != binding.Scope {
				t.Fatalf("source route %+v: %q %v", source, got, err)
			}
			if route == CredentialGuarantee && len(source.Attrs) != 2 {
				t.Fatalf("original preparation changed: %+v", source.Attrs)
			}
		}
		// Recovery prepares the exact original work while its applier is absent.
		again, _, err := (HTTPExecution{}).PrepareScoped(binding.Scope, "operation", download.Kind, raw, []string{route})
		if err != nil || !bytes.Equal(work, again) {
			t.Fatalf("route reinterpreted on recovery: %q %q %v", work, again, err)
		}
	}
	both := []string{CredentialGuarantee, ScopedCredentialGuarantee}
	if _, _, err := e.PrepareScoped(binding.Scope, "operation", download.Kind, raw, both); err == nil {
		t.Fatal("mixed requested routes prepared")
	}
	if outcome, _ := e.CheckSubjectAdmission(binding, download.Kind, raw, both); outcome != api.AcceptanceOutcomeInvalid {
		t.Fatalf("mixed routes admission %v", outcome)
	}
	if !slices.Contains(e.ExecutionGuarantees(), ScopedCredentialGuarantee) {
		t.Fatal("configured scoped enforcer not advertised")
	}
	for _, credentials := range []CredentialApplier{nil, &switchApplier{}, &scopedTestApplier{switchApplier: &switchApplier{}, available: false},
		scopedApplierWithoutChecker{applier, applier, applier}, scopedCheckerWithoutApplier{applier, applier, applier}} {
		if slices.Contains((HTTPExecution{Credentials: credentials}).ExecutionGuarantees(), ScopedCredentialGuarantee) {
			t.Fatal("scoped route advertised without configured enforcer")
		}
	}
}

func TestScopedAdmissionRequiresReceivingSubjectAndKeepsIntent(t *testing.T) {
	binding := scopedBinding(t)
	applier := newScopedApplier(binding)
	root := t.TempDir()
	p, err := acceptanceprovider.OpenWithExecutor(root, "scoped-admission", HTTPExecution{Credentials: applier})
	if err != nil {
		t.Fatal(err)
	}
	window, err := p.BindBinding(binding).GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	submission := credentialSubmission(window, "scoped", "http://example.invalid/file")
	submission.RequiredGuarantees = []string{ScopedCredentialGuarantee}
	result, err := p.Bind("opaque-only").Submit(submission)
	if err != nil || result.Outcome != api.AcceptanceOutcomeUnavailable {
		t.Fatalf("opaque subject accepted: %+v %v", result, err)
	}
	wrong := *binding.Subject
	wrong.Program = "package:other"
	wrongBinding := binding
	wrongBinding.Subject = &wrong
	result, err = p.BindBinding(wrongBinding).Submit(submission)
	if err != nil || result.Outcome != api.AcceptanceOutcomeForbidden || result.Reason != "credential:not_permitted:hf" {
		t.Fatalf("wrong subject accepted: %+v %v", result, err)
	}
	mixed := submission
	mixed.RequiredGuarantees = []string{CredentialGuarantee, ScopedCredentialGuarantee}
	result, err = p.BindBinding(binding).Submit(mixed)
	if err != nil || result.Outcome != api.AcceptanceOutcomeInvalid {
		t.Fatalf("mixed guarantee accepted: %+v %v", result, err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "acceptance", "requests"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("refusal journaled: %v %v", entries, err)
	}
	result, err = p.BindBinding(binding).Submit(submission)
	if err != nil || result.Outcome != api.AcceptanceOutcomeAccepted || !slices.Contains(result.Receipt.AcceptedGuarantees, ScopedCredentialGuarantee) {
		t.Fatalf("scoped admission %+v %v", result, err)
	}
	original := submission
	original.RequiredGuarantees = []string{CredentialGuarantee}
	result, err = p.BindBinding(binding).Submit(original)
	if err != nil || result.Outcome == api.AcceptanceOutcomeAccepted {
		t.Fatalf("same identity changed route: %+v %v", result, err)
	}
	if len(applier.checked) != 0 || len(applier.subjectChecked) != 0 || len(applier.scopedChecked) != 2 {
		t.Fatalf("admission downgraded: original=%v subject=%v scoped=%v", applier.checked, applier.subjectChecked, applier.scopedChecked)
	}
}

func TestScopedExecutionNeverFallsBackToOriginalSubjectOrScope(t *testing.T) {
	binding := scopedBinding(t)
	src := download.Source{Attrs: map[string]string{download.CredentialAttr: "hf", download.CredentialScopeAttr: binding.Scope, download.CredentialRouteAttr: ScopedCredentialGuarantee, "operation_id": "another-operation"}}
	for _, test := range []struct {
		name    string
		lookup  acceptanceprovider.SubjectLookup
		applier CredentialApplier
	}{
		{"no lookup", nil, newScopedApplier(binding)},
		{"subject missing", func(string, string) (*acceptanceprovider.AuthenticatedSubject, bool, error) { return nil, true, nil }, newScopedApplier(binding)},
		{"owner missing", func(string, string) (*acceptanceprovider.AuthenticatedSubject, bool, error) { return nil, false, nil }, newScopedApplier(binding)},
		{"lookup outage", func(string, string) (*acceptanceprovider.AuthenticatedSubject, bool, error) {
			return nil, false, errors.New("outage")
		}, newScopedApplier(binding)},
		{"original only", func(string, string) (*acceptanceprovider.AuthenticatedSubject, bool, error) {
			return binding.Subject, true, nil
		}, &switchApplier{apply: "applied"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			creds := appliedCredentials{ctx: context.Background(), applier: test.applier, lookup: test.lookup, operationID: "claimed-operation"}
			_, err := creds.LookupSource(src, "example.invalid")
			var refusal *download.CredentialError
			if !errors.As(err, &refusal) || refusal.Outcome != "unavailable" {
				t.Fatalf("missing scoped dependency: %v", err)
			}
		})
	}
	applier := newScopedApplier(binding)
	lookup := func(operation, scope string) (*acceptanceprovider.AuthenticatedSubject, bool, error) {
		if operation != "claimed-operation" || scope != binding.Scope {
			t.Fatalf("source selected another job: %q %q", operation, scope)
		}
		return binding.Subject, true, nil
	}
	creds := appliedCredentials{ctx: context.Background(), applier: applier, lookup: lookup}
	bound := creds.ForOperation("claimed-operation").(download.SourceCredentials)
	if _, err := bound.LookupSource(src, "example.invalid"); err != nil {
		t.Fatal(err)
	}
	if len(applier.appliedScopes) != 0 || len(applier.subjectApplied) != 0 || len(applier.scopedApplied) != 1 {
		t.Fatal("scoped execution used original applier")
	}
	wrong := *binding.Subject
	wrong.Account = "1001"
	creds.lookup = func(string, string) (*acceptanceprovider.AuthenticatedSubject, bool, error) { return &wrong, true, nil }
	bound = creds.ForOperation("claimed-operation").(download.SourceCredentials)
	headers, err := bound.LookupSource(src, "example.invalid")
	var refusal *download.CredentialError
	if headers != nil || !errors.As(err, &refusal) || refusal.Outcome != "not_permitted" {
		t.Fatalf("wrong execution subject applied credential: %v %v", headers, err)
	}
}

func TestScopedCredentialRecoveryRetainsRouteWithOriginalApplierOnly(t *testing.T) {
	fastService(t)
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint("legacy=", legacy), func(t *testing.T) {
			binding := scopedBinding(t)
			wrap := func(e HTTPExecution) acceptanceprovider.Executor {
				if legacy {
					return LegacySinkExecution{HTTPExecution: e}
				}
				return e
			}
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Error("scoped recovery downgraded and contacted origin")
			}))
			defer origin.Close()
			root := t.TempDir()
			p, err := acceptanceprovider.OpenWithExecutor(root, "scoped-recovery", wrap(HTTPExecution{Credentials: newScopedApplier(binding)}))
			if err != nil {
				t.Fatal(err)
			}
			window, err := p.BindBinding(binding).GetHistoryWindow()
			if err != nil {
				t.Fatal(err)
			}
			submission := credentialSubmission(window, "recover", origin.URL+"/file")
			submission.RequiredGuarantees = []string{ScopedCredentialGuarantee}
			accepted, err := p.BindBinding(binding).Submit(submission)
			if err != nil || accepted.Outcome != api.AcceptanceOutcomeAccepted {
				t.Fatalf("accept %+v %v", accepted, err)
			}
			original := &switchApplier{check: "applied", apply: "applied"}
			p, err = acceptanceprovider.OpenWithExecutor(root, "scoped-recovery", wrap(HTTPExecution{Credentials: original}))
			if err != nil {
				t.Fatalf("accepted route unavailable on recovery: %v", err)
			}
			if slices.Contains(p.SupportedGuarantees(), ScopedCredentialGuarantee) {
				t.Fatal("offered absent scoped dependency")
			}
			reconciled, err := p.BindBinding(binding).Reconcile(submission.Identity)
			if err != nil || reconciled.Outcome != api.AcceptanceOutcomeAccepted || !slices.Contains(reconciled.Receipt.AcceptedGuarantees, ScopedCredentialGuarantee) {
				t.Fatalf("recovery dropped promise: %+v %v", reconciled, err)
			}
			store, err := job.NewFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			record, err := store.Load(accepted.Receipt.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			spec, err := download.SpecOf(record)
			if err != nil || spec.Sources[0].Attrs[download.CredentialRouteAttr] != ScopedCredentialGuarantee {
				t.Fatalf("recovery changed route: %+v %v", spec, err)
			}
			execute(t, p)
			waitFor(t, "scoped dependency unavailable", 10*time.Second, func() bool { return observeScoped(t, p, binding.Scope, submission.Identity).Failure != nil })
			failure := observeScoped(t, p, binding.Scope, submission.Identity).Failure
			if failure.Cause != api.FailureCauseCredential || failure.Classification != api.FailureClassRetryable || !strings.Contains(failure.Message, "credential:unavailable:hf") {
				t.Fatalf("missing scoped dependency: %+v", failure)
			}
			if len(original.appliedScopes) != 0 || len(original.subjectApplied) != 0 {
				t.Fatal("recovery applied original credential")
			}
		})
	}
}

func TestOperationBoundCredentialsForParallelProbesRangesAndRedirects(t *testing.T) {
	fastService(t)
	body := testBody(32 << 20)
	for _, route := range []string{CredentialGuarantee, ScopedCredentialGuarantee} {
		t.Run(route, func(t *testing.T) {
			binding := scopedBinding(t)
			applier := newScopedApplier(binding)
			var mu sync.Mutex
			var ranges, headers, hosts []string
			var targetURL string
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				ranges, headers, hosts = append(ranges, r.Header.Get("Range")), append(headers, r.Header.Get("Authorization")), append(hosts, r.Host)
				mu.Unlock()
				switch r.URL.Path {
				case "/start":
					http.Redirect(w, r, "/same-host", http.StatusTemporaryRedirect)
				case "/same-host":
					http.Redirect(w, r, targetURL+"/body", http.StatusTemporaryRedirect)
				default:
					http.ServeContent(w, r, "file", time.Time{}, bytes.NewReader(body))
				}
			}))
			defer origin.Close()
			targetURL = strings.Replace(origin.URL, "127.0.0.1", "localhost", 1)
			p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "scoped-ranges", HTTPExecution{Credentials: applier})
			if err != nil {
				t.Fatal(err)
			}
			window, err := p.BindBinding(binding).GetHistoryWindow()
			if err != nil {
				t.Fatal(err)
			}
			input := request.Encode(&request.Request{Artifact: request.Artifact{Size: int64(len(body)), Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(body))}, Sources: []request.Source{{Scheme: "http", Locator: origin.URL + "/start", Credential: "hf"}}})
			id := api.RequestIdentity{Key: "range-job", HistoryEpoch: window.HistoryEpoch}
			result, err := p.BindBinding(binding).Submit(api.Submission{Identity: id, Kind: download.Kind, Spec: input, RequiredGuarantees: []string{route}})
			if err != nil || result.Outcome != api.AcceptanceOutcomeAccepted {
				t.Fatalf("submit %+v %v", result, err)
			}
			execute(t, p)
			waitFor(t, "operation-bound parallel requests complete", 15*time.Second, func() bool { return observeScoped(t, p, binding.Scope, id).State == api.WorkStateComplete })
			mu.Lock()
			defer mu.Unlock()
			applier.mu.Lock()
			defer applier.mu.Unlock()
			applied := applier.subjectApplied
			if route == ScopedCredentialGuarantee {
				applied = applier.scopedApplied
				if len(applier.subjectApplied) != 0 || len(applier.appliedScopes) != 0 {
					t.Fatal("scoped range downgraded")
				}
			} else if len(applier.scopedApplied) != 0 || len(applier.appliedScopes) != 0 {
				t.Fatal("original range changed route or lost subject")
			}
			if len(ranges) < 9 || len(applied) != len(ranges) || !slices.Contains(ranges, "bytes=0-0") || !slices.Contains(ranges, "bytes=16777216-33554431") {
				t.Fatalf("parallel operation was unbound or not rechecked: ranges=%v applied=%v", ranges, applied)
			}
			for i, header := range headers {
				if route == ScopedCredentialGuarantee && (!strings.HasPrefix(header, "Bearer scoped-") || slices.Index(headers[:i], header) >= 0) {
					t.Fatalf("reused scoped headers: %v", headers)
				} else if header == "" {
					t.Fatalf("credential missing on redirect: %v", hosts)
				}
			}
		})
	}
}

func TestDelegationRefusesScopedCredentialRoute(t *testing.T) {
	binding := scopedBinding(t)
	e := DelegatedExecution{HTTPExecution: HTTPExecution{Credentials: newScopedApplier(binding)}, Delegators: []download.Delegator{&RemoteJobDelegator{Name: "remote"}}}
	if slices.Contains(e.ExecutionGuarantees(), ScopedCredentialGuarantee) {
		t.Fatal("delegation advertised scoped authorization")
	}
	raw := request.Encode(&request.Request{Sources: []request.Source{{Scheme: "https", Locator: "https://example.invalid/file", Credential: "hf"}}})
	if _, _, err := e.PrepareScoped(binding.Scope, "operation", download.Kind, raw, []string{ScopedCredentialGuarantee}); err == nil {
		t.Fatal("delegation prepared scoped route")
	}
	if outcome, _ := e.CheckSubjectAdmission(binding, download.Kind, raw, []string{ScopedCredentialGuarantee}); outcome != api.AcceptanceOutcomeInvalid {
		t.Fatal("delegation admitted scoped route")
	}
	spec := download.Spec{Sources: []download.Source{{Scheme: "https", Locator: "https://example.invalid/file", Attrs: map[string]string{download.CredentialAttr: "hf", download.CredentialScopeAttr: binding.Scope, download.CredentialRouteAttr: ScopedCredentialGuarantee}}}}
	if _, _, err := remoteRequest(spec); err == nil {
		t.Fatal("remote request downgraded scoped route")
	}
	if _, ok := download.NewDelegators(e.Delegators...).ForSpec(spec, spec.Sources[0], nil); ok {
		t.Fatal("runner delegated scoped attrs without a preserved authority protocol")
	}
}
