package serve

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	job "github.com/openabstractions/abstraction-job/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

type admissionRefusingRemote struct {
	outcome api.AcceptanceOutcome
	reason  string
	submits int
}

func (*admissionRefusingRemote) GetHistoryWindow() (api.HistoryWindow, error) {
	return api.HistoryWindow{LogicalOwner: "remote", HistoryEpoch: "remote-epoch"}, nil
}
func (r *admissionRefusingRemote) Submit(api.Submission) (api.AcceptanceResult, error) {
	r.submits++
	return api.AcceptanceResult{Outcome: r.outcome, Reason: r.reason}, nil
}
func (*admissionRefusingRemote) Reconcile(api.RequestIdentity) (api.AcceptanceResult, error) {
	return api.AcceptanceResult{Outcome: api.AcceptanceOutcomeDefinitelyNotAccepted}, nil
}
func (*admissionRefusingRemote) CancelWork(api.RequestIdentity) (api.CancellationResult, error) {
	return api.CancellationResult{}, nil
}

// Stop after delegation and before the ordinary local adoption sweep. This is
// the process-exit window in which an in-memory admission refusal was lost.
type remoteAdmissionSweep struct {
	DelegatedExecution
}

func (remoteAdmissionSweep) Profile() string { return "remote-admission-sweep-v1" }
func (e remoteAdmissionSweep) Serve(ctx context.Context, store job.Store) error {
	r := download.NewRunner(store, "remote-admission-worker")
	defer r.Close()
	r.Delegators = download.NewDelegators(e.Delegators...)
	r.RecordCause = true
	r.SharedStore = true
	_, err := r.DelegateAll(ctx)
	return err
}

func (e remoteAdmissionSweep) ServeWithSubjectLookup(ctx context.Context, store job.Store, _ acceptanceprovider.SubjectLookup) error {
	return e.Serve(ctx, store)
}

func remoteAdmissionFixture(t *testing.T, outcome api.AcceptanceOutcome, reason string) (func() *acceptanceprovider.Provider, *admissionRefusingRemote, api.RequestIdentity) {
	t.Helper()
	remote := &admissionRefusingRemote{outcome: outcome, reason: reason}
	newDelegate := func() *RemoteJobDelegator {
		return &RemoteJobDelegator{Name: "remote", Transport: &api.RecoverableAcceptanceDispatcher{Handler: remote}}
	}
	root := t.TempDir()
	open := func() *acceptanceprovider.Provider {
		t.Helper()
		p, err := acceptanceprovider.OpenWithExecutor(root, "local", remoteAdmissionSweep{DelegatedExecution: DelegatedExecution{Delegators: []download.Delegator{newDelegate()}}})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := open()
	bound := p.Bind("local-caller")
	window, err := bound.GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	id := api.RequestIdentity{Key: "remote-refusal", HistoryEpoch: window.HistoryEpoch}
	contents := []byte("fixture")
	raw := request.Encode(&request.Request{
		Artifact: request.Artifact{Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(contents)), Size: int64(len(contents))},
		Sources:  []request.Source{{Scheme: "https", Locator: "https://example.invalid/model", Credential: "fixture"}},
	})
	accepted, err := bound.Submit(api.Submission{Identity: id, Kind: download.Kind, Spec: raw,
		RequiredGuarantees: []string{string(download.CapRecoverableSubmission), CredentialGuarantee}})
	if err != nil || accepted.Outcome != api.AcceptanceOutcomeAccepted {
		t.Fatalf("local admission: %+v %v", accepted, err)
	}
	return open, remote, id
}

func TestRemotePermanentAdmissionRefusalSurvivesRestart(t *testing.T) {
	open, remote, id := remoteAdmissionFixture(t, api.AcceptanceOutcomeInvalid, "credential:not_granted:fixture")
	p := open()
	if err := p.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if remote.submits != 1 {
		t.Fatalf("first delegation submitted %d times", remote.submits)
	}
	// New provider and new delegate object: no in-process refusal map survives.
	p = open()
	if err := p.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if remote.submits != 1 {
		t.Fatalf("restart resubmitted a permanent refusal %d times", remote.submits)
	}
	observed, err := p.BindOperations("local-caller").ObserveWork(id)
	if err != nil || observed.Snapshot == nil || observed.Snapshot.Failure == nil {
		t.Fatalf("durable refusal: %+v %v", observed, err)
	}
	if failure := observed.Snapshot.Failure; observed.Snapshot.State != api.WorkStateFailed ||
		failure.Cause != api.FailureCauseCredential || !strings.Contains(failure.Message, "credential:not_granted:fixture") {
		t.Fatalf("refusal changed across restart: %+v", observed.Snapshot)
	}
}

func TestRemoteUnavailableAdmissionRemainsRetryableAfterRestart(t *testing.T) {
	open, remote, id := remoteAdmissionFixture(t, api.AcceptanceOutcomeUnavailable, "credential:unavailable:fixture")
	p := open()
	if err := p.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	p = open()
	if err := p.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if remote.submits != 2 {
		t.Fatalf("unavailable admission made %d attempts; want one per sweep", remote.submits)
	}
	observed, err := p.BindOperations("local-caller").ObserveWork(id)
	if err != nil || observed.Snapshot == nil || observed.Snapshot.State == api.WorkStateFailed {
		t.Fatalf("retryable admission became terminal: %+v %v", observed, err)
	}
}
