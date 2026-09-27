package serve

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// DelegatedExecution supplies explicitly configured external adapters to the
// existing download runner. The service owns their configuration and lifetime.
// Adapter objects must be fresh for each Serve lifetime and safe for concurrent
// capability inspection. Input and result allocation follow HTTPExecution.
type DelegatedExecution struct {
	HTTPExecution
	Delegators []download.Delegator
}

func (DelegatedExecution) Profile() string { return "download-delegated-request-v1" }

// ExecutionGuarantees offers recoverable submission only while a configured
// delegate can reconcile it, and credentials@1 while a configured delegate
// applies credentials by name on its own side [DL-K3]. RecoveryGuarantees
// separately retain the promises already accepted across a restart.
func (e DelegatedExecution) ExecutionGuarantees() []string {
	var guarantees []string
	for _, d := range e.Delegators {
		capabilities := d.Capabilities()
		if _, canLocate := d.(download.Locator); canLocate && slices.Contains(capabilities, download.CapRecoverableSubmission) && !slices.Contains(guarantees, string(download.CapRecoverableSubmission)) {
			guarantees = append(guarantees, string(download.CapRecoverableSubmission))
		}
		if slices.Contains(capabilities, download.Capability(CredentialGuarantee)) && !slices.Contains(guarantees, CredentialGuarantee) {
			guarantees = append(guarantees, CredentialGuarantee)
		}
	}
	return guarantees
}

// RecoveryGuarantees are this profile's own; the embedded HTTP profile's are
// not recovered here.
func (e DelegatedExecution) RecoveryGuarantees() []string {
	return []string{string(download.CapRecoverableSubmission), CredentialGuarantee}
}

// CheckAdmission checks that one configured adapter can take the requested
// source and every execution guarantee together. The receiving service still
// decides its own credential and authorization admission after handoff.
func (e DelegatedExecution) CheckAdmission(scope, kind string, raw []byte, required []string) (api.AcceptanceOutcome, string) {
	if len(required) == 0 {
		return 0, "" // Anonymous work can use the local HTTP executor.
	}
	work, _, err := e.PrepareScoped(scope, "admission", kind, raw, required)
	if err != nil {
		return 0, "" // Preparation reports malformed or unsupported input.
	}
	var spec download.Spec
	if err := json.Unmarshal(work, &spec); err != nil {
		return 0, "" // Preparation reports malformed input.
	}
	delegates := download.NewDelegators(e.Delegators...)
	for _, source := range spec.Sources {
		one := spec
		one.Sources = []download.Source{source}
		if _, ok := delegates.ForSpec(one, source, required); ok {
			return 0, ""
		}
	}
	return api.AcceptanceOutcomeInvalid, "no configured delegate can serve the requested source and guarantees"
}

// CheckSubjectAdmission keeps adapter eligibility with this profile. The
// receiving delegate decides credential authority after handoff.
func (e DelegatedExecution) CheckSubjectAdmission(binding acceptanceprovider.Binding, kind string, raw []byte, required []string) (api.AcceptanceOutcome, string) {
	return e.CheckAdmission(binding.Scope, kind, raw, required)
}

// Preparation is deterministic and performs no external submission. Adapter
// eligibility is checked by Runner before Start;
// recovery can reconstruct accepted work while its adapter is unavailable.
func (e DelegatedExecution) PrepareWithGuarantees(id, kind string, raw []byte, required []string) ([]byte, []string, error) {
	for _, guarantee := range required {
		if guarantee != string(download.CapRecoverableSubmission) {
			return nil, nil, errors.New("unsupported execution guarantee")
		}
	}
	work, err := e.Prepare(id, kind, raw)
	if err != nil {
		return nil, nil, err
	}
	return work, append([]string(nil), required...), nil
}

// PrepareScoped keeps this profile's own preparation. A request naming a
// credential records the name beside the source, as HTTP execution does; only
// a delegate claiming credentials@1 takes it, and it forwards the name alone
// [DL-K3]. An anonymous request prepares exactly as PrepareWithGuarantees.
func (e DelegatedExecution) PrepareScoped(scope, id, kind string, raw []byte, required []string) ([]byte, []string, error) {
	if !requiresCredentials(required) {
		return e.PrepareWithGuarantees(id, kind, raw, required)
	}
	for _, guarantee := range required {
		if guarantee != string(download.CapRecoverableSubmission) && guarantee != CredentialGuarantee {
			return nil, nil, errors.New("unsupported execution guarantee")
		}
	}
	work, _, err := prepareRequest(scope, id, kind, raw, true, false)
	if err != nil {
		return nil, nil, err
	}
	return work, append([]string(nil), required...), nil
}

func (e DelegatedExecution) Serve(ctx context.Context, store job.Store) error {
	return serveExecution(ctx, store, e.OnError, e.Delegators)
}

// ServeWithSubjectLookup retains this profile's delegates when the job
// provider supplies private subject evidence for recovered work.
func (e DelegatedExecution) ServeWithSubjectLookup(ctx context.Context, store job.Store, lookup acceptanceprovider.SubjectLookup) error {
	return serveExecutionWithLookup(ctx, store, e.OnError, e.Delegators, nil, nil, nil, lookup)
}
