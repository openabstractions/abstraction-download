package serve

import (
	"context"
	"errors"
	"slices"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
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

// ExecutionGuarantees offers recoverable submission, and credentials@1 while a
// configured delegate applies credentials by name on its own side [DL-K3].
func (e DelegatedExecution) ExecutionGuarantees() []string {
	guarantees := []string{string(download.CapRecoverableSubmission)}
	for _, d := range e.Delegators {
		if slices.Contains(d.Capabilities(), download.Capability(CredentialGuarantee)) {
			return append(guarantees, CredentialGuarantee)
		}
	}
	return guarantees
}

// RecoveryGuarantees are this profile's own; the embedded HTTP profile's are
// not recovered here.
func (e DelegatedExecution) RecoveryGuarantees() []string {
	return []string{string(download.CapRecoverableSubmission), CredentialGuarantee}
}

// CheckAdmission admits: a delegated request is decided by the receiving
// service's own admission.
func (DelegatedExecution) CheckAdmission(string, string, []byte, []string) (api.AcceptanceOutcome, string) {
	return 0, ""
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
