package serve

import (
	"context"
	"errors"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
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

func (e DelegatedExecution) ExecutionGuarantees() []string {
	return []string{string(download.CapRecoverableSubmission)}
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

func (e DelegatedExecution) Serve(ctx context.Context, store job.Store) error {
	return serveExecution(ctx, store, e.OnError, e.Delegators)
}
