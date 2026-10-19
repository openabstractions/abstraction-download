package serve

import (
	"encoding/json"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

var _ acceptanceprovider.RightsRefusalReporter = HTTPExecution{}

// abstraction-job JOB-A19: a failed download whose credential use a rights
// decision refused (not_permitted) is a refusal by rights, so the same
// request submitted again is decided again. Every other failure, a credential
// the applier could not reach or lost included, is a failure of the work.
func TestOnlyARefusedCredentialUseIsARefusalByRights(t *testing.T) {
	for _, c := range []struct {
		name, failure, cause string
		state                job.State
		want                 bool
	}{
		{"not permitted", "download: credential:not_permitted:hf", "credential", job.StateFailed, true},
		{"not permitted, wrapped", "fetch https://hf.co/x: download: credential:not_permitted:hf", "credential", job.StateFailed, true},
		{"unavailable", "download: credential:unavailable:hf", "credential", job.StateFailed, false},
		{"lost", "download: credential:lost:hf", "credential", job.StateFailed, false},
		{"not found", "http 404", "not_found", job.StateFailed, false},
		{"still running", "download: credential:not_permitted:hf", "credential", job.StatePending, false},
		// The words elsewhere in the error, a source's own text, are not the
		// applier's.
		{"source text first", "source said credential:not_permitted:hf; download: credential:unavailable:hf", "credential", job.StateFailed, false},
		{"source text only", "http 403: credential:not_permitted:hf", "credential", job.StateFailed, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			first, _ := json.Marshal(map[string]any{"error": c.failure, "permanent": true})
			second, _ := json.Marshal(map[string]any{"error": c.failure, "permanent": true, "cause": c.cause})
			record := &job.Record{State: c.state, Extensions: map[string]json.RawMessage{
				download.FailureExtension: first, download.FailureCauseExtension: second}}
			if got := (HTTPExecution{}).RefusedByRights(record); got != c.want {
				t.Fatalf("read %v, want %v", got, c.want)
			}
		})
	}
}
