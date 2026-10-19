package serve

import (
	"encoding/json"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// JOB-A17: the download kind names the file its result reader reads, so the
// job service can link it: results/<operation id>, or a migrated job's
// contained sink. Unfinished work and an escaping sink name nothing.
func TestDownloadResultNameIsTheFileItsReaderReads(t *testing.T) {
	var _ acceptanceprovider.ResultNamer = HTTPExecution{}
	var _ acceptanceprovider.ResultNamer = LegacySinkExecution{}
	complete := &job.Record{ID: "operation123", Kind: download.Kind, State: job.StateComplete}
	if name, err := (HTTPExecution{}).OperationResultName(complete); err != nil || name != "results/operation123" {
		t.Fatalf("name %q %v", name, err)
	}
	for _, record := range []*job.Record{
		{ID: "operation123", Kind: download.Kind, State: job.StateRunning},
		{ID: "operation123", Kind: "other", State: job.StateComplete},
		{ID: "../x", Kind: download.Kind, State: job.StateComplete},
	} {
		if name, err := (HTTPExecution{}).OperationResultName(record); err == nil {
			t.Fatalf("%+v named %q", record, name)
		}
	}
	legacy := func(final string) []byte {
		b, _ := json.Marshal(download.Spec{Sources: []download.Source{{Scheme: "https", Locator: "https://example.test/x"}}, Sink: download.Sink{Final: final, Partial: final + ".part"}})
		return b
	}
	migrated := &job.Record{ID: "legacy-op", Kind: download.Kind, State: job.StateComplete, Spec: legacy("models/caller-chosen.bin")}
	if name, err := (LegacySinkExecution{}).OperationResultName(migrated); err != nil || name != "models/caller-chosen.bin" {
		t.Fatalf("legacy name %q %v", name, err)
	}
	migrated.Spec = legacy("../outside.bin")
	if name, err := (LegacySinkExecution{}).OperationResultName(migrated); err == nil {
		t.Fatalf("an escaping sink named %q", name)
	}
	fresh := &job.Record{ID: "legacy-op", Kind: download.Kind, State: job.StateComplete, Spec: legacy("results/legacy-op")}
	if name, err := (LegacySinkExecution{}).OperationResultName(fresh); err != nil || name != "results/legacy-op" {
		t.Fatalf("new work name %q %v", name, err)
	}
}
