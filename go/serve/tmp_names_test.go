package serve

import (
	"encoding/json"
	"errors"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

func TestDownloadTmpNamesOnlyExactServiceAllocation(t *testing.T) {
	for _, names := range []download.Sink{{Final: "results/fixture", Partial: "work/fixture"}, {Final: "caller/model", Partial: "caller/model.part"}, {Final: "/caller/model", Partial: "/caller/model.part"}, {Final: "results/fixture", Partial: "results/fixture.part"}} {
		raw, _ := json.Marshal(download.Spec{Sources: []download.Source{{Scheme: "https", Locator: "https://example.test/file"}}, Sink: names})
		r := &job.Record{ID: "fixture", Kind: download.Kind, State: job.StateComplete, Spec: raw}
		result, partial, err := (HTTPExecution{}).OperationTmpNames(r)
		if names.Partial == "work/fixture" {
			if err != nil || result != names.Final || partial != names.Partial {
				t.Fatal(result, partial, err)
			}
		} else if !errors.Is(err, acceptanceprovider.ErrResultNotNamed) {
			t.Fatal("caller destination named", names, err)
		}
		if _, _, err := (DelegatedExecution{}).OperationTmpNames(r); !errors.Is(err, acceptanceprovider.ErrResultNotNamed) {
			t.Fatal("delegated/NAS allocation named", err)
		}
	}
}
