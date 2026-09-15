package serve

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	job "github.com/openabstractions/abstraction-job/go"
)

func TestLegacySinkProfileKeepsNewWorkAndContainsLegacySinks(t *testing.T) {
	e := LegacySinkExecution{}
	if e.Profile() == (HTTPExecution{}).Profile() {
		t.Fatal("legacy sink acceptance needs a separately versioned profile")
	}
	payload := request.Encode(&request.Request{Artifact: request.Artifact{Size: 3}, Sources: []request.Source{{Scheme: "https", Locator: "https://example.test/x"}}})
	fresh, err := e.Prepare("op-1", download.Kind, payload)
	want, _ := (HTTPExecution{}).Prepare("op-1", download.Kind, payload)
	if err != nil || string(fresh) != string(want) {
		t.Fatalf("new submission preparation changed: %s %v", fresh, err)
	}
	legacy := func(final string) []byte {
		b, _ := json.Marshal(download.Spec{Sources: []download.Source{{Scheme: "https", Locator: "https://example.test/x"}}, Sink: download.Sink{Final: final, Partial: final + ".part"}})
		return b
	}
	if work, requires, err := e.PrepareLegacy("legacy-op", download.Kind, legacy("models/caller-chosen.bin"), nil); err != nil || requires != nil || !json.Valid(work) {
		t.Fatal("relative legacy sink refused", err)
	}
	absolute := filepath.Join(t.TempDir(), "outside.bin")
	for name, raw := range map[string][]byte{
		"absolute":  legacy(absolute),
		"escaping":  legacy("../outside.bin"),
		"reserved":  legacy("jobs/x.json"),
		"unknown":   []byte(`{"sources":[{"scheme":"https","locator":"https://example.test/x"}],"sink":{"final":"models/x"},"surprise":1}`),
		"no-source": legacy("models/x"),
	} {
		if name == "no-source" {
			raw = []byte(`{"sink":{"final":"models/x"}}`)
		}
		if _, _, err := e.PrepareLegacy("legacy-op", download.Kind, raw, nil); err == nil {
			t.Fatalf("%s legacy sink accepted", name)
		}
	}
	if _, _, err := e.PrepareLegacy("legacy-op", download.Kind, legacy("models/x"), []string{"extra@1"}); err == nil {
		t.Fatal("legacy preparation accepted execution guarantees")
	}
	if _, _, err := e.PrepareLegacy("legacy-op", "other", legacy("models/x"), nil); err == nil {
		t.Fatal("legacy preparation accepted another kind")
	}

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "models"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "caller-chosen.bin"), []byte("legacy bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	record := &job.Record{ID: "legacy-op", Kind: download.Kind, State: job.StateComplete, Spec: legacy("models/caller-chosen.bin")}
	data, total, err := e.ReadOperationResult(root, record, 0, 64)
	if err != nil || string(data) != "legacy bytes" || total != 12 {
		t.Fatalf("legacy result %q %d %v", data, total, err)
	}
	record.Spec = legacy("../outside.bin")
	if _, _, err := e.ReadOperationResult(root, record, 0, 64); err == nil {
		t.Fatal("escaping result read")
	}
	if _, _, err := (HTTPExecution{}).ReadOperationResult(root, &job.Record{ID: "legacy-op", Kind: download.Kind, State: job.StateComplete, Spec: legacy("models/caller-chosen.bin")}, 0, 64); err == nil {
		t.Fatal("unmigrated HTTP profile read a caller-chosen sink")
	}
}
