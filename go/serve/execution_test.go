package serve

import (
	"bytes"
	"encoding/json"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
)

func TestHTTPPreparationOwnsDestinations(t *testing.T) {
	e := HTTPExecution{}
	raw := []byte(`{"artifact":{},"sources":[{"scheme":"https","locator":"https://example.com/file"}]}`)
	prepared, err := e.Prepare("operation123", "download", raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := e.Prepare("operation123", "download", raw)
	if err != nil || !bytes.Equal(prepared, again) {
		t.Fatal("unstable preparation")
	}
	var spec download.Spec
	if err := json.Unmarshal(prepared, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Sink.Partial != "work/operation123" || spec.Sink.Final != "results/operation123" {
		t.Fatalf("destinations: %+v", spec.Sink)
	}
	if _, err := e.Prepare("../outside", "download", raw); err == nil {
		t.Fatal("unsafe identity accepted")
	}
	if _, err := e.Prepare("id", "other", raw); err == nil {
		t.Fatal("unsupported kind accepted")
	}
}

func TestHTTPPreparationRefusesUnsupportedRequests(t *testing.T) {
	for _, raw := range []string{
		`{"artifact":{},"sources":[],"sink":{"final":"elsewhere"}}`,
		`{"artifact":{},"sources":[]}`,
		`{"artifact":{"size":-1},"sources":[{"scheme":"https","locator":"https://example.com"}]}`,
		`{"artifact":{"digest":"sha256:bad"},"sources":[{"scheme":"https","locator":"https://example.com"}]}`,
		`{"artifact":{},"sources":[{"scheme":"https","locator":"https://user:secret@example.com"}]}`,
		`{"artifact":{},"sources":[{"scheme":"file","locator":"file:///private"}]}`,
		`{"artifact":{},"sources":[{"scheme":"http","locator":"https://example.com"}]}`,
	} {
		if _, err := (HTTPExecution{}).Prepare("id", "download", []byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
