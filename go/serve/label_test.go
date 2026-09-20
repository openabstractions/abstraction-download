package serve

import (
	"strings"
	"testing"
	"unicode/utf8"

	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

func requestFor(locators ...string) []byte {
	r := request.Request{}
	for _, locator := range locators {
		scheme, _, _ := strings.Cut(locator, ":")
		r.Sources = append(r.Sources, request.Source{Scheme: scheme, Locator: locator})
	}
	return request.Encode(&r)
}

func TestDerivedDownloadLabel(t *testing.T) {
	for _, c := range []struct{ locator, want string }{
		{"https://user:tok@host:8443/a/b/model.bin?X-Amz-Signature=secret#f", "host · model.bin"},
		{"https://huggingface.co/org/repo/resolve/main/model.safetensors", "huggingface.co · model.safetensors"},
		{"https://host/a/b/", "host · b"},
		{"https://host/a/b//", "host · b"},
		{"https://host", "host"},
		{"https://host/", "host"},
		{"https://host?token=secret", "host"},
		{"http://[::1]:8080/x", "::1 · x"},
		{"https://host/models/my%20model.gguf", "host · my model.gguf"},
		{"https://host/a%0Ab", "host · a%0Ab"},
		{"https://host/%E2%80%A8", "host · %E2%80%A8"},
		{"/relative/path", ""},
		{"not a url\x00", ""},
	} {
		got := HTTPExecution{}.DeriveLabel("download", requestFor(c.locator))
		if got != c.want {
			t.Errorf("%q derived %q, want %q", c.locator, got, c.want)
		}
		for _, secret := range []string{"user", "tok@", "8443", "Signature", "secret", "#f"} {
			if strings.Contains(got, secret) {
				t.Errorf("%q derived %q carrying %q", c.locator, got, secret)
			}
		}
	}
	if got := (HTTPExecution{}).DeriveLabel("download", requestFor("https://first/one", "https://second/two")); got != "first · one" {
		t.Errorf("first source: %q", got)
	}
	for kind, spec := range map[string][]byte{"other": requestFor("https://host/x"), "download": []byte("{"), "": requestFor()} {
		if kind == "" {
			kind = "download"
		}
		if got := (HTTPExecution{}).DeriveLabel(kind, spec); got != "" {
			t.Errorf("kind %q spec %q derived %q", kind, spec, got)
		}
	}
	long := HTTPExecution{}.DeriveLabel("download", requestFor("https://host/"+strings.Repeat("é", 300)))
	if normalized, err := api.NormalizeLabel(long); err != nil || normalized != long || !utf8.ValidString(long) || !strings.HasPrefix(long, "host · é") {
		t.Errorf("long label %q: %v", long, err)
	}
}

// TestProviderStoresDerivedDownloadLabel runs derivation through the job
// provider: an unlabelled download is stored with the derived label, marked
// derived, and a caller label wins [JOB-A12].
func TestProviderStoresDerivedDownloadLabel(t *testing.T) {
	root := t.TempDir()
	p, err := acceptanceprovider.OpenWithExecutor(root, "label-owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	observe := func(s api.Submission) *api.OperationSnapshot {
		t.Helper()
		if v, err := p.Bind("alice").Submit(s); err != nil || v.Outcome.String() != "accepted" {
			t.Fatalf("submit: %+v %v", v, err)
		}
		o, err := p.BindOperations("alice").ObserveWork(s.Identity)
		if err != nil || o.Snapshot == nil {
			t.Fatalf("observe: %+v %v", o, err)
		}
		return o.Snapshot
	}
	spec := requestFor("https://user:tok@huggingface.co/org/repo/resolve/main/model.safetensors?download=true")
	derived := api.Submission{Identity: api.RequestIdentity{Key: "derived", HistoryEpoch: window.HistoryEpoch}, Kind: "download", Spec: requestFor("https://huggingface.co/org/repo/resolve/main/model.safetensors?download=true")}
	if s := observe(derived); s.Label != "huggingface.co · model.safetensors" || !s.LabelDerived {
		t.Fatalf("derived snapshot: %+v", s)
	}
	caller := api.Submission{Identity: api.RequestIdentity{Key: "caller", HistoryEpoch: window.HistoryEpoch}, Kind: "download", Spec: derived.Spec, Label: "org/repo@main · model.safetensors"}
	if s := observe(caller); s.Label != "org/repo@main · model.safetensors" || s.LabelDerived {
		t.Fatalf("caller snapshot: %+v", s)
	}
	// A source with credentials is refused by this profile; nothing labelled is kept.
	refused := api.Submission{Identity: api.RequestIdentity{Key: "refused", HistoryEpoch: window.HistoryEpoch}, Kind: "download", Spec: spec}
	if v, err := p.Bind("alice").Submit(refused); err != nil || v.Outcome.String() == "accepted" {
		t.Fatalf("credentialed source accepted: %+v %v", v, err)
	}
	p, err = acceptanceprovider.OpenWithExecutor(root, "label-owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	o, err := p.BindOperations("alice").ObserveWork(derived.Identity)
	if err != nil || o.Snapshot == nil || o.Snapshot.Label != "huggingface.co · model.safetensors" || !o.Snapshot.LabelDerived {
		t.Fatalf("restart: %+v %v", o, err)
	}
}
