package download

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// End to end, against a real listener: a record that names credential "hf" and
// points its source at a host the owner never bound must reach that host with no
// Authorization header. This is the confused deputy on the wire — the listener
// stands in for the attacker's server on the LAN — and it must receive nothing.
//
// The canary is the tree's convention: a token that must never appear on disk or
// on a wire it was not bound to.
func TestTokenNeverReachesAnUnboundHost(t *testing.T) {
	const canary = "hf_thisMustNeverAppearOnDisk_EXAMPLE"

	var mu sync.Mutex
	var authSeen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authSeen = append(authSeen, r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not the artifact"))
	}))
	defer srv.Close()

	// The token is held by this machine and bound to Hugging Face — never to the
	// listener's host. A record chose the listener; the binding is what refuses it.
	t.Setenv("ABSTRACTION_CRED_HF", canary)
	t.Setenv("ABSTRACTION_CRED_HF_HOSTS", "huggingface.co")

	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id, err := Submit(store, Spec{
		Artifact: Artifact{Size: 16},
		Sources: []Source{{
			Scheme:  "http",
			Locator: srv.URL + "/models/x.gguf",
			Attrs:   map[string]string{CredentialAttr: "hf"},
		}},
		Sink: Sink{Final: "models/x.gguf"},
	})
	if err != nil {
		t.Fatal(err)
	}

	r := NewRunner(store, "victim") // EnvCredentials by default
	// The transfer must not succeed either way; what is on trial is the header.
	_ = r.Run(context.Background(), id)

	mu.Lock()
	defer mu.Unlock()
	for _, a := range authSeen {
		if strings.Contains(a, canary) || strings.HasPrefix(a, "Bearer") {
			t.Fatalf("the owner's token reached a host it was never bound to: %q", a)
		}
	}
}

// The bound host still receives the token — the binding must not break the
// download it protects. Here the listener IS the bound host (127.0.0.1).
func TestTokenReachesTheBoundHost(t *testing.T) {
	const canary = "hf_thisMustNeverAppearOnDisk_EXAMPLE"

	var mu sync.Mutex
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = r.Header.Get("Authorization")
		mu.Unlock()
		http.Error(w, "gated", http.StatusUnauthorized)
	}))
	defer srv.Close()

	t.Setenv("ABSTRACTION_CRED_HF", canary)
	t.Setenv("ABSTRACTION_CRED_HF_HOSTS", "127.0.0.1")

	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id, err := Submit(store, Spec{
		Artifact: Artifact{Size: 16},
		Sources: []Source{{
			Scheme:  "http",
			Locator: srv.URL + "/x",
			Attrs:   map[string]string{CredentialAttr: "hf"},
		}},
		Sink: Sink{Final: filepath.Join(root, "x.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = NewRunner(store, "victim").Run(context.Background(), id)

	mu.Lock()
	defer mu.Unlock()
	if got != "Bearer "+canary {
		t.Fatalf("the bound host did not receive the credential: %q", got)
	}
}
