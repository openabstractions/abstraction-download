package download

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// boundCredentials answers LookupSource from the source's own attributes and
// records every host it was asked about.
type boundCredentials struct {
	mu      sync.Mutex
	asked   []string
	outcome string
}

func (b *boundCredentials) Lookup(string, string) (map[string]string, bool) {
	panic("Lookup must not be called when LookupSource exists")
}

func (b *boundCredentials) LookupSource(src Source, host string) (map[string]string, error) {
	b.mu.Lock()
	b.asked = append(b.asked, src.Attrs[CredentialScopeAttr]+"@"+host)
	b.mu.Unlock()
	if b.outcome != "" {
		return nil, CredentialRefusal(src.Attrs[CredentialAttr], b.outcome)
	}
	return map[string]string{"X-Api-Key": "secret-for-" + src.Attrs[CredentialAttr]}, nil
}

func TestSourceCredentialsReplaceLookupAndTypeTheRefusal(t *testing.T) {
	creds := &boundCredentials{}
	src := Source{Scheme: "https", Locator: "https://huggingface.co/x", Attrs: map[string]string{CredentialAttr: "hf", CredentialScopeAttr: "owner-program@1:abc"}}
	got, err := headersFor(src, creds)
	if err != nil || got["X-Api-Key"] != "secret-for-hf" {
		t.Fatalf("headers %v: %v", got, err)
	}
	if len(creds.asked) != 1 || creds.asked[0] != "owner-program@1:abc@huggingface.co" {
		t.Fatalf("asked %v", creds.asked)
	}
	for outcome, permanent := range map[string]bool{"not_permitted": true, "revoked": true, "target_refused": true, "unavailable": false} {
		creds.outcome = outcome
		_, err := headersFor(src, creds)
		var typed *CredentialError
		if !errors.As(err, &typed) || typed.Outcome != outcome || Permanent(err) != permanent {
			t.Fatalf("%s: %v (permanent %v)", outcome, err, Permanent(err))
		}
		if err.Error() != "download: credential:"+outcome+":hf" || strings.Contains(err.Error(), "secret") {
			t.Fatalf("%s message: %q", outcome, err)
		}
		if CauseOf(err) != "credential" {
			t.Fatalf("%s cause %q", outcome, CauseOf(err))
		}
	}
}

// A header applied for one host does not follow a redirect to another host;
// a redirect within the same host keeps it.
func TestCallerHeadersStopAtACrossHostRedirect(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = r.Header.Get("X-Api-Key")
		mu.Unlock()
		w.Write([]byte("ok"))
	}))
	defer target.Close()
	// The origin is reached as 127.0.0.1 and redirects once to itself and once
	// to localhost, a different host name for the same listener.
	other := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/same":
			http.Redirect(w, r, target.URL+"/same-target", http.StatusFound)
		case "/cross":
			http.Redirect(w, r, other+"/cross-target", http.StatusFound)
		}
	}))
	defer origin.Close()
	for _, path := range []string{"/same", "/cross"} {
		hreq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, origin.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		supplied := map[string]string{"X-Api-Key": "applied-secret"}
		for k, v := range supplied {
			hreq.Header.Set(k, v)
		}
		resp, err := HTTP{}.do(hreq, nil, supplied)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	mu.Lock()
	defer mu.Unlock()
	if seen["/same-target"] != "applied-secret" {
		t.Fatalf("same-host redirect lost the header: %v", seen)
	}
	if got, ok := seen["/cross-target"]; !ok || got != "" {
		t.Fatalf("cross-host redirect carried the header: %v", seen)
	}
}
