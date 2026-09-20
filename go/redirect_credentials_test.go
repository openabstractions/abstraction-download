package download

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// hostCredentials applies a credential only to the hosts it lists, with a new
// value on every application, and refuses with a fixed outcome for a host it
// is told to refuse.
type hostCredentials struct {
	mu      sync.Mutex
	targets map[string]bool
	refuse  map[string]string
	asked   []string
	n       int
}

func (h *hostCredentials) Lookup(string, string) (map[string]string, bool) {
	panic("LookupSource exists")
}

func (h *hostCredentials) LookupSource(src Source, host string) (map[string]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.asked = append(h.asked, host)
	if outcome := h.refuse[host]; outcome != "" {
		return nil, CredentialRefusal(src.Attrs[CredentialAttr], outcome)
	}
	if !h.targets[host] {
		return nil, CredentialRefusal(src.Attrs[CredentialAttr], "target_refused")
	}
	h.n++
	return map[string]string{"X-Api-Key": fmt.Sprintf("applied-%d-for-%s", h.n, host)}, nil
}

// Every redirect applies the credential again for its target [DL-K2]: a
// same-host redirect carries a fresh application, a host the credential does
// not list gets no credential header, a listed other host gets its own, and a
// refusal other than target_refused ends the request.
func TestRedirectAppliesTheCredentialPerHost(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = r.Header.Get("X-Api-Key") + "|" + r.Header.Get("X-Record")
		mu.Unlock()
		w.Write([]byte("ok"))
	}))
	defer target.Close()
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

	send := func(creds *hostCredentials, path string) error {
		src := Source{Scheme: "http", Locator: origin.URL + path, Headers: map[string]string{"X-Record": "record-header"},
			Attrs: map[string]string{CredentialAttr: "hf", CredentialScopeAttr: "scope"}}
		headers, err := headersFor(src, creds)
		if err != nil {
			return err
		}
		ctx := withCredentialHops(context.Background(), src, creds, headers)
		hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, src.Locator, nil)
		if err != nil {
			return err
		}
		for k, v := range headers {
			hreq.Header.Set(k, v)
		}
		resp, err := HTTP{}.do(hreq, nil, headers)
		if err != nil {
			return err
		}
		return resp.Body.Close()
	}

	listsOnlyOrigin := &hostCredentials{targets: map[string]bool{"127.0.0.1": true}}
	for _, path := range []string{"/same", "/cross"} {
		if err := send(listsOnlyOrigin, path); err != nil {
			t.Fatal(path, err)
		}
	}
	mu.Lock()
	if got := seen["/same-target"]; got != "applied-2-for-127.0.0.1|record-header" {
		t.Fatalf("same-host redirect reused or lost the credential: %q", got)
	}
	if got := seen["/cross-target"]; got != "|" {
		t.Fatalf("an unlisted redirect host received %q", got)
	}
	mu.Unlock()
	if strings.Join(listsOnlyOrigin.asked, ",") != "127.0.0.1,127.0.0.1,127.0.0.1,localhost" {
		t.Fatalf("applier asked for %v", listsOnlyOrigin.asked)
	}

	listsBoth := &hostCredentials{targets: map[string]bool{"127.0.0.1": true, "localhost": true}}
	if err := send(listsBoth, "/cross"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if got := seen["/cross-target"]; got != "applied-2-for-localhost|" {
		t.Fatalf("a listed redirect host received %q", got)
	}
	mu.Unlock()

	revoked := &hostCredentials{targets: map[string]bool{"127.0.0.1": true, "localhost": true}, refuse: map[string]string{"localhost": "revoked"}}
	delete(seen, "/cross-target")
	err := send(revoked, "/cross")
	var refusal *CredentialError
	if !errors.As(err, &refusal) || refusal.Outcome != "revoked" || !Permanent(err) || CauseOf(err) != "credential" {
		t.Fatalf("a refused redirect application: %v", err)
	}
	if _, reached := seen["/cross-target"]; reached {
		t.Fatal("the redirect was sent after its credential was refused")
	}
}
