package download

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	// refuseAfter, when set, refuses revoked once this many applications
	// have been made.
	refuseAfter int
}

func (h *hostCredentials) Lookup(string, string) (map[string]string, bool) {
	panic("LookupSource exists")
}

func (h *hostCredentials) LookupSource(src Source, host string) (map[string]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.asked = append(h.asked, host)
	if h.refuseAfter > 0 && h.n >= h.refuseAfter {
		return nil, CredentialRefusal(src.Attrs[CredentialAttr], "revoked")
	}
	if outcome := h.refuse[host]; outcome != "" {
		return nil, CredentialRefusal(src.Attrs[CredentialAttr], outcome)
	}
	if !h.targets[host] {
		return nil, CredentialRefusal(src.Attrs[CredentialAttr], "target_refused")
	}
	h.n++
	return map[string]string{"X-Api-Key": fmt.Sprintf("applied-%d-for-%s", h.n, host)}, nil
}

// Every redirect to the same origin applies the credential again [DL-K2]; a
// redirect to another origin goes without it, whatever the credential's list
// says, as curl without --location-trusted and browsers do [DL-K5]. A refusal
// other than target_refused on a same-origin hop ends the request.
func TestRedirectAppliesTheCredentialPerHost(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}
	record := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = r.Header.Get("X-Api-Key") + "|" + r.Header.Get("X-Record")
		mu.Unlock()
		w.Write([]byte("ok"))
	}
	target := newTrustedTLSServer(t, http.HandlerFunc(record))
	// example.com is a name httptest's certificate carries; the client dials
	// it on loopback, so it is another host on the target's port.
	otherHost := strings.Replace(target.URL, "127.0.0.1", "example.com", 1)
	origin := newTrustedTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/same":
			http.Redirect(w, r, "/same-target", http.StatusFound)
		case "/other-port":
			http.Redirect(w, r, target.URL+"/other-port-target", http.StatusFound)
		case "/other-host":
			http.Redirect(w, r, otherHost+"/other-host-target", http.StatusFound)
		default:
			record(w, r)
		}
	}))

	transport := http.DefaultClient.Transport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, strings.Replace(addr, "example.com", "127.0.0.1", 1))
	}
	http.DefaultClient = &http.Client{Transport: transport}

	send := func(creds *hostCredentials, path string) error {
		src := Source{Scheme: "https", Locator: origin.URL + path, Headers: map[string]string{"X-Record": "record-header"},
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

	listsBoth := &hostCredentials{targets: map[string]bool{"127.0.0.1": true, "example.com": true}}
	for _, path := range []string{"/same", "/other-port", "/other-host"} {
		if err := send(listsBoth, path); err != nil {
			t.Fatal(path, err)
		}
	}
	mu.Lock()
	if got := seen["/same-target"]; got != "applied-2-for-127.0.0.1|record-header" {
		t.Fatalf("same-origin redirect reused or lost the credential: %q", got)
	}
	if got := seen["/other-port-target"]; got != "|record-header" {
		t.Fatalf("a redirect to another port received %q", got)
	}
	if got := seen["/other-host-target"]; got != "|" {
		t.Fatalf("a redirect to another listed host received %q", got)
	}
	mu.Unlock()
	if strings.Join(listsBoth.asked, ",") != "127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1" {
		t.Fatalf("applier asked for %v", listsBoth.asked)
	}

	revoked := &hostCredentials{targets: map[string]bool{"127.0.0.1": true}}
	revoked.refuseAfter = 1
	delete(seen, "/same-target")
	err := send(revoked, "/same")
	var refusal *CredentialError
	if !errors.As(err, &refusal) || refusal.Outcome != "revoked" || !Permanent(err) || CauseOf(err) != "credential" {
		t.Fatalf("a refused redirect application: %v", err)
	}
	if _, reached := seen["/same-target"]; reached {
		t.Fatal("the redirect was sent after its credential was refused")
	}
}
