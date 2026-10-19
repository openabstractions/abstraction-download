package download

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// clearAndSecure serves https on example.com (the name httptest's certificate
// carries) and http on every name at port 80, through one client that dials
// both on loopback. The names are not loopback, so the credential rule sees
// the request a remote host would receive.
type clearAndSecure struct {
	mu     sync.Mutex
	seen   map[string]string
	secure *httptest.Server
	clear  *httptest.Server
	client *http.Client
}

func newClearAndSecure(t *testing.T, secure http.HandlerFunc) *clearAndSecure {
	t.Helper()
	s := &clearAndSecure{seen: map[string]string{}}
	record := func(scheme string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			s.mu.Lock()
			s.seen[scheme+"://"+r.Host+r.URL.Path] = r.Header.Get("X-Api-Key")
			s.mu.Unlock()
		}
	}
	s.secure = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("https")(w, r)
		secure(w, r)
	}))
	t.Cleanup(s.secure.Close)
	s.clear = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("http")(w, r)
		w.Write([]byte("clear bytes"))
	}))
	t.Cleanup(s.clear.Close)
	transport := s.secure.Client().Transport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		target := s.clear.Listener.Addr().String()
		if strings.HasSuffix(addr, ":443") {
			target = s.secure.Listener.Addr().String()
		}
		return (&net.Dialer{}).DialContext(ctx, network, target)
	}
	s.client = &http.Client{Transport: transport}
	return s
}

func (s *clearAndSecure) send(creds Credentials, locator string) error {
	src := Source{Scheme: strings.SplitN(locator, ":", 2)[0], Locator: locator,
		Attrs: map[string]string{CredentialAttr: "hf", CredentialScopeAttr: "scope"}}
	headers, err := headersFor(src, creds)
	if err != nil {
		return err
	}
	ctx := withCredentialHops(context.Background(), src, creds, headers)
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, locator, nil)
	if err != nil {
		return err
	}
	for k, v := range headers {
		hreq.Header.Set(k, v)
	}
	resp, err := HTTP{Client: s.client}.do(hreq, nil, headers)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func (s *clearAndSecure) got(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.seen[key]
	return v, ok
}

func insecureRefusal(t *testing.T, err error) {
	t.Helper()
	var refusal *CredentialError
	if !errors.As(err, &refusal) || refusal.Outcome != OutcomeInsecureTransport || !Permanent(err) || CauseOf(err) != "credential" {
		t.Fatalf("want a permanent credential:insecure_transport refusal, got %v", err)
	}
}

// An http source naming a credential is refused before any request is sent
// and before the applier is asked: no target list makes clear text safe [DL-K5].
func TestCredentialRefusedForAnHTTPSource(t *testing.T) {
	s := newClearAndSecure(t, func(http.ResponseWriter, *http.Request) {})
	creds := &hostCredentials{targets: map[string]bool{"example.com": true}}
	insecureRefusal(t, s.send(creds, "http://example.com/model"))
	if len(creds.asked) != 0 {
		t.Fatalf("the applier was asked for %v", creds.asked)
	}
	if _, reached := s.got("http://example.com/model"); reached {
		t.Fatal("an http source with a credential was requested")
	}
	// The same source over https carries the credential.
	if err := s.send(creds, "https://example.com/model"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.got("https://example.com/model"); got == "" {
		t.Fatal("the https source lost its credential")
	}
}

// A same-host redirect from https to http goes without the credential, while
// the credential's list names that host [DL-K5, DL-K2].
func TestCredentialDroppedOnADowngradeRedirect(t *testing.T) {
	s := newClearAndSecure(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.com/clear", http.StatusFound)
	})
	creds := &hostCredentials{targets: map[string]bool{"example.com": true}}
	if err := s.send(creds, "https://example.com/model"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.got("https://example.com/model"); got == "" {
		t.Fatal("the https request lost its credential")
	}
	got, reached := s.got("http://example.com/clear")
	if !reached {
		t.Fatal("the redirect was not followed")
	}
	if got != "" {
		t.Fatalf("the http hop carried the credential in clear: %q", got)
	}
	if strings.Join(creds.asked, ",") != "example.com" {
		t.Fatalf("the applier was asked for the http hop: %v", creds.asked)
	}
}

// A redirect to another listed host over http goes without the credential as
// well, while an https hop to a listed host carries its own application.
func TestCredentialDroppedOnACrossHostClearRedirect(t *testing.T) {
	s := newClearAndSecure(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://mirror.example/clear", http.StatusFound)
	})
	creds := &hostCredentials{targets: map[string]bool{"example.com": true, "mirror.example": true}}
	if err := s.send(creds, "https://example.com/model"); err != nil {
		t.Fatal(err)
	}
	got, reached := s.got("http://mirror.example/clear")
	if !reached {
		t.Fatal("the redirect was not followed")
	}
	if got != "" {
		t.Fatalf("the cross-host http hop carried the credential in clear: %q", got)
	}
	if strings.Join(creds.asked, ",") != "example.com" {
		t.Fatalf("the applier was asked for the http hop: %v", creds.asked)
	}
}

// Only https carries a credential, loopback included [DL-K5].
func TestCredentialTransport(t *testing.T) {
	for raw, want := range map[string]bool{
		"https://example.com/a":   true,
		"HTTPS://example.com/a":   true,
		"http://127.0.0.1:8080/a": false,
		"http://localhost/a":      false,
		"http://example.com/a":    false,
		"ftp://example.com/a":     false,
		"file:///c:/a":            false,
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := CredentialTransport(u); got != want {
			t.Errorf("%s: %v, want %v", raw, got, want)
		}
	}
	if CredentialTransport(nil) {
		t.Error("a nil URL may carry a credential")
	}
}

// One origin is one scheme, host and port, with the default port written out.
func TestCredentialOrigin(t *testing.T) {
	same := [][2]string{
		{"https://hf.co/a", "https://HF.co:443/b"},
		{"https://hf.co./a", "https://hf.co/b?x"},
	}
	for _, pair := range same {
		a, _ := url.Parse(pair[0])
		b, _ := url.Parse(pair[1])
		if origin(a) != origin(b) {
			t.Errorf("%s and %s are one origin", pair[0], pair[1])
		}
	}
	other := [][2]string{
		{"https://hf.co/a", "http://hf.co/a"},
		{"https://hf.co/a", "https://hf.co:8443/a"},
		{"https://hf.co/a", "https://cdn.hf.co/a"},
	}
	for _, pair := range other {
		a, _ := url.Parse(pair[0])
		b, _ := url.Parse(pair[1])
		if origin(a) == origin(b) {
			t.Errorf("%s and %s are two origins", pair[0], pair[1])
		}
	}
}
