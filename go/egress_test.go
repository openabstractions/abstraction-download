package download

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Every address class rule 4 names is refused in each of its forms, the
// local network is refused unless the person allowed the host by name, and a
// public address passes [DL-G4, DL-G5].
func TestEgressAddressRules(t *testing.T) {
	e := Egress{AllowedLocal: func(host string) bool { return host == "nas.home" }}
	for addr, want := range map[string]string{
		"127.0.0.1":              reasonLoopback,
		"127.8.9.10":             reasonLoopback,
		"::1":                    reasonLoopback,
		"::ffff:127.0.0.1":       reasonLoopback,
		"::127.0.0.1":            reasonLoopback,
		"169.254.169.254":        reasonLinkLocal,
		"::ffff:169.254.169.254": reasonLinkLocal,
		"fe80::1":                reasonLinkLocal,
		"0.0.0.0":                reasonUnspecified,
		"0.1.2.3":                reasonUnspecified,
		"::":                     reasonUnspecified,
		"::ffff:0.0.0.0":         reasonUnspecified,
		"224.0.0.1":              reasonMulticast,
		"239.255.255.250":        reasonMulticast,
		"ff02::1":                reasonMulticast,
		"255.255.255.255":        reasonBroadcast,
		"::ffff:255.255.255.255": reasonBroadcast,
		"10.0.0.5":               reasonLocal,
		"172.16.1.1":             reasonLocal,
		"172.31.255.255":         reasonLocal,
		"192.168.1.10":           reasonLocal,
		"100.64.0.1":             reasonLocal,
		"fd00::1":                reasonLocal,
		"::ffff:192.168.1.10":    reasonLocal,
		"93.184.216.34":          "",
		"172.32.0.1":             "",
		"2606:4700::1":           "",
	} {
		err := e.Address("files.example", netip.MustParseAddr(addr))
		if want == "" {
			if err != nil {
				t.Errorf("%s refused: %v", addr, err)
			}
			continue
		}
		if !errors.Is(err, ErrUnreachable) || EgressReason(err) != want || Permanent(err) {
			t.Errorf("%s: %v, want %q", addr, err, want)
		}
	}
	if err := e.Address("nas.home", netip.MustParseAddr("192.168.1.10")); err != nil {
		t.Errorf("an allowed local host was refused: %v", err)
	}
	// The list opens only the local network: a named host stays refused
	// this computer and link-local [DL-G4].
	listsLoopback := Egress{AllowedLocal: func(host string) bool { return host == "127.0.0.1" || host == "localhost" || host == "nas.home" }}
	for _, c := range []struct{ host, addr string }{{"nas.home", "127.0.0.1"}, {"127.0.0.1", "127.0.0.1"}, {"nas.home", "::1"}, {"nas.home", "169.254.169.254"}} {
		if err := listsLoopback.Address(c.host, netip.MustParseAddr(c.addr)); err == nil {
			t.Errorf("the allowed local list opened %s for %s", c.addr, c.host)
		}
	}
	if err := listsLoopback.Name("localhost"); err == nil || EgressReason(err) != reasonLoopback {
		t.Errorf("the allowed local list opened localhost: %v", err)
	}
}

// By name, the rules apply to a host that is an address or names this
// computer, and the person's refused list applies to the name [DL-G6, DL-G7].
func TestEgressNameRules(t *testing.T) {
	e := Egress{Refused: func(host string) error {
		if host == "blocked.example" {
			return errors.New("switched off")
		}
		return nil
	}}
	for host, want := range map[string]string{
		"localhost":       reasonLoopback,
		"app.localhost":   reasonLoopback,
		"127.0.0.1":       reasonLoopback,
		"[::1]":           reasonLoopback,
		"169.254.169.254": reasonLinkLocal,
		"10.0.0.5":        reasonLocal,
		"files.example":   "",
	} {
		err := e.Name(host)
		if want == "" && err != nil || want != "" && (err == nil || EgressReason(err) != want) {
			t.Errorf("%s: %v, want %q", host, err, want)
		}
	}
	if err := e.Name("blocked.example"); err == nil || err.Error() != "switched off" {
		t.Errorf("the refused list: %v", err)
	}
}

// https always; http only with a SHA-256 digest; every other scheme refused
// [DL-G1].
func TestEgressSchemeRules(t *testing.T) {
	e := Egress{}
	for raw, want := range map[string]string{
		"https://files.example/a": "",
		"http://files.example/a":  reasonPlainHTTP,
		"ftp://files.example/a":   reasonScheme,
		"file:///c:/a":            reasonScheme,
		"smb://nas/a":             reasonScheme,
		"data:text/plain,a":       reasonScheme,
	} {
		u, _ := url.Parse(raw)
		err := e.SchemeAllowed(u, false)
		if want == "" && err != nil || want != "" && (err == nil || EgressReason(err) != want) {
			t.Errorf("%s: %v, want %q", raw, err, want)
		}
	}
	u, _ := url.Parse("http://files.example/a")
	if err := e.SchemeAllowed(u, true); err != nil {
		t.Errorf("http with a digest: %v", err)
	}
}

// egressFixture serves https as example.com and http as plain.example, both
// on loopback, through a guarded transport whose resolver answers from a
// script and whose dials are recorded.
type egressFixture struct {
	secure, clear *httptest.Server
	g             *guardedTransport
	mu            sync.Mutex
	dialled       []string
	seen          []string
}

func newEgressFixture(t *testing.T, e Egress, secure, clear http.HandlerFunc, dns *answers) *egressFixture {
	t.Helper()
	f := &egressFixture{}
	wrap := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			f.mu.Lock()
			f.seen = append(f.seen, r.Host+r.URL.Path)
			f.mu.Unlock()
			h(w, r)
		}
	}
	f.secure = httptest.NewTLSServer(wrap(secure))
	t.Cleanup(f.secure.Close)
	f.clear = httptest.NewServer(wrap(clear))
	t.Cleanup(f.clear.Close)
	base := f.secure.Client().Transport.(*http.Transport).Clone()
	base.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		f.mu.Lock()
		f.dialled = append(f.dialled, addr)
		f.mu.Unlock()
		host, _, err := net.SplitHostPort(addr)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			return nil, errors.New("egress fixture refuses a non-loopback dial: " + addr)
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	old := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: base}
	t.Cleanup(func() { http.DefaultClient = old })
	f.g = &guardedTransport{check: e.Address, lookup: dns.lookup}
	return f
}

func (f *egressFixture) securePort() string {
	return f.secure.Listener.Addr().(*net.TCPAddr).AddrPort().String()[len("127.0.0.1:"):]
}
func (f *egressFixture) clearPort() string {
	return f.clear.Listener.Addr().(*net.TCPAddr).AddrPort().String()[len("127.0.0.1:"):]
}

func (f *egressFixture) get(e Egress, digest, locator string) error {
	ctx := withEgress(context.Background(), &e, digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, locator, nil)
	if err != nil {
		return err
	}
	resp, err := (HTTP{Client: &http.Client{Transport: f.g}}).do(req, e.Name, nil)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

const fixtureDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

// An https-to-http redirect is refused without a digest and followed with
// one; more than ten redirects are refused; every hop is checked again by
// name [DL-G3].
func TestEgressRedirectRules(t *testing.T) {
	// Loopback stands in for a public address here: the names are not
	// loopback names, so every scheme rule applies.
	e := Egress{Loopback: true}
	loopback := &answers{seq: [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}}}
	var f *egressFixture
	f = newEgressFixture(t, e, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/downgrade":
			http.Redirect(w, r, "http://plain.example:"+f.clearPort()+"/clear", http.StatusFound)
		case r.URL.Path == "/metadata":
			http.Redirect(w, r, "https://169.254.169.254/latest/meta-data", http.StatusFound)
		case strings.HasPrefix(r.URL.Path, "/loop"):
			http.Redirect(w, r, r.URL.Path+"x", http.StatusFound)
		default:
			w.Write([]byte("ok"))
		}
	}, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("clear")) }, loopback)
	origin := "https://example.com:" + f.securePort()

	err := f.get(e, "", origin+"/downgrade")
	if err == nil || EgressReason(err) != reasonDowngrade {
		t.Fatalf("a downgrade without a digest: %v", err)
	}
	for _, s := range f.seen {
		if strings.HasPrefix(s, "plain.example") {
			t.Fatal("the downgraded hop was sent")
		}
	}
	if err := f.get(e, fixtureDigest, origin+"/downgrade"); err != nil {
		t.Fatalf("a downgrade with a digest: %v", err)
	}
	if err := f.get(e, "", origin+"/metadata"); err == nil || EgressReason(err) != reasonLinkLocal {
		t.Fatalf("a redirect to a metadata address: %v", err)
	}
	for _, d := range f.dialled {
		if strings.HasPrefix(d, "169.254.") {
			t.Fatal("the metadata address was dialled")
		}
	}
	if err := f.get(e, "", origin+"/loop"); err == nil || !strings.Contains(err.Error(), "stopped after 10 redirects") {
		t.Fatalf("an eleventh redirect: %v", err)
	}
}

// DNS rebinding: a name whose answer changes from an allowed address to a
// link-local one between a check of the name and the dial is refused on the
// address actually dialled, and the refused address is never dialled [DL-G4].
func TestEgressRebindingIsCheckedOnTheDialledAddress(t *testing.T) {
	ctx := t.Context()
	if deadline, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	e := Egress{Loopback: true}
	dns := &answers{seq: [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}, {netip.MustParseAddr("169.254.169.254")}}}
	firstBody := make(chan struct{})
	var firstOnce sync.Once
	releaseFirst := func() { firstOnce.Do(func() { close(firstBody) }) }
	dnsAnswer := make(chan struct{})
	var dnsOnce sync.Once
	releaseDNS := func() { dnsOnce.Do(func() { close(dnsAnswer) }) }
	var requests atomic.Int32
	f := newEgressFixture(t, e, func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			select {
			case <-firstBody:
			case <-r.Context().Done():
				return
			}
		}
		w.Write([]byte("ok"))
	},
		func(w http.ResponseWriter, r *http.Request) {}, dns)
	t.Cleanup(releaseFirst)
	t.Cleanup(releaseDNS)
	// This rule concerns a new dial. A connection already pinned to the first
	// loopback answer can be reused safely, and closing idle connections does
	// not interrupt an in-use connection that may become idle afterwards.
	f.g.transport().DisableKeepAlives = true
	secondLookup := make(chan struct{})
	var lookupOnce sync.Once
	f.g.lookup = func(lookupCtx context.Context, host string) ([]netip.Addr, error) {
		answer, err := dns.lookup(lookupCtx, host)
		if len(answer) == 1 && answer[0].IsLinkLocalUnicast() {
			lookupOnce.Do(func() { close(secondLookup) })
			select {
			case <-dnsAnswer:
			case <-lookupCtx.Done():
				return nil, lookupCtx.Err()
			}
		}
		return answer, err
	}
	locator := "https://example.com:" + f.securePort() + "/file"
	request := func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(withEgress(ctx, &e, ""), http.MethodGet, locator, nil)
		if err != nil {
			return nil, err
		}
		return (HTTP{Client: &http.Client{Transport: f.g}}).do(req, e.Name, nil)
	}
	// The name passes its check, and the first answer is allowed.
	if err := e.Name("example.com"); err != nil {
		t.Fatal(err)
	}
	first, err := request()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	f.g.transport().CloseIdleConnections()
	second := make(chan error, 1)
	go func() {
		resp, err := request()
		if err == nil {
			err = resp.Body.Close()
		}
		second <- err
	}()
	select {
	case <-secondLookup:
	case <-ctx.Done():
		t.Fatal("second request did not reach fresh resolution", ctx.Err())
	}
	// Let the old response finish only after the second request has started.
	// With pooling enabled, this deterministically hands the old loopback
	// connection to that request despite the earlier CloseIdleConnections.
	releaseFirst()
	if _, err := io.Copy(io.Discard, first.Body); err != nil {
		t.Fatal(err)
	}
	if err := first.Body.Close(); err != nil {
		t.Fatal(err)
	}
	releaseDNS()
	select {
	case err = <-second:
	case <-ctx.Done():
		t.Fatal("second request did not finish", ctx.Err())
	}
	dns.mu.Lock()
	lookups := dns.asked
	dns.mu.Unlock()
	f.mu.Lock()
	dialled := append([]string(nil), f.dialled...)
	f.mu.Unlock()
	t.Logf("requests=%d DNS lookups=%d direct dials=%v", requests.Load(), lookups, dialled)
	if err == nil || EgressReason(err) != reasonLinkLocal {
		t.Fatalf("a rebound answer: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range f.dialled {
		if !strings.HasPrefix(d, "127.0.0.1:") {
			t.Fatalf("dialled %v", f.dialled)
		}
	}
	if lookups != 2 {
		t.Fatalf("%d lookups", lookups)
	}
}
