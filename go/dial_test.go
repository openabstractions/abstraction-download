package download

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const refusedAddressWhy = "the person switched this address off"

// refusesTen refuses 10.1.2.3, the way a person's list naming it does.
func refusesTen(host string) error {
	if host == "10.1.2.3" {
		return errors.New(refusedAddressWhy)
	}
	return nil
}

// answers resolves every name to the next answer in its list, and repeats
// the last one.
type answers struct {
	mu    sync.Mutex
	asked int
	seq   [][]netip.Addr
}

func (a *answers) lookup(_ context.Context, host string) ([]netip.Addr, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := a.asked
	if i >= len(a.seq) {
		i = len(a.seq) - 1
	}
	a.asked++
	return a.seq[i], nil
}

func guardedGet(t *testing.T, g *guardedTransport, locator string) error {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, locator, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Close = true // every request dials, so every request resolves
	resp, err := (&http.Client{Transport: g}).Do(req)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func refusedForAddress(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrUnreachable) || !strings.Contains(err.Error(), refusedAddressWhy) || Permanent(err) {
		t.Fatalf("want a not-now refusal naming the person's reason, got %v", err)
	}
}

// A name whose answer the person's list refuses is refused at the dial, and
// an IPv4 address in IPv6 form is the same address [DL-E8].
func TestARefusedAddressIsRefusedAtTheDial(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()
	port := strings.TrimPrefix(srv.URL, "http://127.0.0.1:")
	for _, answer := range []string{"10.1.2.3", "::ffff:10.1.2.3"} {
		g := &guardedTransport{check: RefusedAddresses(refusesTen),
			lookup: (&answers{seq: [][]netip.Addr{{netip.MustParseAddr(answer)}}}).lookup}
		refusedForAddress(t, guardedGet(t, g, "http://nas.example:"+port+"/"))
	}
	// An address the list does not name is dialled.
	g := &guardedTransport{check: RefusedAddresses(refusesTen),
		lookup: (&answers{seq: [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}}}).lookup}
	if err := guardedGet(t, g, "http://mirror.example:"+port+"/"); err != nil || hits.Load() != 1 {
		t.Fatalf("an allowed address: %v, %d hits", err, hits.Load())
	}
}

// DNS rebinding: the name's answer changes after a connection to it was
// allowed, and the next connection dials only what it checked. The refused
// answer is never dialled [DL-E8].
func TestARebindingAnswerIsCheckedAtTheDial(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()
	port := strings.TrimPrefix(srv.URL, "http://127.0.0.1:")
	dns := &answers{seq: [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}, {netip.MustParseAddr("10.1.2.3")}}}
	g := &guardedTransport{check: RefusedAddresses(refusesTen), lookup: dns.lookup}
	locator := "http://rebind.example:" + port + "/"
	if err := guardedGet(t, g, locator); err != nil {
		t.Fatal(err)
	}
	refusedForAddress(t, guardedGet(t, g, locator))
	if hits.Load() != 1 || dns.asked != 2 {
		t.Fatalf("%d requests reached the server, %d lookups", hits.Load(), dns.asked)
	}
}

// Through a proxy the proxy resolves names: the connection to the proxy is
// opened without resolving the request's host, which Reach asks about by name.
func TestAProxiedRequestIsCheckedByName(t *testing.T) {
	var proxied atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied.Add(1)
		w.Write([]byte("via proxy"))
	}))
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)
	old := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	t.Cleanup(func() { http.DefaultClient = old })

	dns := &answers{seq: [][]netip.Addr{{netip.MustParseAddr("10.1.2.3")}}}
	g := &guardedTransport{check: RefusedAddresses(refusesTen), lookup: dns.lookup}
	if err := guardedGet(t, g, "http://far.example/file"); err != nil || proxied.Load() != 1 || dns.asked != 0 {
		t.Fatalf("proxied request: %v, %d at the proxy, %d lookups", err, proxied.Load(), dns.asked)
	}
	// The name itself is still refused before any connection.
	reach := func(host string) error {
		if host == "far.example" {
			return errors.New(refusedAddressWhy)
		}
		return nil
	}
	req, _ := http.NewRequest(http.MethodGet, "http://far.example/file", nil)
	if _, err := (HTTP{Client: &http.Client{Transport: g}}).do(req, reach, nil); !errors.Is(err, ErrUnreachable) || proxied.Load() != 1 {
		t.Fatalf("a refused name through a proxy: %v, %d at the proxy", err, proxied.Load())
	}
}
