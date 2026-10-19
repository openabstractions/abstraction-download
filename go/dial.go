package download

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sync"
	"time"
)

// AddressCheck answers, for a connection about to be opened straight to addr
// on behalf of host, whether it may be. nil is yes. host is the name the
// request named, or the address itself when it named one.
type AddressCheck func(host string, addr netip.Addr) error

// GuardedHTTP is HTTP whose every direct connection asks check about the
// address it is about to dial, after the name has resolved. The check and
// the dial see the same address, so a DNS answer that changes between a check
// of the name and the connection cannot slip past it. A connection to a proxy
// is opened unchecked: the proxy resolves the names, and the request's host is
// asked about by name through Reach.
func GuardedHTTP(check AddressCheck) HTTP {
	return HTTP{Client: &http.Client{Transport: &guardedTransport{check: check}}}
}

// guardedTransport builds its transport from the process's default client,
// the one HTTP{} uses, and rebuilds it only when that client changes. A test
// that points http.DefaultClient at its own certificates is followed, and a
// running service keeps one pool of connections.
type guardedTransport struct {
	check AddressCheck
	// lookup resolves a host name. Nil uses the system resolver.
	lookup func(ctx context.Context, host string) ([]netip.Addr, error)

	mu      sync.Mutex
	source  *http.Transport
	built   *http.Transport
	proxies sync.Map // proxy addresses this transport has been told to use
}

func (g *guardedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return g.transport().RoundTrip(req)
}

func (g *guardedTransport) transport() *http.Transport {
	base, _ := http.DefaultClient.Transport.(*http.Transport)
	if base == nil {
		base, _ = http.DefaultTransport.(*http.Transport)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.built != nil && g.source == base {
		return g.built
	}
	t := base.Clone()
	proxy := t.Proxy
	t.Proxy = func(r *http.Request) (*url.URL, error) {
		if proxy == nil {
			return nil, nil
		}
		u, err := proxy(r)
		if u != nil {
			g.proxies.Store(proxyAddr(u), true)
		}
		return u, err
	}
	dial := t.DialContext
	if dial == nil {
		dial = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return g.dial(ctx, dial, network, addr)
	}
	g.source, g.built = base, t
	return t
}

// dial resolves addr's host, asks check about each answer and dials the first
// one allowed and reachable by its address, so nothing resolves the name again
// between the check and the connection.
func (g *guardedTransport) dial(ctx context.Context, dial func(context.Context, string, string) (net.Conn, error), network, addr string) (net.Conn, error) {
	if _, proxied := g.proxies.Load(addr); proxied {
		return dial(ctx, network, addr)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	var answers []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		answers = []netip.Addr{ip}
	} else {
		lookup := g.lookup
		if lookup == nil {
			lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
				return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			}
		}
		if answers, err = lookup(ctx, host); err != nil {
			return nil, err
		}
	}
	// A refusal is the answer a person can act on, so it outranks an
	// address that merely did not answer.
	var refused, failed error
	for _, a := range answers {
		if err := g.check(host, a); err != nil {
			if refused == nil {
				refused = err
			}
			continue
		}
		conn, err := dial(ctx, network, net.JoinHostPort(a.String(), port))
		if err == nil {
			return conn, nil
		}
		if failed == nil {
			failed = err
		}
	}
	switch {
	case refused != nil:
		return nil, refused
	case failed != nil:
		return nil, failed
	}
	return nil, fmt.Errorf("download: %s resolved to no address", host)
}

// proxyAddr is the host:port a transport dials for proxy u, with the
// scheme's default port written out the way net/http writes it.
func proxyAddr(u *url.URL) string {
	port := u.Port()
	if port == "" {
		switch u.Scheme {
		case "https":
			port = "443"
		case "socks5", "socks5h":
			port = "1080"
		default:
			port = "80"
		}
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// RefusedAddresses is the AddressCheck for a refusal list: an address the
// list names is refused with its reason, as the host name is [DL-E8]. An
// IPv4 address written in IPv6 form is asked about in its IPv4 form.
func RefusedAddresses(reach Reach) AddressCheck {
	return func(host string, addr netip.Addr) error {
		if reach == nil {
			return nil
		}
		ip := addr.Unmap().WithZone("").String()
		if err := reach(ip); err != nil {
			return fmt.Errorf("%w %s (%s): %w", ErrUnreachable, host, ip, err)
		}
		return nil
	}
}
