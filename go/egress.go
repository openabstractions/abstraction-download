package download

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"path/filepath"
	"strings"
)

// Egress is where a download an application asked for may connect, as
// established downloaders decide it [DL-G1 to DL-G8]. A service sets it on its
// runner; a bare runner has none and reaches what its Reach allows.
type Egress struct {
	// Refused is the person's refused-host list, asked about each host name
	// and each address it resolves to [DL-G6]. Nil refuses nothing.
	Refused Reach
	// AllowedLocal reports whether the person allowed host, by exact name, to
	// reach an address on the local network [DL-G5]. Nil allows none. It
	// never opens this computer's addresses [DL-G4].
	AllowedLocal func(host string) bool
	// Loopback lets the addresses of this computer through, and http without
	// a digest to them. Only tests whose fixtures serve on loopback set it.
	Loopback bool
}

// The plain reasons a refusal gives [DL-G8].
const (
	reasonScheme      = "Refused: only https and http downloads are allowed"
	reasonPlainHTTP   = "Refused: an http download needs a SHA-256 digest"
	reasonDowngrade   = "Refused: a redirect from https to http needs a SHA-256 digest"
	reasonLoopback    = "Refused: address is on this computer"
	reasonLinkLocal   = "Refused: address is link-local"
	reasonUnspecified = "Refused: address is unspecified"
	reasonMulticast   = "Refused: address is multicast"
	reasonBroadcast   = "Refused: address is broadcast"
	reasonLocal       = "Refused: address is on your local network"
)

// localNetworks are the private ranges refused unless the person allowed the
// host by name [DL-G5].
var localNetworks = []netip.Prefix{
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("fc00::/7"),
}

// thisNetwork is 0.0.0.0/8, which a connection reaches as this computer.
var thisNetwork = netip.MustParsePrefix("0.0.0.0/8")

// refusal is a connection this machine will not open, not now: the person
// may allow the host, or another machine may adopt the job [DL-E8].
func refusal(host, reason string) error {
	return fmt.Errorf("%w %s: %w", ErrUnreachable, host, errors.New(reason))
}

// plainIPv4 is addr's IPv4 form when it is IPv4-mapped (::ffff:a.b.c.d) or
// IPv4-compatible (::a.b.c.d), so each form of a refused address is refused.
func plainIPv4(addr netip.Addr) netip.Addr {
	addr = addr.WithZone("").Unmap()
	if addr.Is6() {
		b := addr.As16()
		zero := true
		for _, v := range b[:12] {
			zero = zero && v == 0
		}
		if zero && !addr.IsUnspecified() && addr != netip.IPv6Loopback() {
			return netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
		}
	}
	return addr
}

// allowed reports whether the person's allowed local hosts name host.
func (e Egress) allowed(host string) bool {
	return e.AllowedLocal != nil && e.AllowedLocal(strings.ToLower(strings.Trim(host, "[]")))
}

// addressReason is the reason addr is refused for host, or "" [DL-G4, DL-G5].
// The person's allowed local hosts open only the local network, to the hosts
// they name. Loopback is where the person's unauthenticated local services
// listen, and no list entry opens it.
func (e Egress) addressReason(host string, addr netip.Addr) string {
	a := plainIPv4(addr)
	switch {
	case a.IsLoopback():
		if e.Loopback {
			return ""
		}
		return reasonLoopback
	case a.IsMulticast():
		return reasonMulticast
	case a.IsLinkLocalUnicast():
		return reasonLinkLocal
	case a.IsUnspecified() || (a.Is4() && thisNetwork.Contains(a)):
		return reasonUnspecified
	case a == netip.AddrFrom4([4]byte{255, 255, 255, 255}):
		return reasonBroadcast
	}
	for _, n := range localNetworks {
		if n.Contains(a) {
			if e.allowed(host) {
				return ""
			}
			return reasonLocal
		}
	}
	return ""
}

// Address is the AddressCheck for a dialled address: the address rules and
// the person's refused list [DL-G4, DL-G5, DL-G6].
func (e Egress) Address(host string, addr netip.Addr) error {
	if reason := e.addressReason(host, addr); reason != "" {
		return refusal(host, reason)
	}
	return RefusedAddresses(e.Refused)(host, addr)
}

// Name is the Reach for a host name, asked before every connection and every
// redirect: the person's refused list, and the address rules for a host that
// is an address or names this computer. Through a proxy it is the only check
// [DL-G7].
func (e Egress) Name(host string) error {
	if e.Refused != nil {
		if err := e.Refused(host); err != nil {
			return err
		}
	}
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	if h == "localhost" || strings.HasSuffix(h, ".localhost") {
		if e.Loopback {
			return nil
		}
		return errors.New(reasonLoopback)
	}
	if addr, err := netip.ParseAddr(strings.Trim(h, "[]")); err == nil {
		if reason := e.addressReason(h, addr); reason != "" {
			return errors.New(reason)
		}
	}
	return nil
}

// loopbackName reports whether u names this computer.
func loopbackName(u *url.URL) bool {
	h := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if h == "localhost" || strings.HasSuffix(h, ".localhost") {
		return true
	}
	addr, err := netip.ParseAddr(h)
	return err == nil && plainIPv4(addr).IsLoopback()
}

// SchemeAllowed is the scheme rule for a source or a redirect target: https
// always, http only for a request bound to a SHA-256 digest [DL-G1].
func (e Egress) SchemeAllowed(u *url.URL, digestBound bool) error {
	switch strings.ToLower(u.Scheme) {
	case "https":
		return nil
	case "http":
		if digestBound || (e.Loopback && loopbackName(u)) {
			return nil
		}
		return refusal(u.Hostname(), reasonPlainHTTP)
	}
	return refusal(u.Hostname(), reasonScheme)
}

// redirectAllowed is the scheme rule for one redirect hop: an https-to-http
// downgrade needs the digest [DL-G3].
func (e Egress) redirectAllowed(from, to *url.URL, digestBound bool) error {
	if strings.EqualFold(from.Scheme, "https") && strings.EqualFold(to.Scheme, "http") && !digestBound &&
		!(e.Loopback && loopbackName(to)) {
		return refusal(to.Hostname(), reasonDowngrade)
	}
	return e.SchemeAllowed(to, digestBound)
}

// egressHop rides a request's context so HTTP.do applies the scheme rules to
// the source and to every redirect without learning where the policy lives.
type egressHop struct {
	egress      Egress
	digestBound bool
}

type egressKey struct{}

func withEgress(ctx context.Context, e *Egress, digest string) context.Context {
	if e == nil {
		return ctx
	}
	return context.WithValue(ctx, egressKey{}, egressHop{egress: *e, digestBound: strings.HasPrefix(strings.ToLower(digest), "sha256:")})
}

func egressOf(ctx context.Context) (egressHop, bool) {
	hop, ok := ctx.Value(egressKey{}).(egressHop)
	return hop, ok
}

// LocalHosts is the person's allowed local hosts: each name in it may reach
// an address on the local network, such as a NAS [DL-G5]. The file has the
// refused list's shape, a name and a note each, under the account's
// configuration folder, and it is read at the moment of each connection.
type LocalHosts struct{ Path string }

// DefaultLocalHosts is the list in the account's configuration folder.
func DefaultLocalHosts() LocalHosts {
	refused := DefaultRefusals().Path
	if refused == "" {
		return LocalHosts{}
	}
	return LocalHosts{Path: filepath.Join(filepath.Dir(refused), "allowed-local.json")}
}

// Allows reports whether host is on the list, by exact name. An unreadable
// list allows nothing.
func (l LocalHosts) Allows(host string) bool {
	hosts, err := l.List()
	if err != nil {
		return false
	}
	name, err := plainHost(host)
	if err != nil {
		return false
	}
	_, ok := hosts[name]
	return ok
}

// List is every allowed name with its note.
func (l LocalHosts) List() (map[string]string, error) { return Refusals(l).List() }

// Add allows host to reach the local network, with a note.
func (l LocalHosts) Add(host, note string) error { return Refusals(l).Refuse(host, note) }

// Remove takes host off the list.
func (l LocalHosts) Remove(host string) error { return Refusals(l).Allow(host) }

// EgressReason is the plain reason an egress refusal gives, from "Refused:"
// on, or err's own text when it gives none [DL-G8].
func EgressReason(err error) string {
	text := err.Error()
	if i := strings.Index(text, "Refused: "); i >= 0 {
		return text[i:]
	}
	return text
}
