package nas

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Found is a host on this network that might hold a shared store.
//
// Everything a device says about itself is a claim, and this struct keeps the
// two apart. Address is the source of the datagram, which the socket reports and
// the sender cannot choose. Name and Says come out of the payload: any machine
// on the LAN can answer these questions calling itself anything it likes. So a
// caller may route by Address and must only ever display Name and Says.
type Found struct {
	Address string `json:"address"`
	// Name is what the reverse lookup or the device's own header calls it.
	Name string `json:"name"`
	// Says is the device's description of itself, verbatim and untrusted.
	Says string `json:"says"`
	// Shares is what the platform's own share browser could list. Incomplete by
	// design: a share marked hidden is served and not listed, so a person must
	// still be able to name one this does not mention.
	Shares []string `json:"shares"`
	// SharesUnknown is why nobody asked, and is empty when Shares is an answer.
	// A caller reading only len(Shares) cannot tell a host that serves nothing
	// from a platform with no way to ask, and off Windows it is always the
	// second.
	SharesUnknown string `json:"shares_unknown,omitempty"`
	// How names the questions it answered, because "it serves SMB" and "it
	// answered a UPnP search" are very different amounts of evidence.
	How []string `json:"how"`
}

// Find asks the network what is on it. It offers; nothing here adopts.
//
// Two questions, both plain UDP multicast, because the documented one does not
// work here: Synology Assistant's UDP 9999 broadcast got no reply from a DS218+
// two metres away, on three payloads and both broadcast addresses. What did
// answer, measured 2026-09-07, was an SSDP M-SEARCH on 239.255.255.250:1900 and
// an mDNS PTR for _smb._tcp on 224.0.0.251:5353 — see
// research/nas-discovery/RESULTS.txt.
func Find(ctx context.Context) []Found {
	var mu sync.Mutex
	seen := map[string]*Found{}
	note := func(addr net.IP, how, says, calls string) {
		ip := addr.String()
		mu.Lock()
		defer mu.Unlock()
		f := seen[ip]
		if f == nil {
			f = &Found{Address: ip}
			seen[ip] = f
		}
		if !contains(f.How, how) {
			f.How = append(f.How, how)
		}
		if says != "" && f.Says == "" {
			f.Says = says
		}
		if calls != "" && f.Name == "" {
			f.Name = calls
		}
	}

	var wg sync.WaitGroup
	for _, q := range []struct {
		how, group string
		port       int
		ask        []byte
		read       func([]byte) (says, calls string)
	}{
		{"upnp", "239.255.255.250", 1900, mSearch(),
			func(b []byte) (string, string) { return serverHeader(b), "" }},
		{"smb", "224.0.0.251", 5353, ptrQuery("_smb", "_tcp", "local"),
			func(b []byte) (string, string) { return "", instanceName(b) }},
	} {
		// Once per local address, not once. A socket bound to nothing sends
		// multicast out whichever interface the routing table prefers, and this
		// machine has five: a WSL bridge, a Bluetooth link, and three Wi-Fi
		// adapters of which one has the LAN and two hold 169.254 addresses. Two
		// runs in five found nothing at all, from a NAS that had answered a
		// minute earlier — the question had gone out of a door with no house
		// behind it.
		for _, from := range everyAddress() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, r := range askGroup(ctx, from, q.group, q.port, q.ask) {
					says, calls := q.read(r.body)
					note(r.from, q.how, says, calls)
				}
			}()
		}
	}
	wg.Wait()

	out := make([]Found, 0, len(seen))
	for _, f := range seen {
		out = append(out, *f)
	}
	fill(ctx, out)
	sort.Slice(out, func(i, j int) bool {
		if len(out[i].Shares) != len(out[j].Shares) {
			return len(out[i].Shares) > len(out[j].Shares)
		}
		return out[i].Address < out[j].Address
	})
	return out
}

// fill names each host and lists what it serves, in parallel: a share
// enumeration is a second and a half against a NAS that is awake and the whole
// timeout against one that is not.
func fill(ctx context.Context, hosts []Found) {
	var wg sync.WaitGroup
	for i := range hosts {
		wg.Add(1)
		go func(f *Found) {
			defer wg.Done()
			names, err := shares(ctx, f.Address)
			if err != nil {
				f.SharesUnknown = err.Error()
			}
			f.Shares = names
			if len(f.Shares) > 0 && !contains(f.How, "smb") {
				f.How = append(f.How, "smb")
			}
			if f.Name == "" {
				f.Name = name(ctx, f.Address, f.Says)
			}
		}(&hosts[i])
	}
	wg.Wait()
}

// name prefers what the resolver says over what the device says, and falls back
// to the address, which is the only part nobody chose.
func name(ctx context.Context, addr, says string) string {
	var r net.Resolver
	if names, err := r.LookupAddr(ctx, addr); err == nil && len(names) > 0 {
		return strings.TrimSuffix(strings.TrimSuffix(names[0], "."), ".local")
	}
	if vendor, _, ok := strings.Cut(says, "/"); ok && vendor != "" {
		return vendor
	}
	return addr
}

type reply struct {
	from net.IP
	body []byte
}

// asked is how often the question is repeated inside the window.
//
// Multicast is lossy and both of these protocols expect the asker to repeat —
// SSDP for exactly this reason gives the responder an MX to spread its reply
// over. Measured: a single query got nothing at all back from a NAS that was
// awake and had answered the same question a minute earlier, which as a control
// panel's "Find on my network" button reads as an empty list and a person
// concluding they have no NAS.
const asked = 400 * time.Millisecond

// everyAddress is each IPv4 address this machine can ask from. Interfaces that
// are down, loopback, or cannot carry multicast are left out; a link-local
// address is kept, because a network with no DHCP server is still a network.
func everyAddress() []string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return []string{""}
	}
	var out []string
	for _, i := range ifaces {
		if i.Flags&net.FlagUp == 0 || i.Flags&net.FlagLoopback != 0 || i.Flags&net.FlagMulticast == 0 {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil {
				out = append(out, n.IP.String())
			}
		}
	}
	if len(out) == 0 {
		return []string{""}
	}
	return out
}

// askGroup repeats one question to a multicast group from one local address and
// collects what comes back, capped so a chatty network cannot fill memory.
func askGroup(ctx context.Context, from, group string, port int, ask []byte) []reply {
	c, err := net.ListenPacket("udp4", net.JoinHostPort(from, "0"))
	if err != nil {
		return nil
	}
	defer c.Close()
	to := &net.UDPAddr{IP: net.ParseIP(group), Port: port}

	// Its own window, not the caller's. A device that is going to answer a
	// multicast question answers in milliseconds; the rest of the caller's time
	// belongs to asking each of them what it serves, which is slow and is where
	// a host that is switched off costs the whole timeout.
	deadline := time.Now().Add(2 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	var out []reply
	buf := make([]byte, 2048)
	for len(out) < 64 {
		if _, err := c.WriteTo(ask, to); err != nil && len(out) == 0 {
			return nil
		}
		next := time.Now().Add(asked)
		if next.After(deadline) {
			next = deadline
		}
		c.SetReadDeadline(next)
		for len(out) < 64 {
			n, from, err := c.ReadFrom(buf)
			if err != nil {
				break
			}
			if udp, ok := from.(*net.UDPAddr); ok {
				out = append(out, reply{from: udp.IP, body: append([]byte(nil), buf[:n]...)})
			}
		}
		if !time.Now().Before(deadline) {
			return out
		}
	}
	return out
}

func mSearch() []byte {
	return []byte("M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\n" +
		"MAN: \"ssdp:discover\"\r\nMX: 1\r\nST: ssdp:all\r\n\r\n")
}

func serverHeader(body []byte) string {
	for _, line := range strings.Split(string(body), "\r\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(k), "server") {
			return printable(strings.TrimSpace(v))
		}
	}
	return ""
}

// instanceName is what a device calls the service it just advertised — the
// first label of the first PTR answer, which for _smb._tcp is the name a person
// sees on their own network and recognises.
//
// A name off the wire and nothing more. It is displayed, never resolved and
// never joined to a path: the address a share is reached at comes from the
// datagram's source, which the sender does not get to choose.
func instanceName(msg []byte) string {
	if len(msg) < 12 {
		return ""
	}
	i, questions, answers := 12, int(msg[4])<<8|int(msg[5]), int(msg[6])<<8|int(msg[7])
	for ; questions > 0; questions-- {
		_, next, ok := readName(msg, i)
		if !ok {
			return ""
		}
		i = next + 4
	}
	for ; answers > 0; answers-- {
		_, next, ok := readName(msg, i)
		if !ok || next+10 > len(msg) {
			return ""
		}
		rdata := next + 10
		if kind := int(msg[next])<<8 | int(msg[next+1]); kind == 12 {
			if target, _, ok := readName(msg, rdata); ok {
				return printable(strings.SplitN(target, ".", 2)[0])
			}
		}
		i = rdata + int(msg[next+8])<<8 | int(msg[next+9])
	}
	return ""
}

// readName walks a DNS name, following the compression pointers the format is
// built on, and returns where the name ended in the message rather than where
// the last pointer landed.
func readName(msg []byte, i int) (name string, next int, ok bool) {
	var labels []string
	for hops := 0; hops < 24; hops++ {
		if i >= len(msg) {
			return "", 0, false
		}
		switch n := int(msg[i]); {
		case n == 0:
			if next == 0 {
				next = i + 1
			}
			return strings.Join(labels, "."), next, true
		case n&0xc0 == 0xc0:
			if i+1 >= len(msg) {
				return "", 0, false
			}
			if next == 0 {
				next = i + 2
			}
			i = n&0x3f<<8 | int(msg[i+1])
		default:
			if i+1+n > len(msg) {
				return "", 0, false
			}
			labels = append(labels, string(msg[i+1:i+1+n]))
			i += 1 + n
		}
	}
	return "", 0, false
}

// ptrQuery asks who serves a service.
func ptrQuery(labels ...string) []byte {
	q := []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0}
	for _, l := range labels {
		q = append(q, byte(len(l)))
		q = append(q, l...)
	}
	return append(q, 0, 0, 12, 0, 1)
}

// printable keeps a device's own words readable and harmless. It is displayed
// beside a control that redirects downloads, and a name carrying control
// characters or a hundred lines is a device deciding how this machine's window
// looks.
func printable(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 0x20 && r != 0x7f && b.Len() < 80 {
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

// Check proves a store could live here, rather than asserting it.
//
// Reachable is not enough and neither is a directory listing: a share mounted
// read-only, or one whose guest account may list and not write, fails at the
// first job and looks exactly like a download that started. So a byte is
// written, read back and removed. Available() does the first half of this on
// every discovery and this is the half it cannot afford to do there.
func Check(ctx context.Context, root string) error {
	d, err := New(root)
	if err != nil {
		return err
	}
	if err := d.Available(); err != nil {
		return err
	}
	probe := filepath.Join(root, "jobs", fmt.Sprintf(".writable-%d", time.Now().UnixNano()))
	want := []byte(probe)
	if err := os.WriteFile(probe, want, 0o644); err != nil {
		return fmt.Errorf("nas: %s is reachable but not writable: %w", root, err)
	}
	defer os.Remove(probe)
	got, err := os.ReadFile(probe)
	if err != nil {
		return fmt.Errorf("nas: %s took the write and would not read it back: %w", root, err)
	}
	if string(got) != string(want) {
		return fmt.Errorf("nas: %s read back something else", root)
	}
	return nil
}

// Path is the UNC this machine would use for a store on one of these hosts.
//
// Built from the address and never from anything the device said, and the share
// and directory are checked rather than pasted: a name with a separator or a
// parent reference in it walks out of the share a person chose, which is the
// same escape the sink field already refuses one layer down.
func Path(address, share, dir string) (string, error) {
	if net.ParseIP(address) == nil {
		return "", fmt.Errorf("nas: %q is not an address this machine found", address)
	}
	parts := []string{address}
	for _, seg := range append([]string{share}, strings.Split(dir, "/")...) {
		if seg = strings.TrimSpace(seg); seg == "" {
			continue
		}
		if seg == "." || seg == ".." || strings.ContainsAny(seg, `\/:*?"<>|`) {
			return "", fmt.Errorf("nas: %q is not a folder name", seg)
		}
		parts = append(parts, seg)
	}
	if len(parts) < 2 {
		return "", fmt.Errorf("nas: no share named on %s", address)
	}
	return "//" + strings.Join(parts, "/"), nil
}
