package main

import (
	"crypto/sha1"
	"fmt"
	"net"
	"strings"
)

const group = "openabstractions reach"

func windows(list []refusal, program string) (string, []string) {
	var b strings.Builder
	var warnings []string
	b.WriteString("# reachwall: the hosts this machine refuses, as Windows Firewall rules. Elevated shell; re-runnable.\n")
	b.WriteString("$ErrorActionPreference = 'Stop'\n")
	if names(list) {
		b.WriteString("if ((Get-MpPreference).EnableNetworkProtection -eq 0) { throw 'reachwall: a rule on a name is enforced only while Defender Network Protection is on, and it is off: Set-MpPreference -EnableNetworkProtection Enabled, or the rule passes everything in silence' }\n")
		warnings = append(warnings, "a name is refused by the addresses Windows sees in DNS answers after this runs: an address the program already holds, or fetched over DoH or a proxy, still connects, and a reboot forgets them all until the next query")
	}
	fmt.Fprintf(&b, "Get-NetFirewallRule -Group %s -ErrorAction SilentlyContinue | Remove-NetFirewallRule\n", ps(group))
	for _, r := range list {
		b.WriteString("\n")
		match := "-RemoteAddress " + ps(r.host)
		if net.ParseIP(r.host) == nil {
			var ids []string
			for _, keyword := range []string{r.host, "*." + r.host} {
				id := ps(guid(keyword))
				fmt.Fprintf(&b, "Remove-NetFirewallDynamicKeywordAddress -Id %s -ErrorAction SilentlyContinue\n", id)
				fmt.Fprintf(&b, "New-NetFirewallDynamicKeywordAddress -Id %s -Keyword %s -AutoResolve $true\n", id, ps(keyword))
				ids = append(ids, id)
			}
			match = "-RemoteDynamicKeywordAddresses @(" + strings.Join(ids, ", ") + ")"
		}
		fmt.Fprintf(&b, "New-NetFirewallRule -Group %s -DisplayName %s -Description %s -Direction Outbound -Action Block -Profile Any %s",
			ps(group), ps("reach "+r.host), ps(r.reason), match)
		if program != "" {
			fmt.Fprintf(&b, " -Program %s", ps(program))
		}
		b.WriteString("\n")
	}
	return b.String(), warnings
}

func names(list []refusal) bool {
	for _, r := range list {
		if net.ParseIP(r.host) == nil {
			return true
		}
	}
	return false
}

func ps(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// RFC 9562 UUIDv5 in the DNS namespace, so the same keyword always has the
// same id and a re-run replaces its own keyword instead of leaking one.
func guid(keyword string) string {
	h := sha1.New()
	h.Write([]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8})
	h.Write([]byte(keyword))
	u := h.Sum(nil)[:16]
	u[6] = u[6]&0x0f | 0x50
	u[8] = u[8]&0x3f | 0x80
	return fmt.Sprintf("{%x-%x-%x-%x-%x}", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}
