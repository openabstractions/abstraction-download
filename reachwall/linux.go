package main

import (
	"fmt"
	"net"
	"strings"
	"unicode/utf8"
)

const prefixBytes = 127

func linux(list []refusal, gid string) (string, []string) {
	var b strings.Builder
	var warnings []string
	owner := "ct state new "
	if gid != "" {
		owner += "meta skgid " + gid + " "
	}
	b.WriteString("#!/usr/sbin/nft -f\n# reachwall: the hosts this machine refuses, as nftables rules. Re-runnable: replaces its own table.\n")
	b.WriteString("table inet reachwall\ndelete table inet reachwall\n")
	if len(list) == 0 {
		return b.String(), nil
	}
	b.WriteString("table inet reachwall {\n")
	var rules []string
	for _, r := range list {
		verdict := " counter log prefix " + nftString(prefix(r)) + " reject with icmpx admin-prohibited"
		ip := net.ParseIP(r.host)
		switch {
		case ip == nil:
			set := setName(r.host)
			fmt.Fprintf(&b, "\tset %s { type ipv4_addr; flags interval; }\n\tset %s6 { type ipv6_addr; flags interval; }\n", set, set)
			rules = append(rules, owner+"ip daddr @"+set+verdict, owner+"ip6 daddr @"+set+"6"+verdict)
			warnings = append(warnings, fmt.Sprintf("name %s: nftables matches addresses, not names, and resolving it here would freeze one DNS answer into a rule; sets %s and %s6 are empty, so nothing is refused, until a resolver fills them from live answers (README)", r.host, set, set))
		case ip.To4() != nil:
			rules = append(rules, owner+"ip daddr "+r.host+verdict)
		default:
			rules = append(rules, owner+"ip6 daddr "+r.host+verdict)
		}
	}
	b.WriteString("\tchain output {\n\t\ttype filter hook output priority filter; policy accept;\n")
	for _, rule := range rules {
		b.WriteString("\t\t" + rule + "\n")
	}
	b.WriteString("\t}\n}\n")
	return b.String(), warnings
}

func prefix(r refusal) string {
	s := "reach " + r.host + ": " + r.reason + " "
	for len(s) > prefixBytes {
		_, size := utf8.DecodeLastRuneInString(s)
		s = s[:len(s)-size]
	}
	return s
}

func nftString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
}

func setName(host string) string {
	return "host_" + strings.Map(func(c rune) rune {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			return c
		}
		return '_'
	}, host)
}
