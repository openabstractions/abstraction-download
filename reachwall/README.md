# reachwall — the refused list, in the platform's own packet filter

`Refusals` (`download/go/reach.go`) is a file of hosts this machine will not
reach, each with a reason. The runner refuses them for every program that asks
its seat. This compiles the same file into rules the platform enforces on
programs that never asked: Windows Firewall, or nftables. The reason stays in
our file and our error; the platform gets the host and the verdict.

Front end in the `ufw` / `firewalld` sense: one small policy compiled into the
native ruleset, replacing its own rules on every run. Go, standard library.

## Run

    go build -o bin/ ./download/reachwall
    bin/reachwall                        # this platform's rules, printed
    bin/reachwall --os linux             # the other platform's, from anywhere
    bin/reachwall --program 'C:\Apps\x.exe'   # windows: one executable
    bin/reachwall --group 60001          # linux: sockets owned by one gid
    bin/reachwall --apply                # load them (elevated / root)

With `{"127.0.0.1": "not this week, the disk is full"}` in the file, Linux gets

    table inet reachwall {
        chain output {
            type filter hook output priority filter; policy accept;
            ct state new ip daddr 127.0.0.1 counter log prefix "reach 127.0.0.1: not this week, the disk is full " reject with icmpx admin-prohibited
        }
    }

and a program that connects anyway sees `EHOSTUNREACH`, while `journalctl -k`
carries the line with the reason. `counter` says whether it ever fired.
`ct state new` is the contract's moment, a connection opening; without it the
rule also swallows the kernel's own *admin-prohibited* reply and the program
retransmits for ever instead of being told.

## What survives the translation

| ours | Windows | nftables |
|---|---|---|
| a literal address | `-RemoteAddress`, exact | `ip daddr`, exact |
| a name, and its subdomains | two dynamic keywords, `host` and `*.host`, `AutoResolve`: the firewall fills them from DNS answers it observes | nothing: a set per name, **empty until a resolver fills it** (`dnsmasq --nftset=/host/4#inet#reachwall#host_x,6#inet#reachwall#host_x6`) |
| the reason | `-Description` on the rule, readable in `wf.msc` | `log prefix`, 127 bytes, kernel log |
| one program | an executable path | a gid (`meta skgid`); a cgroup needs `nft_socket`, which the WSL kernel does not build |
| the machine | every program, when `--program` is absent | every socket, when `--group` is absent |

The tool never resolves a name itself: an address written into a rule is one
DNS answer frozen for ever, the same defect as a vendored CA bundle.

## What may break

- Windows resolves a name only from DNS answers it sees **after** the rule
  exists, only while Defender Network Protection is on, and not through DoH or
  a proxy; a reboot forgets every address until the next query. The script
  stops if Network Protection is off rather than install a rule that passes
  everything. Measured 2026-09-06 on the owner's machine: off.
- A name on Linux refuses nothing until something feeds its set. The tool says
  so on stderr every run.
- A keyword for a host later allowed is left behind, inert, because a keyword
  does not say who made it.
- `--apply` on Windows needs an elevated shell and has **not been run**; the
  Linux path was proven in WSL Ubuntu 26.04, kernel 6.18. macOS: not examined.
