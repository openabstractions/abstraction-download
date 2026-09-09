package download

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// ErrUnreachable means this machine will not open a connection to the host,
// whatever the source would have said.
//
// Not Permanent: the refusal is the machine's, not the job's. A person turns the
// host back on, or a machine that may reach it adopts the record, and the same
// job runs unchanged — the rule an absent trust store already follows.
var ErrUnreachable = errors.New("download: this machine will not reach")

// Reach answers, for the host a connection is about to be opened to, whether it
// may be. nil is yes. An error is the reason, in words a person chose, and it
// is written into the record so an application can show it.
//
// A seat, not a policy: the library asks and never decides. What sits in it is
// the caller's — Discover wires the hosts a person switched off; a bare
// NewRunner reaches everything, which is what a test wants.
type Reach func(host string) error

// check asks about a locator this machine has not parsed yet. checkURL asks
// about one the transport has already parsed, and is the one the fetcher uses:
// handing the transport's own *url.URL back means the host authorised and the
// host connected to are the same parse, not two parses that happen to agree.
func (r Reach) check(locator string) error {
	host, err := hostOf(locator)
	if err != nil {
		return err
	}
	return r.checkHost(host)
}

func (r Reach) checkURL(u *url.URL) error {
	host, err := hostOfURL(u)
	if err != nil {
		return err
	}
	return r.checkHost(host)
}

func (r Reach) checkHost(host string) error {
	if r == nil || host == "" {
		return nil
	}
	if err := r(host); err != nil {
		return fmt.Errorf("%w %s: %w", ErrUnreachable, host, err)
	}
	return nil
}

// HostOf is the host a locator opens a connection to, folded to lower case,
// without a port, without the brackets of an IPv6 literal and without the DNS
// root's trailing dot. "" when the locator names none — a local path reaches
// nothing — and "" for an authority this machine will not read, which callers
// that decide something must ask hostOf about instead.
func HostOf(locator string) string {
	host, err := hostOf(locator)
	if err != nil {
		return ""
	}
	return host
}

func hostOf(locator string) (string, error) {
	if strings.HasPrefix(locator, `\\`) {
		return plainHost(cutAtAny(locator[2:], `/\?#`))
	}
	u, err := url.Parse(locator)
	if err != nil {
		return "", fmt.Errorf("%w: %s is not a URI: %w", ErrUnreachable, locator, err)
	}
	return hostOfURL(u)
}

func hostOfURL(u *url.URL) (string, error) {
	if u.User != nil {
		return "", fmt.Errorf("%w: %s carries userinfo in its authority, and the three transports "+
			"this layer speaks for end userinfo at three different bytes", ErrUnreachable, u.Redacted())
	}
	host, err := plainHost(u.Hostname())
	if err != nil {
		return "", err
	}
	if host == "" && (u.Scheme == "http" || u.Scheme == "https") {
		return "", fmt.Errorf("%w: %s names no host", ErrUnreachable, u.Redacted())
	}
	return host, nil
}

// plainHost accepts only an authority that every transport reads the same way,
// and the exclusions are not tidiness. A byte above 0x7F is an internationalised
// name the transport canonicalises with an IDNA table this layer does not carry,
// so `xn--` spellings must be written out; the DNS root's dot is dropped because
// a resolver treats `hf.co.` and `hf.co` as one name and a refusal list must
// too.
func plainHost(host string) (string, error) {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if i := strings.IndexFunc(host, notInAHostName); i >= 0 {
		return "", fmt.Errorf("%w: %q is not a plain host name, so the host this machine would "+
			"authorise is not the host it would connect to", ErrUnreachable, host)
	}
	return host, nil
}

func notInAHostName(c rune) bool {
	return !strings.ContainsRune("abcdefghijklmnopqrstuvwxyz0123456789.-_:", c)
}

func cutAtAny(s, seps string) string {
	if i := strings.IndexAny(s, seps); i >= 0 {
		return s[:i]
	}
	return s
}

// Refusals is the hosts this machine will not reach, one reason each, in a file
// the window writes and every runner reads at the moment it would connect. A
// switch takes effect on the next connection and nothing restarts.
//
// A name covers its subdomains: refusing huggingface.co refuses
// cdn-lfs.huggingface.co, because a person naming a site means the site.
type Refusals struct{ Path string }

func DefaultRefusals() Refusals {
	dir, err := os.UserConfigDir()
	if err != nil {
		dir = os.TempDir()
	}
	return Refusals{Path: filepath.Join(dir, "openabstractions", "download", "refused.json")}
}

func (f Refusals) Check(host string) error {
	hosts, err := f.List()
	if err != nil {
		return fmt.Errorf("%s is unreadable: %w", f.Path, err)
	}
	for name, why := range hosts {
		if host == name || strings.HasSuffix(host, "."+name) {
			return errors.New(why)
		}
	}
	return nil
}

func (f Refusals) List() (map[string]string, error) {
	raw, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	hosts := map[string]string{}
	return hosts, json.Unmarshal(raw, &hosts)
}

func (f Refusals) Refuse(host, reason string) error {
	return f.edit(func(hosts map[string]string) { hosts[strings.ToLower(host)] = reason })
}

func (f Refusals) Allow(host string) error {
	return f.edit(func(hosts map[string]string) { delete(hosts, strings.ToLower(host)) })
}

func (f Refusals) edit(change func(map[string]string)) error {
	hosts, err := f.List()
	if err != nil {
		return err
	}
	change(hosts)
	raw, err := json.MarshalIndent(hosts, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(f.Path), 0o700); err != nil {
		return err
	}
	tmp := f.Path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.Path)
}
