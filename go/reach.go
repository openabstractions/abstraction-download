package download

import (
	"encoding/json"
	"errors"
	"fmt"
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

func (r Reach) check(host string) error {
	host = strings.ToLower(host)
	if r == nil || host == "" {
		return nil
	}
	if err := r(host); err != nil {
		return fmt.Errorf("%w %s: %w", ErrUnreachable, host, err)
	}
	return nil
}

// HostOf is the host a locator opens a connection to, lowercased and without a
// port, or "" when it names none: a local path reaches nothing.
func HostOf(locator string) string {
	rest := locator
	switch {
	case strings.Contains(rest, "://"):
		rest = rest[strings.Index(rest, "://")+3:]
	case strings.HasPrefix(rest, `\\`), strings.HasPrefix(rest, "//"):
		rest = rest[2:]
	default:
		return ""
	}
	if i := strings.IndexAny(rest, `/\?#`); i >= 0 {
		rest = rest[:i]
	}
	if i := strings.LastIndex(rest, "@"); i >= 0 {
		rest = rest[i+1:]
	}
	if strings.HasPrefix(rest, "[") {
		if i := strings.Index(rest, "]"); i >= 0 {
			return strings.ToLower(rest[1:i])
		}
	}
	if i := strings.LastIndex(rest, ":"); i >= 0 {
		rest = rest[:i]
	}
	return strings.ToLower(rest)
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
