// Package download moves bytes for a job.
//
// It sits on top of `job` and adds exactly one thing: something that can
// actually fetch. The job layer knows what is wanted, where it may be had, how
// far it got and who is allowed to work on it. This layer knows how to get it.
//
// The split matters because the interesting implementations are not ours. On
// Windows, BITS already runs transfers that survive logoff and reboot; on a NAS
// the right move is an SMB read, not a protocol. A facade that assumed it did
// the transferring itself could never delegate to either. So a Fetcher is
// deliberately small and dumb, and everything that has to be shared between
// implementations — identity, progress, verification, ownership — lives above
// it in the job record where every implementation can see it.
package download

import (
	"context"
	"errors"
	"io"
)

// Capability is something a Fetcher can promise. Fetchers differ enormously and
// a facade that presents them as interchangeable lies to its caller on the tier
// most people actually run: BITS survives a reboot, an in-process HTTP GET does
// not survive its own process, and neither of those facts is discoverable from
// the source's scheme.
type Capability string

const (
	// CapResume can start from a byte offset rather than from zero.
	CapResume Capability = "resume"
	// CapSurvivesProcessExit keeps transferring after the submitting process
	// is gone. Almost nothing has this; it is why the Windows service tier
	// exists at all.
	CapSurvivesProcessExit Capability = "survives_process_exit"
	// CapVerifies checks content itself. Most do not — BITS verifies size and
	// timestamp only, curl verifies nothing — which is why this layer hashes
	// as it writes rather than trusting the transport.
	CapVerifies Capability = "verifies_content"
	// CapDelegates hands the work to something else (a service, a NAS) rather
	// than doing it in this process.
	CapDelegates Capability = "delegates"
)

// Request is one attempt to get bytes from one source.
type Request struct {
	// Source is where to get it. Not a URL — see Source.
	Source Source

	// From is the offset to begin at. Bytes before it are already on disk AND
	// already proven; the runner has truncated the partial to exactly this
	// length before calling, so a Fetcher can append without checking.
	From int64

	// Out receives the bytes, in order, starting at From.
	Out io.Writer

	// Headers are already resolved and ready to send. A Fetcher never learns
	// that a credential exists: the Runner turns the source's credential NAME
	// into a secret at the last moment, so the secret is never in the job
	// record and never in a Fetcher's hands longer than one request.
	Headers map[string]string

	// Report is called as bytes land, with the running total written by THIS
	// request (not including From). It is advisory: the runner decides when to
	// persist, because persisting on every chunk would write the record
	// thousands of times a second.
	Report func(written int64)
}

// Result is what one attempt achieved.
type Result struct {
	// Written is how many bytes this attempt appended.
	Written int64
	// Total is the artifact's full size if the source revealed it, 0 if not.
	// Some sources only disclose length in a response header, and some never
	// do, so this is information rather than a promise.
	Total int64
}

// Fetcher moves bytes for one family of sources.
//
// Implementations do not verify, do not retry across sources, do not decide
// where the file goes and do not touch the job record. All of that is the
// runner's, so that it is done identically no matter which Fetcher ran — which
// is the only way a job started by one implementation can be finished by
// another.
type Fetcher interface {
	// Schemes are the job.Source.Scheme values this can serve.
	Schemes() []string
	// Capabilities are what it promises. Callers may require them.
	Capabilities() []Capability
	// Fetch appends bytes to req.Out. It must return an error rather than a
	// short read: deciding that a truncated transfer is acceptable is not a
	// transport's decision to make.
	Fetch(ctx context.Context, req Request) (Result, error)
}

var (
	// ErrNoFetcher means nothing registered can serve any of the job's sources
	// with the capabilities the job requires.
	ErrNoFetcher = errors.New("download: no fetcher for this job's sources")
	// ErrDigestMismatch means the bytes arrived and were not what was asked
	// for. This is a refusal, never a warning.
	ErrDigestMismatch = errors.New("download: digest mismatch")
	// ErrShortTransfer means the source ended before the expected size.
	ErrShortTransfer = errors.New("download: source ended early")
)

// Registry picks a Fetcher for a source.
type Registry struct {
	fetchers []Fetcher
}

func NewRegistry(fs ...Fetcher) *Registry { return &Registry{fetchers: fs} }

func (r *Registry) Add(f Fetcher) { r.fetchers = append(r.fetchers, f) }

// For returns a Fetcher that serves src and has every capability in requires.
func (r *Registry) For(src Source, requires []string) (Fetcher, bool) {
	for _, f := range r.fetchers {
		if !serves(f, src.Scheme) {
			continue
		}
		if hasAll(f, requires) {
			return f, true
		}
	}
	return nil, false
}

func serves(f Fetcher, scheme string) bool {
	for _, s := range f.Schemes() {
		if s == scheme {
			return true
		}
	}
	return false
}

func hasAll(f Fetcher, requires []string) bool {
	if len(requires) == 0 {
		return true
	}
	have := make(map[string]bool, len(f.Capabilities()))
	for _, c := range f.Capabilities() {
		have[string(c)] = true
	}
	for _, want := range requires {
		if !have[want] {
			return false
		}
	}
	return true
}
