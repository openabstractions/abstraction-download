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

	// From is the offset to begin at. The runner has positioned Out there and
	// nothing before it is this request's business.
	//
	// It used to also mean "the partial is exactly this long, so you may
	// append". It does not any more: the bytes already on disk need not be a
	// prefix, so the file may be longer than From and hold proven bytes past
	// the end of this request. A Fetcher writes to Out and does not reason
	// about the file.
	From int64

	// To is the offset to stop before, exclusive. Zero means "to the end of
	// whatever the source holds", which is what a transfer that runs to the end
	// of the artifact asks for, and the only thing a transfer whose length
	// nobody knows CAN ask for.
	//
	// A Fetcher that ignores this is wrong but not dangerous — it writes the
	// artifact's own bytes over the artifact's own bytes — and it is wrong in
	// the expensive direction: it fetches everything after the gap a second
	// time, which is precisely what having a bounded request was for.
	To int64

	// Validators identify the version of the artifact the bytes already on disk
	// came from, when a previous attempt recorded any. A Fetcher that can ask a
	// source to continue THAT version rather than whatever it is serving now
	// should — see Validators.IfRange. Empty when nothing was recorded, or when
	// From is zero and there is nothing to continue.
	Validators Validators

	// Out receives the bytes, in order, starting at From — unless Restart has
	// been called, after which they start at zero.
	Out io.Writer

	// Restart says the byte stream about to be written begins at zero rather
	// than at From, because the source turned out to be serving a different
	// version of the artifact than the one on disk.
	//
	// This is not an error and not a failure of the transfer: it is the correct
	// outcome of asking a changed source to continue, and the transfer proceeds
	// normally from byte zero. The runner's implementation truncates the partial
	// away, rewinds, resets the rolling hash and forgets the recorded prefix,
	// so everything downstream sees a download that simply started fresh.
	//
	// It must be called BEFORE the first byte of the new stream is written to
	// Out, and a Fetcher that cannot honour a restart must fail rather than
	// append. A nil Restart means the caller cannot rewind: fail in that case
	// too.
	Restart func() error

	// Observed reports what the source said about the version it is serving,
	// once and before any byte is written. The runner records it with the
	// checkpoint so the NEXT attempt can send it back. A Fetcher whose sources
	// have no notion of a version leaves this alone.
	Observed func(Validators)

	// Headers are already resolved and ready to send. A Fetcher never learns
	// that a credential exists: the Runner turns the source's credential NAME
	// into a secret at the last moment, so the secret is never in the job
	// record and never in a Fetcher's hands longer than one request.
	Headers map[string]string

	// Report is called as bytes land, with the running count written by THIS
	// request (not including From), and the artifact's full size if the source
	// has revealed it by now — 0 if it has not, and 0 is not a promise that it
	// never will. It is advisory: the runner decides when to persist, because
	// persisting on every chunk would write the record thousands of times a
	// second.
	//
	// The size is here, and not only in Result, because of a failing case rather
	// than an argument. Result is returned when the transfer ENDS, which is the
	// one moment a size is no longer interesting. So `dl <url>` could never show
	// a percentage — the server had sent Content-Length in its first response and
	// the number sat in a local variable until the download was already over.
	// modelget looked fine only because it resolves the size from HuggingFace
	// before submitting, which hid the gap for every AI download and left it
	// open for every other one.
	Report func(written, total int64)
}

// Result is what one attempt achieved.
type Result struct {
	// Written is how many bytes this attempt appended — or wrote from zero, if
	// it called Request.Restart.
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
	// ErrCannotRestart means a source answered a request to continue with a
	// stream that begins at zero, and the caller offered no way to rewind. The
	// bytes are fine; there is just nowhere to put them.
	ErrCannotRestart = errors.New("download: the stream restarts at zero and this request cannot rewind")
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
