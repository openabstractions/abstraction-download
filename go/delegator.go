package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// A Delegator hands the whole job to something outside this process and then
// watches it.
//
// This is the second of exactly two shapes, and between them they cover every
// implementation category the surveys turned up:
//
//	Fetcher    streams bytes through us      http, a file, an SMB path
//	Delegator  the other system does it      BITS, a curl subprocess, a NAS
//	                                         daemon, a torrent client, a
//	                                         durable execution engine
//
// The distinction is not stylistic. A Fetcher gives us an io.Writer's worth of
// control and dies with our process. A Delegator writes the file itself, under
// its own account, keeps going while every process that asked is closed, and
// hands back nothing but a handle. Windows BITS is the archetype: it owns an
// on-disk queue, survives reboot, and will not release the file at all until
// Finalize is called.
//
// Trying to force BITS through the Fetcher interface would have meant
// reimplementing BITS badly. Adding this interface instead is what the surveys
// were for.
type Delegator interface {
	// Close releases whatever building this delegate acquired.
	//
	// Tier.New IS the probe, and it runs again on every Offers and every
	// Rebind. Every tier that shipped before this one was a directory path or a
	// COM call with nothing to release, so the missing counterpart cost
	// nothing and nobody noticed it was missing. A delegate that owns a child
	// process leaks one per call, which is what a plugin is — so the
	// counterpart is on the interface rather than optional, because a leak
	// nobody is obliged to close is a leak.
	//
	// It must be safe on a delegate that acquired nothing: most return nil.
	io.Closer

	// System is the name recorded in job.Delegation.System. It decides who can
	// interpret an ExternalID later — possibly after a reboot, in another
	// process, written by another language.
	System() string

	// Schemes are the source schemes this can serve.
	Schemes() []string

	// Capabilities are what it promises. A Delegator that does not claim
	// CapSurvivesProcessExit is not worth delegating to.
	Capabilities() []Capability

	// Start hands the work over and returns the external handle. It must not
	// block until the transfer finishes — the entire point is that the transfer
	// outlives this call, and usually this process.
	//
	// An error from Start is read as "this delegate did not take the work", and
	// the job is then offered elsewhere or run here. A delegate whose Start can
	// accept the work and still fail — because the answer crosses a share, a
	// pipe or a process boundary that can drop after the far side has committed
	// — must implement Locator, or that error becomes a second transfer of the
	// same artifact. See Locator.
	Start(ctx context.Context, spec Spec, from int64) (externalID string, err error)

	// Poll reports on delegated work. Any process may call it, including one
	// that never started anything — that is what makes progress observable from
	// outside, which a callback cannot be.
	Poll(ctx context.Context, externalID string) (Status, error)

	// Finalize takes delivery into dest. BITS calls this Complete(), and until
	// it happens the file is not the caller's and may not exist at its final
	// path at all. A job left unacknowledged sits in the BITS queue for 90 days.
	//
	// dest is the job's destination resolved on THIS machine. BITS ignores it,
	// because it was told where to put the file at Start and has been holding it
	// there. That is exactly why the parameter was missing at first, and why it
	// had to be added: an implementation whose work lands somewhere else — a NAS
	// writing to its own disk — has no way to discover the local destination
	// from a handle alone. The interface described one binding's habits rather
	// than the operation. "Take delivery" needs to say delivery of what, and
	// where.
	Finalize(ctx context.Context, externalID, dest string) error

	// Abandon cancels the work and cleans up after it. Note that BITS's Cancel
	// deletes completed files as well as partial ones, so this is not a way to
	// keep what has arrived so far.
	Abandon(ctx context.Context, externalID string) error
}

// DelegateState is the external system's view of the work.
type DelegateState string

const (
	// DelegateRunning is still working, or waiting to.
	DelegateRunning DelegateState = "running"
	// DelegateTransferred has all the bytes but has not handed them over.
	// Finalize turns this into a file the caller owns.
	DelegateTransferred DelegateState = "transferred"
	// DelegateFailed gave up.
	DelegateFailed DelegateState = "failed"
	// DelegateGone means the external system has never heard of this handle.
	// BITS reaps jobs after 90 days, queue databases get corrupted and
	// replaced, and a machine can be rebuilt — so a handle that no longer
	// resolves is a normal outcome, not an exception.
	DelegateGone DelegateState = "gone"
)

// Status is one poll's answer.
type Status struct {
	State DelegateState
	// Done is how far the ARTIFACT is, counted from zero — not how much this
	// attempt transferred. A delegate handed a resume offset therefore reports
	// at least that offset from its first poll, and one that reports less has
	// refuted its own CapResume claim (Runner.Reconcile). Both delegates that
	// shipped before this sentence already meant it — BITS counts
	// BytesTransferred against the file it owns, the NAS reports the remote
	// record's Progress.Done — and neither said so, which left the one cheap
	// instrument we have resting on a coincidence.
	Done  int64
	Total int64
	Err   string
	// Suspended distinguishes "stopped because somebody asked" from "running",
	// which State deliberately does not: to a supervisor deciding whether to
	// take work back, a suspended job is not failed and not finished, so it maps
	// to Running and always did.
	//
	// It is here rather than on the record because it is an observation, not a
	// decision. What somebody WANTS lives in Intent; this is what the delegate
	// happens to be doing about it, and adding it to the record would have meant
	// a schema change across three languages to carry a fact that is re-read on
	// every poll anyway.
	Suspended bool
}

var (
	// ErrNoDelegator means nothing registered can hand this job anywhere.
	ErrNoDelegator = errors.New("download: no delegator for this job's sources")
	// ErrNotDelegated means the job is not in someone else's hands.
	ErrNotDelegated = errors.New("download: job is not delegated")
	// ErrStrandedHere is a record already in somebody's hands, naming a system
	// THIS process cannot speak to. Distinct from ErrNoDelegator, which is this
	// process having nowhere to send a job in the first place: there the work is
	// still ours to do, and here it is not ours to touch at all.
	//
	// It wraps ErrNoDelegator so that a caller asking only the coarse question
	// gets the same answer it always did.
	ErrStrandedHere = fmt.Errorf("%w: nothing here understands", ErrNoDelegator)
	// ErrClaimFalsified is a delegate whose own behaviour destroyed a
	// capability it claimed. The job is not lost — it comes back here with its
	// checkpoint intact — so this is not permanent; it is the one moment the
	// difference between what a delegate says and what it does is visible, and
	// it names the delegate because attribution is how a bad plugin stops
	// looking like us.
	ErrClaimFalsified = errors.New("download: a delegate falsified a capability it claimed")
	// ErrOutcomeUnknown is a handoff whose result nobody can determine: the
	// delegate may hold this work and may never have seen it, and no question
	// this process can ask separates the two.
	//
	// It is a third answer, not a flavour of failure, and it is the answer this
	// layer could not give. ErrNoDelegator means nothing here can serve the job
	// and it is still ours; a Refusal means a named delegate looked and said no;
	// this means something may be transferring an artifact we can no longer see.
	// A caller that treats it as failure starts a second transfer, which is the
	// one outcome worse than stopping.
	//
	// So the job is left delegated to nobody and not adopted. It is not lost:
	// the delegate is asked again on the next sweep, under the same request
	// identity, and answers as soon as it can be reached.
	ErrOutcomeUnknown = errors.New("download: the delegate's answer was lost and it may hold this work")
)

// Locator is an OPTIONAL capability: a delegate that can be asked what it did
// with a request, rather than only being told to start one.
//
// It exists because Start's answer can be lost after the far side has committed
// — a record written across a share whose reply never came back, a child
// process that created a transfer and died before printing its handle. Every
// delegate this project ships has that window, and without this the loss is
// indistinguishable from a refusal, so the facade falls back and the same
// artifact is fetched twice.
//
// This is the reconnect half of an at-least-once handoff, and the pattern is
// old: an idempotency key on a payment request, a transaction recovery scan by
// XID in XA, an operation name in a long-running-operation API. What is being
// named is the REQUEST, not the work, and it is minted by the caller before the
// first attempt precisely so that it survives the attempt failing.
//
// Locate must be safe to call from any process at any time, including one that
// never started anything, and it must be cheap: it is on the recovery path of
// every uncertain handoff.
type Locator interface {
	// Locate reports the external handle for a request this delegate accepted.
	//
	// Three answers, and the difference between them is the whole point:
	//
	//	handle, nil   it holds this work; adopt the handle rather than start again
	//	"", nil       it is CERTAIN it never accepted this request; fall back freely
	//	"", err       it cannot tell, which is ErrOutcomeUnknown and not a refusal
	//
	// The empty answer is a positive claim, not a shrug. A delegate that cannot
	// distinguish "I never had it" from "I cannot reach my own records" must
	// return an error for both.
	Locate(ctx context.Context, request string) (externalID string, err error)
}

// Delegators picks a Delegator for a source, the same way Fetchers does for
// Fetchers.
type Delegators struct {
	all []Delegator
}

func NewDelegators(ds ...Delegator) *Delegators { return &Delegators{all: ds} }

func (d *Delegators) Add(x Delegator) { d.all = append(d.all, x) }

// Without returns these delegators minus one system, so a supervisor can be run
// deliberately one tier lower than the machine would otherwise choose.
//
// This is for the operator, not for applications, and the difference is the
// whole design. An application must never say "not the NAS" — it does not know
// what a NAS is. A person standing at the machine saying "show me what happens
// without it" is asking a legitimate question about their own computer, and
// answering it is how the tiers become demonstrable rather than asserted.
//
// Deliberately negative-only. There is no Only(system): forcing a tier that is
// not configured or not reachable cannot work, and a flag that silently does
// nothing is worse than no flag.
//
// What is removed is CLOSED, because a probe built it and this chain was the
// only thing holding it. Every caller reassigns over the chain it passed in, so
// leaving the dropped delegate open would leak whatever it acquired — a child
// process, for a plugin — with nothing left pointing at it.
func (d *Delegators) Without(system string) *Delegators {
	if d == nil {
		return NewDelegators()
	}
	kept := make([]Delegator, 0, len(d.all))
	for _, x := range d.all {
		if x.System() == system {
			x.Close()
			continue
		}
		kept = append(kept, x)
	}
	return &Delegators{all: kept}
}

// Selective is an OPTIONAL capability: a delegate that can tell, from the job
// itself, that it could never do this one.
//
// Scheme and capabilities are not enough, and the gap strands work silently. A
// NAS serves "https" and promises to survive process exit, so it was handed a
// job whose source was http://127.0.0.1 -- an address that means THIS machine,
// and is unreachable from any other. The far side could never fetch it, the
// record sat running forever, and every sweep reported reconciling it.
//
// Only the delegate can answer this, which is the same argument this project
// already makes about intent: "asking for something the current owner cannot do
// is not an error here, because only the owner knows what it can do".
//
// Optional, so a delegate with nothing to refuse implements nothing.
//
// ONLY FOR A DELEGATE IN THIS PROCESS. It takes the whole Spec, so a delegate
// on the far side of a pipe can only answer it by being shown every job it
// might serve — which is the power to watch, handed to something that was only
// meant to be given work. Anything out of process declares instead: see
// Declaration.
type Selective interface {
	// CanServe reports whether this delegate could perform this job at all.
	// It is about POSSIBILITY, not preference or load: answering false means
	// the work would be impossible here, not merely inconvenient.
	CanServe(spec Spec) bool
}

// Declaration is what a delegate could never do, said once and evaluated here.
//
// It replaces Selective for any delegate that does not share our address space,
// and the difference is who sees the job. CanServe asks the delegate about
// every job it might serve; a Declaration is a filter this package applies, so
// a delegate sees only the jobs it won. Chromium spent two years replacing the first shape
// with the second — webRequest saw every request, declarativeNetRequest states
// rules the browser evaluates — and the per-job veto here was written when
// every tier was ours and the distinction did not bite.
//
// It is a filter, never a behaviour: the delegate still runs its own code on
// the jobs it is given, and this says nothing about how.
//
// The vocabulary is closed and small on purpose. Anything it cannot express is
// a job the delegate never sees, which is the direction that fails safe, and
// every term added is one more thing three languages must agree about forever.
//
// Scheme is deliberately absent. Delegator.Schemes is already declared once and
// already evaluated here, and a second spelling of one axis is a fact in two
// places.
type Declaration struct {
	// Hosts is the shape of source host this delegate can reach.
	Hosts HostShape `json:"hosts,omitempty"`

	// MinSize and MaxSize bound the artifact, in bytes. Zero is no bound.
	//
	// An artifact of unknown size passes both. Some sources only reveal length
	// in a response header, so refusing on an absent fact would strand exactly
	// the jobs whose size nobody could have declared.
	MinSize int64 `json:"minSize,omitempty"`
	MaxSize int64 `json:"maxSize,omitempty"`

	// Digests are the digest algorithms this delegate can be asked to deliver,
	// spelled as they are in Artifact.Digest — "sha256". Empty is any.
	//
	// The algorithm, and never a set of particular artifacts. A cache's
	// contents change between one probe and the next, and this has always been
	// about possibility rather than inventory: a delegate that has not got the
	// bytes fails the job it was given, and a delegate that cannot hash them
	// should never have been given it.
	Digests []string `json:"digests,omitempty"`
}

// HostShape is the closed set of answers about the source hosts a delegate can
// reach.
type HostShape string

const (
	// HostAny places no constraint.
	HostAny HostShape = ""
	// HostRoutable refuses a source that names the machine asking. It is the
	// whole of the NAS's real rule: a box on the other end of a share cannot
	// fetch http://127.0.0.1, and a job that says so sat running forever.
	HostRoutable HostShape = "routable"
)

// Valid reports whether every term is one this package understands.
//
// An unknown term is a refusal to offer the tier at all, and that is the point
// of the check: reading a constraint we cannot evaluate as "no constraint"
// would hand a delegate precisely the jobs its author wrote the constraint to
// keep away from it. A closed vocabulary that fails open is not closed.
func (d Declaration) Valid() error {
	switch d.Hosts {
	case HostAny, HostRoutable:
	default:
		return fmt.Errorf("download: unknown host shape %q", d.Hosts)
	}
	if d.MinSize < 0 || d.MaxSize < 0 || (d.MaxSize > 0 && d.MinSize > d.MaxSize) {
		return fmt.Errorf("download: size bounds %d..%d hold nothing", d.MinSize, d.MaxSize)
	}
	for _, a := range d.Digests {
		if a == "" || strings.ContainsAny(a, ": \t") {
			return fmt.Errorf("download: %q is not a digest algorithm", a)
		}
	}
	return nil
}

// And is both declarations at once: the tightest bound on every axis.
//
// Two parties declare — the person who installed the delegate, in the file that
// installed it, and the delegate about itself, at probe. Neither may loosen the
// other, and combining them any other way would let one of them do exactly
// that: a delegate that could widen what a person wrote down has made the
// person's file decorative.
// It reports an error rather than a declaration nothing can satisfy. An empty
// list would read as "no constraint" — the field's own spelling for any — and
// silently serving everything is the one wrong answer to two parties who agree
// on nothing.
func (d Declaration) And(o Declaration) (Declaration, error) {
	if o.Hosts != HostAny {
		d.Hosts = o.Hosts
	}
	d.MinSize = max(d.MinSize, o.MinSize)
	if o.MaxSize > 0 && (d.MaxSize == 0 || o.MaxSize < d.MaxSize) {
		d.MaxSize = o.MaxSize
	}
	if len(d.Digests) == 0 {
		d.Digests = o.Digests
	} else if len(o.Digests) > 0 {
		var both []string
		for _, a := range d.Digests {
			if has(o.Digests, a) {
				both = append(both, a)
			}
		}
		if len(both) == 0 {
			return d, fmt.Errorf("download: %v and %v share no digest algorithm", d.Digests, o.Digests)
		}
		d.Digests = both
	}
	return d, d.Valid()
}

// permits reports whether the declaration lets this job through.
func (d Declaration) permits(spec Spec) bool {
	if size := spec.Artifact.Size; size > 0 {
		if d.MinSize > 0 && size < d.MinSize {
			return false
		}
		if d.MaxSize > 0 && size > d.MaxSize {
			return false
		}
	}
	if len(d.Digests) > 0 && spec.Artifact.Digest != "" {
		alg, _, _ := strings.Cut(spec.Artifact.Digest, ":")
		if !has(d.Digests, alg) {
			return false
		}
	}
	if d.Hosts == HostRoutable {
		for _, src := range spec.Sources {
			if ThisMachine(src.Locator) {
				return false
			}
		}
	}
	return true
}

// Declares is an OPTIONAL capability: a delegate that states what it could
// never do, rather than being asked about each job.
//
// Every out-of-process delegate implements this and not Selective.
type Declares interface {
	Declared() Declaration
}

// ThisMachine reports whether a locator names the machine asking: loopback, the
// unspecified address, or "localhost".
//
// Deliberately narrow. It says yes only to what is PROVABLY unreachable from
// anywhere else, and does not guess about private ranges or VPNs — a NAS on the
// same LAN reaches 192.168.x quite happily, and a delegate that refused work it
// could actually do would be a worse bug than the one this closes.
func ThisMachine(locator string) bool {
	u, err := url.Parse(locator)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsUnspecified()
	}
	return false
}

// A Refusal is one delegate declining one job, and why.
//
// A job nothing would take is not the same fact as a job nobody could have
// taken, and a person deciding whether to uninstall a plugin needs the
// difference. Without the name, a program somebody else wrote turning work away
// is indistinguishable from this layer having nothing installed.
type Refusal struct {
	System string `json:"system"`
	Why    string `json:"why"`
}

// ForSpec picks a delegate for a whole job, so one that can refuse gets to see
// what it is being offered rather than only the source's scheme.
func (d *Delegators) ForSpec(spec Spec, src Source, requires []string) (Delegator, bool) {
	if d == nil {
		return nil, false
	}
	for _, x := range d.all {
		if !d.eligible(x, spec, src, requires) {
			continue
		}
		return x, true
	}
	return nil, false
}

// WhyNot names the delegates that could have taken this source and turned it
// down. Asked only when nothing took the job, because that is the only moment
// the answer is worth anything.
func (d *Delegators) WhyNot(spec Spec, src Source, requires []string) []Refusal {
	if d == nil {
		return nil
	}
	var out []Refusal
	for _, x := range d.all {
		if !has(x.Schemes(), src.Scheme) {
			continue
		}
		if why := refuted(x, requires); why != "" {
			out = append(out, Refusal{x.System(), why})
			continue
		}
		if !hasAllCaps(believed(x), requires) {
			continue
		}
		if why := declined(x, spec); why != "" {
			out = append(out, Refusal{x.System(), why})
		}
	}
	return out
}

func (d *Delegators) eligible(x Delegator, spec Spec, src Source, requires []string) bool {
	return d.suits(x, src, requires) && declined(x, spec) == ""
}

func (d *Delegators) suits(x Delegator, src Source, requires []string) bool {
	return has(x.Schemes(), src.Scheme) && hasAllCaps(believed(x), requires)
}

// believed is what a delegate claims, less what it has been caught not doing.
func believed(x Delegator) []Capability {
	caps := x.Capabilities()
	out := make([]Capability, 0, len(caps))
	for _, c := range caps {
		if !disbelieves(x.System(), c) {
			out = append(out, c)
		}
	}
	return out
}

// refuted is why a delegate that could otherwise serve this job is not being
// offered it: a claim it made and its own behaviour destroyed.
//
// Said out loud rather than filtered out silently, because "no delegator would
// take it" and "the delegate you installed lied and is no longer trusted with
// this" are different problems and only one of them is the person's to act on.
func refuted(x Delegator, requires []string) string {
	for _, w := range requires {
		if disbelieves(x.System(), Capability(w)) {
			return "claimed " + w + " and was caught not doing it"
		}
	}
	return ""
}

// declined is the delegate's own answer about one job, and the empty string
// means it did not object.
//
// A delegate that declares is never asked, even if it also implements
// Selective: the declaration is the whole of what an out-of-process delegate
// may say, and a delegate that could answer both would make which one binds
// depend on where it happens to run.
func declined(x Delegator, spec Spec) string {
	if dec, ok := x.(Declares); ok {
		if dec.Declared().permits(spec) {
			return ""
		}
		return "declared it can never serve this job"
	}
	// The delegate's own veto, last, because it is the most specific thing
	// anybody knows about the job.
	if sel, ok := x.(Selective); ok && !sel.CanServe(spec) {
		return "refused this job"
	}
	return ""
}

// Close releases every delegate in the chain. A delegate may own a process.
func (d *Delegators) Close() error {
	if d == nil {
		return nil
	}
	var errs []error
	for _, x := range d.all {
		errs = append(errs, x.Close())
	}
	d.all = nil
	return errors.Join(errs...)
}

// Failure carries a failure's CLASS across a process boundary.
//
// download has exactly two endings and they are the whole retry model: a
// refusal ends the job, and anything else leaves it adoptable for the next
// runner to resume. Locally that is errors.Is; over a pipe it is nothing at
// all, because errors.Is does not survive JSON and a sentence is not a class.
// Any delegate outside this process has the problem, so the field is here
// rather than in each transport.
//
// The identity of the far side's error does not cross and is not meant to. What
// crosses is its class and its words, which is what a successor acts on.
type Failure struct {
	Text      string `json:"error"`
	Permanent bool   `json:"permanent,omitempty"`
}

// FailureOf describes an error for the wire. It returns nil for nil.
func FailureOf(err error) *Failure {
	if err == nil {
		return nil
	}
	return &Failure{Text: err.Error(), Permanent: Permanent(err)}
}

// Err rebuilds the error on this side, class intact.
func (f *Failure) Err() error {
	if f == nil || f.Text == "" {
		return nil
	}
	if f.Permanent {
		return permanent{errors.New(f.Text)}
	}
	return errors.New(f.Text)
}

// BySystem finds the delegator that can interpret a recorded handle. Without
// this, a job delegated by yesterday's process is unreachable today.
func (d *Delegators) BySystem(system string) (Delegator, bool) {
	if d == nil {
		return nil, false
	}
	for _, x := range d.all {
		if x.System() == system {
			return x, true
		}
	}
	return nil, false
}

func has(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func hasAllCaps(caps []Capability, requires []string) bool {
	if len(requires) == 0 {
		return true
	}
	have := make(map[string]bool, len(caps))
	for _, c := range caps {
		have[string(c)] = true
	}
	for _, w := range requires {
		if !have[w] {
			return false
		}
	}
	return true
}

// Delegate hands a job to an external system and records the handle.
//
// After this returns, the work is out of our hands and this process may exit.
// The handle in the record is the only thing that can find it again.
func (r *Runner) Delegate(ctx context.Context, id string) error {
	if r.Delegators == nil {
		return ErrNoDelegator
	}
	rec, err := r.Store.Claim(id, r.Owner, r.LeaseTTL)
	if err != nil {
		return err
	}
	epoch := rec.Lease.Epoch

	spec, err := SpecOf(rec)
	if err != nil {
		return err
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		return err
	}

	// Work already proven must not be handed to something that will start over.
	//
	// Measured, not assumed: a 16 MiB transfer interrupted with 6,586,368 bytes
	// checkpointed was delegated to BITS, which issued a GET with no Range header
	// and fetched the whole file again. The digest still matched, so nothing was
	// corrupt — it was simply thrown away, which on a 40 GB model over a slow
	// link is the entire complaint this project exists to answer.
	//
	// So a delegate that cannot resume is only eligible from zero. When none
	// qualifies, this returns ErrNoDelegator, the job stays unclaimed, and the
	// supervisor's own adoption pass runs it here — where the checkpoint IS
	// honoured. Slower than a NAS, and it keeps the bytes.
	requires := rec.Requires
	if cp.VerifiedPrefix > 0 {
		requires = append(append([]string{}, requires...), string(CapResume))
	}
	// Bytes this store has already proven are a local copy, and a delegate
	// would fetch them again from the network and hand them back across a share.
	if len(proven(r.Store, spec.Artifact.Digest, id)) > 0 {
		r.Store.Release(id, epoch)
		return ErrNoDelegator
	}

	// A delegate is a different process, sometimes a different account, and it
	// has no idea where our store lives. Hand it paths that are already real on
	// the machine it runs on.
	//
	// Resolved before any delegate is consulted, because this is the crossing
	// where the record's author and the writer's authority come apart: the
	// submitter chose the destination and the delegate does the writing. A sink
	// that leaves the store root is refused here and no delegate hears about the
	// job at all.
	sinkPartial, sinkFinal, err := LocalSink(r.Store, id, spec.Sink)
	if err != nil {
		r.Store.Release(id, epoch)
		return err
	}

	var refused []Refusal
	for _, src := range spec.Sources {
		// A delegate opens the socket in a process this policy cannot reach
		// into, so the question is asked before it is handed the work.
		if r.Reach.check(src.Locator) != nil {
			continue
		}
		one := spec
		one.Sources = []Source{src}
		// The whole job, not just the scheme, so a delegate that can tell it
		// could never do this one gets to say so before it is handed the work.
		d, ok := r.Delegators.ForSpec(one, src, requires)
		if !ok {
			refused = append(refused, r.Delegators.WhyNot(one, src, requires)...)
			continue
		}
		one.Sink.Partial, one.Sink.Final = sinkPartial, sinkFinal
		// The identity the delegate is asked about if this handoff's answer is
		// lost. It is the record's own id, so it is the same string on every
		// attempt at this job and legible to every process that can read the
		// store, in any language, after a reboot.
		one.Request = id

		// The handoff is written down BEFORE the work is handed over, and this
		// order is the whole fix.
		//
		// Start's answer crosses a share, a pipe or a process boundary, and an
		// answer lost after the far side committed is indistinguishable from a
		// refusal. Recording the handle only afterwards left a window in which a
		// transfer was running and no record said so: the loop fell back,
		// Delegate returned ErrNoDelegator, and the supervisor's own adopt pass
		// fetched the same artifact a second time in the same sweep. Measured —
		// see TestAcceptedThenLostIsNotTransferredTwice.
		//
		// Until the delegate's own handle replaces it, ExternalID IS the request
		// id, and that equality is what Reconcile reads as "unsettled". Only a
		// delegate that can be asked gets one: for any other, an unsettled
		// record is a job nothing could ever settle, which is worse than the
		// window it closes.
		_, canFind := d.(Locator)
		if canFind {
			if _, err := r.Store.Update(id, epoch, func(rr *job.Record) error {
				rr.Delegation = &job.Delegation{System: d.System(), ExternalID: id}
				rr.State = job.StateRunning
				return nil
			}); err != nil {
				r.Store.Release(id, epoch)
				return err
			}
		}

		extID, err := d.Start(ctx, one, cp.VerifiedPrefix)
		if err != nil {
			switch found, why := located(ctx, d, id); {
			case why != nil:
				// Nobody can say whether this delegate holds the work. The
				// record already says it might, so nothing here starts a second
				// transfer; Reconcile asks again on a later sweep.
				r.Store.Release(id, epoch)
				return fmt.Errorf("%w: %s: %v", ErrOutcomeUnknown, d.System(), why)
			case found == "":
				if canFind {
					if uerr := r.unhand(id, epoch); uerr != nil {
						r.Store.Release(id, epoch)
						return uerr
					}
				}
				continue
			default:
				extID = found
			}
		}
		if extID == id {
			// A delegate answering with the request it was given leaves this
			// record permanently unsettled, and would be polled for a handle
			// nothing on its side knows.
			stray := d.Abandon(ctx, extID)
			r.Store.Release(id, epoch)
			return errors.Join(fmt.Errorf("%w: %s returned the request id as its own handle",
				ErrClaimFalsified, d.System()), stray)
		}
		_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Delegation = &job.Delegation{System: d.System(), ExternalID: extID}
			rr.State = job.StateRunning
			return nil
		})
		if err != nil {
			// The handle could not be recorded, so nothing will ever find this
			// work again. Cancelling is better than leaking a transfer nobody
			// knows about — and a refused cancel IS the leak, so it travels
			// back too rather than letting this read as a tidy failure.
			return errors.Join(err, d.Abandon(ctx, extID))
		}
		// Let go of the lease: we are not doing the work, and holding it would
		// stop anyone else from polling and finalising.
		r.Store.Release(id, epoch)
		return nil
	}

	// Nothing here can take it, so let go of the lease claimed at the top.
	//
	// Holding it would make the job invisible to the very sweep that should run
	// it: Adopt only takes orphans, and a job leased by a supervisor that is not
	// working on it is not an orphan. The result was a livelock — delegate,
	// fail, hold, expire, delegate again — that never moved a byte. It was
	// unreachable while every delegator claimed it could resume, and became the
	// normal path the moment they stopped lying.
	r.Store.Release(id, epoch)
	if len(refused) > 0 {
		return fmt.Errorf("%w (%s)", ErrNoDelegator, refusedBy(refused))
	}
	return ErrNoDelegator
}

// located asks a delegate what it did with a request whose Start failed.
//
// A delegate that cannot be asked is taken at its word, which is what every
// delegate got before Locator existed. That is why the obligation is stated on
// Start: a delegate whose acceptance can outlive its answer and does not
// implement Locator has no way to stop this layer starting the work twice.
func located(ctx context.Context, d Delegator, request string) (string, error) {
	l, ok := d.(Locator)
	if !ok {
		return "", nil
	}
	return l.Locate(ctx, request)
}

// unhand takes back a handoff the delegate is certain it never accepted, so the
// job is ours again with its sources and checkpoint intact.
func (r *Runner) unhand(id string, epoch int64) error {
	_, err := r.Store.Update(id, epoch, func(rr *job.Record) error {
		rr.Delegation = nil
		rr.State = job.StatePending
		return nil
	})
	return err
}

// resolve settles a handoff whose answer was lost: the record names a delegate
// and carries the request in place of a handle, so nothing can be polled,
// finalised or cancelled until the delegate says which one it holds.
func (r *Runner) resolve(ctx context.Context, rec *job.Record, d Delegator) error {
	l, ok := d.(Locator)
	if !ok {
		// Every branch that could settle this needs a handle, and inventing
		// either answer is a second transfer or a lost one. See Locator, and
		// the obligation stated on Start.
		return fmt.Errorf("%w: %s cannot be asked what it did with request %s",
			ErrOutcomeUnknown, d.System(), rec.ID)
	}
	extID, err := l.Locate(ctx, rec.ID)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrOutcomeUnknown, d.System(), err)
	}
	claimed, err := r.Store.Claim(rec.ID, r.Owner, r.LeaseTTL)
	if err != nil {
		return err
	}
	epoch := claimed.Lease.Epoch
	if extID == "" {
		err = r.unhand(rec.ID, epoch)
	} else {
		_, err = r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
			rr.Delegation.ExternalID = extID
			return nil
		})
	}
	r.Store.Release(rec.ID, epoch)
	return err
}

func refusedBy(rs []Refusal) string {
	said := make([]string, 0, len(rs))
	for _, r := range rs {
		if !has(said, r.System+" "+r.Why) {
			said = append(said, r.System+" "+r.Why)
		}
	}
	return strings.Join(said, "; ")
}

// Reconcile asks the external system how a delegated job is doing and moves the
// record to match. Anyone may call it — the process that delegated the work is
// usually long gone, which is the entire point.
//
// It verifies the bytes itself before accepting them. BITS "guarantees that the
// version of the file it transfers is consistent based on the file size and time
// stamp, not content", so the delegate finishing is not evidence that the file
// is right.
func (r *Runner) Reconcile(ctx context.Context, id string) error {
	rec, err := r.Store.Load(id)
	if err != nil {
		return err
	}
	if !rec.Delegated() {
		return fmt.Errorf("%w: %s", ErrNotDelegated, id)
	}
	if rec.Delegation.ExternalID == rec.ID {
		// An unsettled handoff: the delegation was written before the work was
		// offered, and the delegate's own handle never came back. Every branch
		// below names that handle — poll it, finalise it, tell it to stop — so
		// settling which one it is, or that there is none, is the only thing
		// this sweep can do with the job.
		d, ok := r.Delegators.BySystem(rec.Delegation.System)
		if !ok {
			return r.stranded(rec)
		}
		return r.resolve(ctx, rec, d)
	}
	if rec.State.Terminal() {
		// A terminal job that still holds a delegation handle has work running
		// somewhere else that nobody will ever collect.
		//
		// This returned nil, and the cost was measured rather than imagined: a
		// pause that reached the record as a cancel left a NAS fetching 3.1 GB
		// for a job the local side had already given up on. It ran to
		// completion, and the finished file sat on the share as a transferred
		// job with no requester. The delegate was never told, because the only
		// code that would have told it stopped one line above.
		//
		// The interface has always had Abandon for exactly this — "a job left
		// unacknowledged sits in the BITS queue for 90 days" — and nothing
		// called it on this path.
		if rec.Delegation.Delivered {
			return nil
		}
		return r.abandonDelegated(ctx, rec)
	}

	// What somebody wants, before asking the delegate how it is getting on.
	//
	// This was missing entirely, and the gap was the worst possible shape: the
	// in-process runner honoured intent at every checkpoint, so pause and cancel
	// worked — on the tier that is used when nothing better is available. Every
	// job that went to BITS or a NAS ignored both. On Windows that is the normal
	// path, so the pause button worked exactly where it did not matter and did
	// nothing where it did.
	//
	// Found by asking whether any of this transfers to a real application, which
	// is not a question the unit tests were ever going to answer.
	if want := rec.Wants(); want != job.WantRun {
		return r.honourDelegated(ctx, rec, want)
	}

	// Already delivered. Polling again would ask about a job the delegate
	// destroyed on Finalize — BITS removes it from the queue — which reads back
	// as Gone, and Gone means "take the work back", so a second sweep would
	// re-download a file that is already correct at its final path.
	//
	// StateTransferred is deliberately not terminal (the consumer has yet to
	// acknowledge), so the terminal check above does not cover this. Delivered
	// is what distinguishes "the delegate finished" from "the delegate lost it".
	if rec.Delegation.Delivered || rec.State == job.StateTransferred {
		return nil
	}
	d, ok := r.Delegators.BySystem(rec.Delegation.System)
	if !ok {
		return r.stranded(rec)
	}

	st, err := d.Poll(ctx, rec.Delegation.ExternalID)
	if err != nil {
		return err
	}

	// The other half of pausing, and it was missing: something has to start it
	// again. The intent above says run, so if the delegate is still stopped
	// because it was asked to stop, ask it to carry on.
	//
	// This is why Status carries Suspended. To a supervisor deciding whether to
	// take work back, a suspended job is neither failed nor finished, so it maps
	// to Running and must — which leaves nothing to distinguish "paused" from
	// "going", and a resumed job sat still forever with every check passing.
	if st.Suspended {
		s, ok := d.(Suspendable)
		if !ok {
			return fmt.Errorf("%w: %s left a transfer suspended and cannot resume it",
				ErrNoDelegator, d.System())
		}
		if err := s.Resume(ctx, rec.Delegation.ExternalID); err != nil {
			return err
		}
		// Nothing else to do this sweep. The next poll sees it moving.
		return nil
	}

	// The delegate's own report, against the claim that got it this job.
	//
	// CapResume is the only capability in the roster that is Falsifiable
	// (fetcher.go): nothing proves it true, and a false one is invisible from
	// the file, because a delegate that fetches from zero delivers bytes whose
	// digest matches perfectly. Delegate hands a job with proven bytes only to
	// a delegate claiming it, and hands over the offset; progress below that
	// offset is that delegate fetching bytes this store had already proven.
	//
	// Nonzero, because a delegate that has not started reports zero and has not
	// lied yet. The first byte below the offset is the refutation, which costs
	// one poll interval and cannot accuse a delegate that is merely queued.
	cp, err := CheckpointOf(rec)
	if err != nil {
		return err
	}
	if st.Done > 0 && st.Done < cp.VerifiedPrefix {
		return r.restarted(ctx, rec, d, cp.VerifiedPrefix, st.Done)
	}

	// Claim only now: polling needs no lease, and taking one before we know
	// there is something to do would block whoever else is watching.
	claimed, err := r.Store.Claim(id, r.Owner, r.LeaseTTL)
	if err != nil {
		return err
	}
	epoch := claimed.Lease.Epoch

	switch st.State {
	case DelegateRunning:
		_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Progress.Done = st.Done
			if st.Total > 0 {
				rr.Progress.Total = st.Total
			}
			rr.Progress.UpdatedAt = job.At(time.Now())
			// The FIRST phase, which is the long one and was the one still
			// missing: steps were being set for the copy back and the verify,
			// so a job spent its whole download saying nothing and only started
			// reporting phases once the bytes were already fetched.
			rr.Progress.Step = &job.Step{
				Name:    "fetching on " + rec.Delegation.System,
				Ordinal: 1,
				Of:      3,
				Done:    st.Done,
				Total:   st.Total,
			}
			return nil
		})
		r.Store.Release(id, epoch)
		return err

	case DelegateFailed, DelegateGone:
		// Hand it back to ourselves. The sources are still in the spec, the
		// checkpoint still says what was proven, and an in-process Fetcher can
		// carry on — a delegate disappearing is a reason to do the work here,
		// not a reason to lose it.
		_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Delegation = nil
			rr.State = job.StatePending
			// A handle that evaporated is not an attempt at these bytes. Error
			// means "the last try at fetching this failed, and here is why",
			// and three things read it that way: a waiting caller ends on it,
			// `dl status` prints it, and the retry backoff counts it. A store
			// that was cleaned out has told us nothing about the download, so
			// writing there makes all three wrong to gain a line jobd already
			// prints from what Reconcile returns. A delegate that tried and
			// failed is the opposite, and keeps its reason.
			if st.Err != "" {
				return setFailure(rr, errors.New(st.Err))
			}
			return nil
		})
		r.Store.Release(id, epoch)
		return err

	case DelegateTransferred:
		spec, err := SpecOf(claimed)
		if err != nil {
			return err
		}
		// The delegate is about to be told where to put the bytes, so the same
		// refusal that guarded Delegate guards the finalise.
		_, final, err := LocalSink(r.Store, claimed.ID, spec.Sink)
		if err != nil {
			return err
		}

		// A delegated download is not one transfer, it is three phases, and only
		// the first was ever visible. Saying which one is happening is the
		// difference between "this finished ten minutes ago and is doing
		// nothing" and "this is copying 40 GB back across a share".
		const phases = 3

		// Both phases below are proportional to the size of the file — a copy
		// across a share, then a hash of every byte that arrived — and nothing
		// here renewed the lease claimed a few lines above. So the terminal
		// Update at the bottom was refused for any artifact that took longer
		// than LeaseTTL, thirty seconds by default, to bring across and verify.
		// The NAS proofs passed because 112 MB fits inside that window. A
		// multi-gigabyte model does not, and it failed quietly first: the steps
		// stopped landing and only the last write said anything.
		//
		// The keeper renews on a timer for as long as the work keeps reporting,
		// and stops renewing once it has been silent for the budget. The bound
		// is not optional — an owner that renewed on a bare timer would hold a
		// lease forever while a delegate held a connection and sent nothing.
		// keeper.go and docs/stall-detection.md have the argument.
		ctx, keep := r.keep(ctx, id, epoch)
		defer keep.stop()

		r.step(keep, &job.Step{
			Name:    "copying from " + rec.Delegation.System,
			Ordinal: 2,
			Of:      phases,
		})
		if rf, ok := d.(ReportingFinalizer); ok {
			err = rf.FinalizeReporting(ctx, rec.Delegation.ExternalID, final,
				func(done, total int64) {
					r.step(keep, &job.Step{
						Name:    "copying from " + rec.Delegation.System,
						Ordinal: 2,
						Of:      phases,
						Done:    done,
						Total:   total,
					})
				})
		} else {
			err = d.Finalize(ctx, rec.Delegation.ExternalID, final)
		}
		if err != nil {
			// A copy this owner's own watchdog cancelled surfaces as "context
			// canceled", which describes the mechanism and not the reason. The
			// reason is on the keeper, and it is the one a person needs.
			if ferr := keep.fenced(); ferr != nil {
				return fmt.Errorf("copying from %s: %w", rec.Delegation.System, ferr)
			}
			return err
		}

		// Now the file is ours, and now we check it — because the delegate
		// mostly did not, and even the one that did sent the bytes over a second
		// network to get here. Re-hashing gigabytes is not instant either, and
		// it was the second half of the silence.
		//
		// It is also the phase the silence budget must not stop. Hashing is
		// local CPU with nothing to report to any network and it legitimately
		// runs for minutes on a large model, so it reports: every chunk to the
		// watchdog, which costs nothing, and to the record no more often than a
		// checkpoint would have been written, because a person cannot read a
		// number that changes fifty times a second.
		r.step(keep, &job.Step{Name: "verifying", Ordinal: 3, Of: phases})
		said := time.Now()
		total, digest, err := hashFile(ctx, final, func(done, of int64) {
			keep.beat()
			if r.PersistInterval > 0 && time.Since(said) < r.PersistInterval {
				return
			}
			said = time.Now()
			r.step(keep, &job.Step{
				Name:    "verifying",
				Ordinal: 3,
				Of:      phases,
				Done:    done,
				Total:   of,
			})
		})
		if err != nil {
			if ferr := keep.fenced(); ferr != nil {
				return fmt.Errorf("verifying what %s delivered: %w", rec.Delegation.System, ferr)
			}
			return err
		}

		// The fence, before the two writes that are not idempotent. An owner
		// whose lease lapsed while it hashed must not tell the store this file
		// is delivered, and must not reset the checkpoint on a mismatch: a
		// successor may have claimed the job and be writing that path right now.
		//
		// Stopping rather than only checking, and stopping HERE rather than in
		// the deferred stop above: a renewal still in flight re-writes a record
		// it loaded moments ago, and would undo the terminal write below.
		if err := keep.stop(); err != nil {
			return err
		}
		if want := spec.Artifact.Digest; want != "" && !sameDigest(digest, want) {
			_, uerr := r.Store.Update(id, epoch, func(rr *job.Record) error {
				rr.Delegation = nil
				rr.State = job.StatePending
				if err := setFailure(rr, fmt.Errorf("%w: delegate delivered %s, want %s", ErrDigestMismatch, digest, want)); err != nil {
					return err
				}
				return setCheckpoint(rr, Checkpoint{})
			})
			if uerr != nil {
				return uerr
			}
			return fmt.Errorf("%w: delegate delivered %s, want %s", ErrDigestMismatch, digest, want)
		}
		_, err = r.Store.Update(id, epoch, func(rr *job.Record) error {
			rr.Progress.Done = total
			rr.Progress.UpdatedAt = job.At(time.Now())
			// Finished work is not on a step. Leaving the last one set would
			// leave a completed download reading "verifying" forever, which is
			// the same class of lie as a finished one reading "paused".
			rr.Progress.Step = nil
			rr.State = job.StateTransferred
			clearFailure(rr)
			rr.Delegation.Delivered = true
			return setCheckpoint(rr, Checkpoint{VerifiedPrefix: total})
		})
		return err
	}
	return fmt.Errorf("download: delegate reported unknown state %q", st.State)
}

// restarted is a delegate that claimed it could resume and began again from
// zero. It takes the job back, cancels the work being done twice, and stops
// believing that claim.
//
// The checkpoint is deliberately not touched: those bytes are the thing being
// rescued. The record goes back to pending with them intact, Adopt runs it here
// where the checkpoint IS honoured, and the delegate is no longer eligible for
// anything requiring resumption — so the next sweep does not hand it straight
// back. Slower than a NAS, and it keeps the bytes.
func (r *Runner) restarted(ctx context.Context, rec *job.Record, d Delegator, proven, done int64) error {
	system := rec.Delegation.System
	Disbelieve(system, CapResume)
	err := fmt.Errorf("%w: %s, published by %s, was given %d proven bytes and reports %d",
		ErrClaimFalsified, system, publisherOf(system), proven, done)

	claimed, cerr := r.Store.Claim(rec.ID, r.Owner, r.LeaseTTL)
	if cerr != nil {
		// Somebody else holds the lease and will poll it themselves; the
		// disbelief above is process-wide and already stands.
		return err
	}
	epoch := claimed.Lease.Epoch
	d.Abandon(ctx, rec.Delegation.ExternalID)
	if _, uerr := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
		rr.Delegation = nil
		rr.State = job.StatePending
		return setFailure(rr, err)
	}); uerr != nil {
		r.Store.Release(rec.ID, epoch)
		return uerr
	}
	r.Store.Release(rec.ID, epoch)
	return err
}

// stranded is a delegated record naming a system this process cannot speak to.
//
// Repeating the complaint on every sweep — which is what this used to do, for
// good — is the one answer that is certainly wrong. Such a record is either
// somebody else's business, in which case saying anything is noise, or work
// nobody here can advance, in which case it wants saying once and then leaving
// alone. It cannot be told from the record which of the two it is: a delegation
// handle carries the delegate's name and nothing about who created it.
//
// So the note goes on the RECORD, and only if it is not already there. That is
// what makes "once" mean once. The `said` map in the supervisor cannot: a
// supervisor installed as a scheduled task is a fresh process on every sweep
// and remembers nothing, which is precisely how the same line came to be
// written for good. The store is the only memory this design has ever had.
//
// It is also the other half of the defect. The only place this surfaced was a
// log line, so a job stuck this way said nothing about being stuck, and the
// record read exactly like one a delegate was busy on. An absent record means
// refusal here as everywhere else: a supervisor that cannot move a job must not
// leave it looking healthy.
//
// Nothing else changes, and deliberately. The state is left alone because the
// delegate may be holding every byte, so this is waiting rather than failure,
// and a process that DOES understand the handle — the same machine with that
// tier linked in, or the one that delegated it — must still be able to finish
// the job and clear the note. Adopt skips delegated records, so nothing here
// starts fetching the same bytes a second time either.
func (r *Runner) stranded(rec *job.Record) error {
	system := rec.Delegation.System
	err := fmt.Errorf("%w %q", ErrStrandedHere, system)

	// Which of the two problems a person has, because the next thing they do
	// differs: a tier this program has but that did not come up is something to
	// fix on this machine, and one it never had is a job for another process.
	why := "no such tier is registered in this program"
	if knownHere(system) {
		why = "that tier is registered here but did not come up"
	}
	note := fmt.Sprintf("waiting for a process that understands %q: %s", system, why)
	if rec.Error == note {
		return err
	}

	claimed, cerr := r.Store.Claim(rec.ID, r.Owner, r.LeaseTTL)
	if cerr != nil {
		// Somebody holds the lease, so somebody is working on it and this
		// process has nothing to add. Not a second failure to report.
		return err
	}
	epoch := claimed.Lease.Epoch
	_, uerr := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
		return setFailure(rr, errors.New(note))
	})
	r.Store.Release(rec.ID, epoch)
	if uerr != nil {
		return uerr
	}
	return err
}

// knownHere reports whether a tier by this name is registered in this program at
// all — linked in, or loaded from a manifest.
//
// The distinction RegisteredTiers was added for: "registered, but not available
// here" and "not registered at all" are very different problems and a user needs
// to tell them apart. It is asked of the program rather than of the runner on
// purpose — a runner may have been handed a shorter list by an operator running
// `--without bits`, and that is still a tier this build understands.
func knownHere(system string) bool {
	for _, name := range RegisteredTiers() {
		if name == system {
			return true
		}
	}
	return false
}

// ReconcileAll walks every delegated download and brings each up to date. This
// is what a service runs on a timer, and on start after a reboot.
func (r *Runner) ReconcileAll(ctx context.Context) (int, error) {
	all, unread, err := readable(r.Store.List())
	if err != nil {
		return 0, err
	}
	n := 0
	failed := []error{unread}
	for _, rec := range all {
		if rec.Kind != Kind || !rec.Delegated() {
			continue
		}
		// Nothing to catch up on, and asking would undo it — see Reconcile.
		//
		// A terminal job is NOT skipped merely for being terminal, only for
		// having been delivered. Skipping it made the abandon-on-terminal path
		// unreachable: Reconcile knew to tell the delegate, and this loop never
		// called it, so a cancelled job left a NAS fetching 3.1 GB to completion
		// for nobody. Terminal means "this side is finished with it", which is
		// exactly when the other side needs telling.
		if rec.Delegation.Delivered || rec.State == job.StateTransferred {
			continue
		}
		if err := r.Reconcile(ctx, rec.ID); err != nil {
			// A delegate this process cannot speak to is not a problem the
			// sweep can report its way out of, and it is not a problem that
			// changes between sweeps. Reconcile has already put the reason
			// where a person will find it — on the record — so raising it here
			// as well is the line that printed on every sweep for good.
			//
			// It is not counted either. A job nothing here can move was not
			// reconciled, and saying it was is the same lie as the healthy
			// `reconciled=2` below.
			if errors.Is(err, ErrStrandedHere) {
				continue
			}
			// One unreachable delegate must not stop the others -- but it must
			// not vanish either. This was a bare `continue`, and the silence
			// cost real time: a job that could not progress looked exactly like
			// a job nobody had to touch, so a supervisor printed a healthy
			// `reconciled=2` every five seconds while one transfer was stuck
			// and another was destroying its own bytes.
			failed = append(failed, fmt.Errorf("%s: %w", rec.ID, err))
			continue
		}
		n++
	}
	// The count AND what went wrong. A caller that only reads the count learns
	// nothing about the jobs that could not be reconciled, which is the state
	// worth knowing about.
	return n, errors.Join(failed...)
}

// DelegateAll offers every unclaimed job to the tiers this process has, and it
// is the second hop of the chain.
//
// Without it the chain stopped one link short. An application handed work to the
// supervisor, and the supervisor — which is the process that knows about BITS
// and a NAS — downloaded everything itself, because its sweep only reconciled
// what was already delegated and adopted what was stranded. Nothing ever asked
// "should this go somewhere better?". The NAS was configured, reachable,
// registered, and never used.
//
// Order matters in the sweep that calls this: reconcile, then delegate, then
// adopt. Delegating before adopting is what stops the supervisor grabbing a job
// it should have passed on; adopting last means anything nobody wanted still
// gets done here rather than sitting forever.
func (r *Runner) DelegateAll(ctx context.Context) (int, error) {
	if r.Delegators == nil || len(r.Delegators.all) == 0 {
		return 0, nil
	}
	candidates, unread, err := readable(r.Store.Orphans())
	if err != nil {
		return 0, err
	}
	n := 0
	for _, o := range candidates {
		if o.Kind != Kind || o.Delegated() {
			continue
		}
		// ErrNoDelegator is the ordinary answer for a job no registered tier can
		// serve — a file: source when only BITS is present, say — and it means
		// "leave it for Adopt", not "something went wrong".
		if err := r.Delegate(ctx, o.ID); err != nil {
			continue
		}
		n++
	}
	return n, unread
}

// Suspendable is an OPTIONAL capability of a Delegator: work it can stop and
// take up again without losing what it has.
//
// Optional because it is genuinely not universal. BITS has it natively — it
// already creates every job suspended and resumes it as a separate step. A
// delegate that cannot suspend has only one honest answer, and it is not to
// carry on quietly.
type Suspendable interface {
	Suspend(ctx context.Context, externalID string) error
	Resume(ctx context.Context, externalID string) error
}

// ReportingFinalizer is an OPTIONAL capability: a delegate whose Finalize does
// real work can say how that work is going.
//
// Most delegates finish instantly. BITS was told where to put the file when the
// job started and has been holding it there, so its Finalize is a call into the
// service and nothing more. A NAS is the opposite: the bytes are on the far side
// of a share and Finalize copies every one of them across, which for a 40 GB
// model is minutes of real transfer.
//
// Without this the record showed the delegate's numbers throughout that copy, so
// a person watched a download that said 100% and did nothing, twice over --
// once for the copy and again for the re-hash. The second transfer was real and
// entirely invisible.
//
// Optional rather than part of Delegator for the reason every capability here
// is: a delegate that finishes instantly has nothing to report and should not be
// made to implement a callback it would call once with the same number.
type ReportingFinalizer interface {
	// FinalizeReporting is Finalize, with progress. report may be called from
	// any goroutine and must be cheap; done and total are bytes, and total is 0
	// when the delegate cannot say.
	FinalizeReporting(ctx context.Context, externalID, dest string, report func(done, total int64)) error
}

// honourDelegated carries out what somebody asked for, on work this process is
// not performing.
//
// The rules are the job layer's, not this layer's invention: cancel must be
// honoured by everything, because stopping is universal; pause must be honoured
// only by implementations that advertise it, and one that cannot must fail the
// job with a stated reason rather than continue as though nobody had asked.
func (r *Runner) honourDelegated(ctx context.Context, rec *job.Record, want job.Want) error {
	d, ok := r.Delegators.BySystem(rec.Delegation.System)
	if !ok {
		// Nothing here understands that delegate's handle, so nothing here can
		// act on it. Some other machine's supervisor will.
		return nil
	}
	claimed, err := r.Store.Claim(rec.ID, r.Owner, r.LeaseTTL)
	if err != nil {
		return err
	}
	epoch := claimed.Lease.Epoch

	switch want {
	case job.WantCancel:
		// Abandon first, then record it. The other order can leave an external
		// transfer running with nothing pointing at it — BITS would keep the job
		// in its queue for 90 days, downloading a file nobody is waiting for.
		if err := d.Abandon(ctx, rec.Delegation.ExternalID); err != nil {
			return err
		}
		_, err := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
			rr.State = job.StateCancelled
			rr.Delegation = nil
			clearFailure(rr)
			return nil
		})
		return err

	case job.WantPause:
		s, canSuspend := d.(Suspendable)
		if !canSuspend {
			// The contract says say so rather than pretend. A pause that
			// silently does nothing is worse than a refusal, because a person
			// watching a progress bar keep moving has no way to tell which of
			// the two happened.
			_, err := r.Store.Update(rec.ID, epoch, func(rr *job.Record) error {
				return setFailure(rr, fmt.Errorf("%s cannot pause a transfer it has taken over", d.System()))
			})
			if err != nil {
				return err
			}
			r.Store.Release(rec.ID, epoch)
			return nil
		}
		if err := s.Suspend(ctx, rec.Delegation.ExternalID); err != nil {
			return err
		}
		// Let the lease go: paused means nobody is working on it, and Orphans
		// already excludes paused jobs so releasing does not invite a sweep to
		// start it again.
		r.Store.Release(rec.ID, epoch)
		return nil
	}
	r.Store.Release(rec.ID, epoch)
	return nil
}

// step records which phase this job is in, and doubles as this owner's proof
// that it still holds the lease.
//
// The write is still advisory: a step nobody could record must never fail the
// transfer it was only describing. But the ERROR is not advisory, and throwing
// it away is what hid the bug the keeper exists to fix. This was `_, _ =`, so
// once the lease lapsed under a long finalise the steps silently stopped
// landing and the first thing anyone heard about it was the terminal update
// failing, minutes later, with nothing to say which write had been the first to
// be refused.
//
// So a refusal for a stale epoch or a lapsed lease goes to the keeper, which
// remembers it and stops the work — that is the fence clause of the lease
// protocol, and this is one of the two places an owner learns it has been
// fenced. Anything else is ignored, as before.
func (r *Runner) step(k *keeper, st *job.Step) {
	_, err := r.Store.Update(k.id, k.epoch, func(rr *job.Record) error {
		rr.Progress.Step = st
		rr.Progress.UpdatedAt = job.At(time.Now())
		return nil
	})
	if err != nil {
		k.refused(err)
		return
	}
	// A phase that got as far as writing the record is a phase that moved.
	k.beat()
}

// abandonDelegated tells a delegate to stop work the local record has already
// given up on.
//
// Once a record is terminal it cannot be claimed, and so it cannot be updated to
// record that this was done — which is correct, because finished work is history
// and history does not change. The consequence is that this would run on every
// sweep forever, so a process remembers what it has abandoned. Forgetting across
// a restart is harmless: Abandon on a handle the delegate has already dropped is
// a no-op by contract.
func (r *Runner) abandonDelegated(ctx context.Context, rec *job.Record) error {
	if rec.Delegation == nil {
		return nil
	}
	handle := rec.Delegation.System + "\x00" + rec.Delegation.ExternalID
	if _, done := r.abandoned.Load(handle); done {
		return nil
	}
	d, ok := r.Delegators.BySystem(rec.Delegation.System)
	if !ok {
		// The tier is not registered in this process. Someone else's business,
		// and remembering would be a lie: a build that HAS that tier should
		// still get to abandon it.
		return nil
	}
	if err := d.Abandon(ctx, rec.Delegation.ExternalID); err != nil {
		return fmt.Errorf("download: abandoning %s on %s: %w", rec.ID, rec.Delegation.System, err)
	}
	r.abandoned.Store(handle, true)
	return nil
}
