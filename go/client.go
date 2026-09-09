package download

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
	storage "github.com/openabstractions/abstraction-storage/go"
)

// Client is downloading, for an application that holds no store, no runner and
// no opinion about who does the work.
//
// Submit, and who executes is settled below this line. If a supervisor is
// watching this machine's store, it takes the work and this process may exit.
// If not, this process does it — and if it exits mid-transfer, the record and
// the partial are durable, the lease lapses, and the next supervisor or the
// next launch adopts it.
//
// An application that does have an opinion says so with WithExecution, and it
// is still the same store: see Execution.
//
// Applications get one from abstraction.Discover; a program that only
// downloads calls Open.
type Client interface {
	// Get fetches source to destination. If destination is a directory, the
	// name is taken from the source.
	//
	// destination is a shell path on THIS machine: a relative one means the
	// caller's working directory, as `dl -o out.bin` has always meant, and it is
	// resolved here rather than carried into the record. A caller that wants a
	// destination another machine can resolve names a store-relative one and
	// calls Submit — the two methods take different things and Sink says which.
	//
	// Returns immediately with a handle. The work outlives this call, and may
	// outlive this process.
	Get(source, destination string) (Handle, error)

	// Submit is Get for a caller that knows more: a digest to verify against,
	// several sources, capabilities an implementation must have to qualify.
	//
	// Its sink is a record's, not a shell's: a relative path is under the store
	// root, on whichever machine picks the job up. See Sink.
	Submit(spec Spec, requires ...string) (Handle, error)

	// Resume is downloading identified by destination rather than by job id,
	// for a program that is run again from a shell and has no id to remember.
	// See resume.go.
	Resume

	// Jobs is a live collection of every download on this machine — including
	// ones that were in flight before this process started, and ones some other
	// program submitted. Bind a UI to it.
	Jobs() job.Subscription

	// Open is a handle to one job by id, for an application that stored the id
	// and came back later.
	Open(id string) Handle

	// Where names who would take a job submitted now, for a status line: "nas",
	// "bits", "the system downloader", "here", "nobody". It is display text,
	// never a branch — an application can tell a person where their bytes are
	// going without knowing what any of those are.
	//
	// It is the same rule that dispatches, asked about a job with no
	// requirements and a sink anybody could resolve. A job that requires
	// something, or names a path only this machine has, may go elsewhere: the
	// record is what answers for THAT job, and Performer reads it.
	Where() string

	// Deliver waits until the bytes are here, then takes delivery of them.
	//
	// The synchronous case, which is most adopters: ComfyUI-Manager calls
	// download_url and expects the file to exist when it returns. Without this
	// every one of them writes the same wait loop and then forgets the second
	// half, and the store fills with finished transfers nobody collected.
	Deliver(ctx context.Context, id string) (*job.Record, error)

	// TakeDelivery is the requester saying "I have it", and it is the second
	// half of a two-phase completion rather than bookkeeping.
	//
	// A transferred job is finished and proven but not collected. Without this
	// call it waits in the store forever for somebody who never comes: BITS
	// holds such a job for 90 days and every list of downloads fills with
	// transfers that ended days ago.
	TakeDelivery(id string) error
}

// Execution is who performs the work this client submits, and it is a separate
// question from which store the client holds.
//
// Separate because selecting who performs the work must not select which work
// exists. Derive the executor from the store instead, and the only way to say
// "do this here" is to point at a store nobody is watching — which silently
// takes away every job the application could see, including the ones some other
// program on this machine submitted.
//
// The ancestor is Guava's directExecutor and Celery's task_always_eager: a
// dispatching client told to run the work in the calling process, without
// becoming a different client.
//
// It binds only what THIS process does at submission. A record carries no
// executor, so a supervisor sweeping a shared store still adopts the job as an
// orphan once the submitter dies. Making the choice durable needs a field on
// the record, which is a contract change in three languages:
// feedback/2026-09-08-vis23.md.
type Execution string

const (
	// ExecuteAnywhere is discovery: a supervisor if one is watching, this
	// process otherwise. The default, and what every existing caller gets.
	ExecuteAnywhere Execution = ""
	// ExecuteHere works the job in this process and never hands it away.
	ExecuteHere Execution = "here"
	// ExecuteDelegated demands a supervisor, and Submit refuses with
	// ErrNoDelegator when none is watching. Refusing is the point: silently
	// running here instead is the collapse this option exists to prevent.
	ExecuteDelegated Execution = "delegated"
)

// Options is everything a Client can be given. Exported so an adopter can hold
// an Option of its own.
type Options struct {
	// Storage is optional: without one the client always fetches, and the
	// caller must name a destination. See store.go.
	Storage storage.Store

	// Execution is who performs the work. Zero is discovery.
	Execution Execution
}

// Option is one setting, applied by NewClient.
type Option func(*Options)

// WithExecution names who performs the work, for an embedder, a test, or a
// maintainer who will not accept their program's behaviour changing because
// something got installed. Two clients over one store are two executors and one
// list of downloads.
func WithExecution(e Execution) Option {
	return func(o *Options) { o.Execution = e }
}

// NewClient wraps a runner and its store. Applications get one from the
// abstraction root rather than building it; this exists for the supervisor and
// for tests, which legitimately work a layer down.
func NewClient(r *Runner, opts ...Option) Client {
	s := &client{runner: r}
	for _, o := range opts {
		o(&s.opts)
	}
	return s
}

type client struct {
	runner *Runner
	opts   Options
}

func (s *client) Open(id string) Handle {
	j := job.Open(s.runner.Store, id, Owner())
	if p, ok := j.(job.Pausable); ok {
		return pausableHandle{handle{Job: j, client: s}, p}
	}
	return handle{Job: j, client: s}
}

func (s *client) TakeDelivery(id string) error { return s.runner.TakeDelivery(id) }

func (s *client) Deliver(ctx context.Context, id string) (*job.Record, error) {
	sub := job.WatchQuiet(s.runner.Store, Kind, 2*s.runner.LeaseTTL)
	defer sub.Close()
	for {
		n, err := sub.Next(ctx)
		if err != nil {
			rec, _ := s.runner.Store.Load(id)
			return rec, err
		}
		rec, err := s.runner.Store.Load(id)
		if err != nil {
			return nil, err
		}
		switch {
		case rec.State == job.StateTransferred:
			if err := s.TakeDelivery(id); err != nil {
				return rec, err
			}
			return s.runner.Store.Load(id)
		case rec.State == job.StateFailed:
			return rec, LastFailure(rec)
		case rec.State.Terminal():
			return rec, nil
		case rec.Error != "" && s.runner.Store.Claimable(rec):
			// An attempt that failed and let go of the job. Not terminal — the
			// partial is still there and a successor will resume it — but it is
			// the end of THIS request.
			return rec, LastFailure(rec)
		}
		if n.Quiet && s.unattended(rec) {
			// Nobody holds a lease, nobody is delegated to, and the record has
			// not moved for longer than it takes a lease to lapse twice. That
			// is not a slow download; it is a download nobody is performing,
			// and it is what a supervisor dying after the nudge, or a runner
			// that never won the claim, leaves behind. Waiting on it is the
			// hang this method exists to make impossible.
			return rec, fmt.Errorf("download: %s is %s and nobody is working on it", id, rec.State)
		}
	}
}

// unattended reports that nothing is working this job right now.
//
// A delegated job holds no lease here and never will, so "claimable" says yes
// about a transfer a NAS is actively performing. What answers for it is the
// supervisor: it is the thing that reconciles the delegate into the record, and
// if it is gone then so is every report the far side would ever have made.
func (s *client) unattended(rec *job.Record) bool {
	if rec.Delegated() {
		_, live := SupervisorOf(s.runner.Store)
		return !live
	}
	return s.runner.Store.Claimable(rec)
}

// Open is discovery plus a client, for a program that only downloads.
//
// It also hands back the job store, as the job.Store interface and never the
// binding, for programs inside this layer — the reference CLI, the supervisor —
// that read and render records. Applications above this layer call
// abstraction.Discover instead and are handed no store at all.
func Open() (Client, job.Store, error) {
	r, err := Discover()
	if err != nil {
		return nil, nil, err
	}
	// Whatever content-addressed stores this machine has, so a download that is
	// already on the disk becomes a local copy instead of a transfer.
	return NewClient(r, WithStorage(storage.New(storage.Discover()...))), r.Store, nil
}

func (s *client) Jobs() job.Subscription { return job.Watch(s.runner.Store, Kind) }

func (s *client) Where() string {
	name, _ := s.performer(Spec{})
	return name
}

// performer names who would work this job and whether this process does it, and
// it is the ONLY rule that decides either.
//
// One rule because the answer is published. A delegate is reached only through
// a supervisor — begin nudges one or calls runHere, and runHere goes to
// Fetchers and never to Delegators — so on a machine with nothing watching the
// store, the delegation chain's head is a tier the work will never touch. Two
// rules answering this drifted apart and named that tier in seven of ten runs
// while it performed two, and it is the string an application shows a person to
// tell them their download survives closing the app.
func (s *client) performer(spec Spec) (name string, runsHere bool) {
	if s.opts.Execution == ExecuteHere {
		return "here", true
	}
	sup, live := SupervisorOf(s.runner.Store)
	if s.opts.Execution == ExecuteDelegated {
		if !live {
			return "nobody", false
		}
		return supervising(sup), false
	}
	if !live || (boundHere(spec) && !onThisMachine(sup)) {
		return "here", true
	}
	return supervising(sup), false
}

func supervising(sup Supervisor) string {
	if sup.Tier != "" {
		return sup.Tier
	}
	return "the system downloader"
}

// Performer names who is working a job, read from the record that describes it
// rather than predicted from the machine's configuration.
//
// The prediction and the fact are different questions and only this one has an
// answer that cannot be wrong. It is published because every application needs
// it: the first built outside this tree wrote it by hand against Delegation and
// Lease before it could print a status line at all.
//
// A lapsed lease names nobody. An owner that was killed does not release, which
// is the whole design — so Lease.Owner outlives the process it names, and the
// hand-written version printed a program that had exited minutes earlier. What
// says whether that owner still answers for the job is the lease's own expiry,
// which is on the record beside the name.
func Performer(rec *job.Record) string {
	switch {
	case rec == nil:
		return "nobody"
	case rec.Delegation != nil && rec.Delegation.System != "":
		return rec.Delegation.System
	case rec.Lease.Held(time.Now()):
		return rec.Lease.Owner
	}
	return "nobody"
}

func (s *client) Get(source, destination string) (Handle, error) {
	spec, err := specFor(source, destination)
	if err != nil {
		return nil, err
	}
	return s.Submit(spec)
}

// specFor turns the two strings a person types into a spec.
//
// Shared with ResumeOrGet, which needs the destination BEFORE it submits
// anything: it has to know which file is being asked for in order to find the
// record that is already downloading it.
func specFor(source, destination string) (Spec, error) {
	if strings.TrimSpace(source) == "" {
		return Spec{}, fmt.Errorf("download: no source")
	}
	dest := destination
	if dest == "" {
		dest = "."
	}
	if isDirectory(dest) {
		dest = filepath.Join(dest, nameFrom(source))
	}
	// Absolute, because the process that finally moves these bytes may have a
	// different working directory, a different user, or be on another machine.
	abs, err := filepath.Abs(dest)
	if err != nil {
		return Spec{}, err
	}
	return Spec{
		Sources: []Source{{Scheme: schemeFrom(source), Locator: source}},
		Sink:    Sink{Final: abs},
	}, nil
}

func (s *client) Submit(spec Spec, requires ...string) (Handle, error) {
	if err := s.noExecutor(); err != nil {
		return nil, err
	}
	if err := s.unknownCapability(requires); err != nil {
		return nil, err
	}
	// Where the bytes land, when the caller did not say. Only possible for a
	// caller that knows the digest, because the store is addressed by content —
	// which is why Get, which has only a URL, still names a path.
	spec, err := s.intoStorage(spec)
	if err != nil {
		return nil, err
	}
	// Bytes already on this machine go in front of the network. This is where
	// 116 GB across four stores stops being four copies.
	spec = s.alreadyHere(spec)
	// And bytes already AT the destination are not work at all. With a digest
	// there is no "download it again": the bytes are the identity, and the
	// finished record is the proof that they are there.
	if id := s.delivered(spec); id != "" {
		return s.Open(id), nil
	}
	if err := s.unservable(spec, requires); err != nil {
		return nil, err
	}

	// Asking twice for the same thing is one piece of work, not two.
	//
	// Without this, running the same command again starts a SECOND transfer of
	// the same artifact, with its own partial beginning at zero, racing the
	// first one to the same destination. That is not a hypothetical: the obvious
	// way to resume an interrupted download is to repeat the command, and it was
	// the first thing tried.
	//
	// Only work still in flight matches. A job that completed, failed or was
	// cancelled must not block a fresh attempt — "download it again" is a real
	// request, and the finished record is history rather than a claim on the
	// destination.
	if existing := s.inFlight(spec); existing != "" {
		// Nudge it along: if its owner died, this is what gets it picked up.
		s.begin(existing, spec)
		return s.Open(existing), nil
	}
	id, err2 := Submit(s.runner.Store, spec, requires...)
	if err2 != nil {
		return nil, err2
	}
	s.begin(id, spec)
	return s.Open(id), nil
}

// delivered is the finished record that already put these exact bytes at this
// destination, or "".
func (s *client) delivered(spec Spec) string {
	_, final, _ := LocalSink(s.runner.Store, "", spec.Sink)
	if strings.TrimSpace(final) == "" {
		return ""
	}
	for _, src := range proven(s.runner.Store, spec.Artifact.Digest, "") {
		if SamePath(src.Locator, final) {
			return src.Attrs["job"]
		}
	}
	return ""
}

// inFlight returns the id of unfinished work for the same artifact and
// destination, or "".
//
// Identity is the destination plus the source. Not the digest — a caller often
// does not know one, which is exactly the case that needs this most.
//
// The destination is compared through SamePath, which asks the volume. The
// record on disk was written by whatever wrote it — an older version of this
// layer, another language, an adopter that joined a native directory to a file
// name with a hardcoded `/` — so comparing the spellings makes two names for
// one file two pieces of work, which is the duplicate fetch this whole layer
// exists to stop.
func (s *client) inFlight(spec Spec) string {
	if len(spec.Sources) == 0 {
		return ""
	}
	// The unreadable ids are dropped, and that is a hole this signature cannot
	// carry: a record nobody can decode may be the very transfer this call is
	// looking for, and the answer "" starts a second download of the same file.
	// Dropping them is nonetheless strictly better than what returning on the
	// error did, which was to go blind to EVERY record because of one.
	all, _, err := readable(s.runner.Store.List())
	if err != nil {
		return ""
	}
	for _, rec := range all {
		if rec.Kind != Kind || rec.State.Terminal() {
			continue
		}
		got, err := SpecOf(rec)
		if err != nil || len(got.Sources) == 0 || !SamePath(got.Sink.Final, spec.Sink.Final) {
			continue
		}
		if got.Sources[0].Locator == spec.Sources[0].Locator {
			return rec.ID
		}
	}
	return ""
}

// begin decides who works the job.
//
// The in-process case is fire and forget on purpose: there is no error to return
// to a caller that has already been handed a durable handle, and any failure
// belongs on the record, where a different process — or this one after a restart
// — can still see it. A returned error would be visible only to whoever happened
// to still be running, which is the audience that does not need telling.
func (s *client) begin(id string, spec Spec) {
	s.clearLastError(id)
	if _, here := s.performer(spec); here {
		go s.runHere(id)
		return
	}
	// Ask the supervisor to look now rather than at its next sweep. Best effort:
	// if the nudge goes nowhere the sweep still finds the work.
	Nudge(s.runner.Store)
}

// unknownCapability refuses a word nothing here has ever heard of.
//
// A capability only ever GRANTS, so an unknown one matches nothing, and a job
// carrying one is refused later for whatever reason the matcher reports
// instead: a caller who typed "survives_process_exits" was told their URL
// scheme was the problem. Accepting a word and ignoring it is the failure this
// layer forbids of a binding.
//
// The roster is not the whole answer. Assured deliberately ignores a word it
// cannot read so that a newer plugin talking to an older core simply gets less,
// and a plugin may promise something the core has never named. So a word is a
// typo only when nothing registered in this process claims it either.
func (s *client) unknownCapability(requires []string) error {
	for _, w := range requires {
		if _, ok := Assured(Capability(w)); ok {
			continue
		}
		if s.claimedHere(Capability(w)) {
			continue
		}
		return fmt.Errorf("%w: %q is not a capability; this core has %s",
			ErrNoFetcher, w, capabilityNames())
	}
	return nil
}

// claimedHere reports whether anything registered in this process promises c,
// whatever the core's own roster knows.
func (s *client) claimedHere(c Capability) bool {
	claims := func(caps []Capability) bool {
		for _, x := range caps {
			if x == c {
				return true
			}
		}
		return false
	}
	if s.runner.Fetchers != nil {
		for _, f := range s.runner.Fetchers.fetchers {
			if claims(f.Capabilities()) {
				return true
			}
		}
	}
	if s.runner.Delegators != nil {
		for _, x := range s.runner.Delegators.all {
			if claims(x.Capabilities()) {
				return true
			}
		}
	}
	return false
}

func capabilityNames() string {
	all := AllCapabilities()
	names := make([]string, len(all))
	for i, c := range all {
		names[i] = string(c)
	}
	return strings.Join(names, ", ")
}

// unservable refuses, before a record exists, a job nothing on this machine
// would perform — and says which of the two reasons it is.
//
// Accepting it and failing it later leaves a record in the store forever for a
// guarantee nobody could have kept. Four of the first outsider's ten records
// were that, and the store is what an application lists to show a person their
// downloads.
//
// The two reasons are the two endings. Nothing here promising the capability at
// all is forever: asking again unchanged cannot help. A delegate promising it
// with nothing watching the store is not now, because a delegate is reached
// only through a supervisor and starting one serves the identical request — the
// same application, machine and request is refused or served depending on
// whether a second process happens to be running, and the caller is entitled to
// know that is what happened.
func (s *client) unservable(spec Spec, requires []string) error {
	if len(requires) == 0 || s.runner.Fetchers == nil {
		return nil
	}
	_, runsHere := s.performer(spec)
	couldDelegate := ""
	for _, src := range spec.Sources {
		if _, ok := s.runner.Fetchers.For(src, requires); ok {
			return nil
		}
		one := spec
		one.Sources = []Source{src}
		if d, ok := s.runner.Delegators.ForSpec(one, src, requires); ok {
			if !runsHere {
				return nil
			}
			couldDelegate = d.System()
		}
	}
	wanted := strings.Join(requires, " and ")
	if couldDelegate != "" {
		return fmt.Errorf("%w: %s promises %s and nothing is watching this store, so nothing would hand it over",
			ErrNoDelegator, couldDelegate, wanted)
	}
	return fmt.Errorf("%w: nothing registered promises %s", ErrNoFetcher, wanted)
}

// noExecutor refuses a submission whose executor the caller demanded and this
// machine does not have.
//
// Asked before a record is written, so a demand nobody can serve leaves nothing
// behind for a sweep to find later and perform anyway.
func (s *client) noExecutor() error {
	if s.opts.Execution != ExecuteDelegated {
		return nil
	}
	if _, live := SupervisorOf(s.runner.Store); live {
		return nil
	}
	return fmt.Errorf("%w: nothing is watching this store", ErrNoDelegator)
}

// clearLastError makes the record answer "what is happening now" again.
//
// The error string outlives the attempt that wrote it, deliberately: a person
// reading the job later should not have to find the log of a process that no
// longer exists. But it is wrong the moment somebody tries again — a waiting
// requester would be handed the old failure before the new attempt had made a
// single request, and a UI would show an error for a job that is progressing.
//
// Best effort. A refused claim means somebody else is working on it, and their
// outcome is the current one.
func (s *client) clearLastError(id string) {
	rec, err := s.runner.Store.Load(id)
	if err != nil || rec.Error == "" {
		return
	}
	held, err := s.runner.Store.Claim(id, s.runner.Owner, s.runner.LeaseTTL)
	if err != nil {
		return
	}
	s.runner.Store.Update(id, held.Lease.Epoch, func(r *job.Record) error {
		clearFailure(r)
		return nil
	})
	s.runner.Store.Release(id, held.Lease.Epoch)
}

// boundHere reports whether the sink names a path only this machine has.
//
// A relative sink resolves against whichever store adopts the job, so any
// machine watching can finish it. An ABSOLUTE one names this filesystem, and a
// supervisor on a NAS handed that job would write to a directory that exists
// here and not there — the bytes land somewhere useless, or nowhere, and the
// application waits for a file that was never coming.
//
// So the fence is here, at the moment of deciding who works: a job nobody else
// could deliver is not offered to anybody else. It is not the whole fence. A
// supervisor sweeping a shared store still finds this job as an orphan if this
// process dies mid-transfer, and nothing in the record tells it not to. That
// wants a spec that can say "this sink is local to the submitter", which is a
// contract change and is written up in feedback/2026-09-05-python-service.md.
func boundHere(spec Spec) bool { return !relativeEverywhere(spec.Sink.Final) }

// onThisMachine reports whether a supervisor shares this filesystem, which is
// what makes it able to deliver an absolute sink after all.
//
// Without this the fence was drawn one step too wide: a supervisor running HERE
// was refused work it could obviously finish, and the submitting process ran it
// itself instead — two owners offering to do the same job, and the one holding
// the lease was the one nobody was watching. A supervisor that does not name a
// host is treated as elsewhere, because the question is being answered about
// somebody else's process and a missing answer is not a yes.
func onThisMachine(sup Supervisor) bool {
	host, err := os.Hostname()
	return err == nil && sup.Host != "" && strings.EqualFold(sup.Host, host)
}

// runHere works the job in this process, waiting out a dead owner's lease.
//
// The waiting is the point. A process killed mid-transfer does not release its
// lease — that is the whole design, and it is why a successor cannot simply
// barge in. But it means the obvious way to resume, running the same command
// again, arrives INSIDE the previous owner's lease window and is refused.
//
// Before this, that refusal went nowhere: begin launched a goroutine, the claim
// failed, the goroutine returned, and the command sat waiting for a transfer
// that nobody had started. It looked exactly like a hang, and it is the first
// thing a person does after killing a download.
//
// So: retry until the lease lapses. It will, within LeaseTTL, because the owner
// is gone. Anything else — the job finishing, someone else adopting it, a real
// error — stops the loop, and the record is where the outcome lives either way.
func (s *client) runHere(id string) {
	ctx := context.Background()
	deadline := time.Now().Add(2*s.runner.LeaseTTL + 5*time.Second)
	for {
		err := s.runner.Run(ctx, id)
		if err == nil || !errors.Is(err, job.ErrLeaseHeld) {
			return
		}
		if time.Now().After(deadline) {
			// Somebody else genuinely holds it and is renewing. That is not a
			// failure: they are doing the work, and this process was only ever
			// offering to.
			return
		}
		rec, err := s.runner.Store.Load(id)
		if err != nil || rec.State.Terminal() || rec.State == job.StateTransferred {
			return
		}
		time.Sleep(time.Second)
	}
}

func isDirectory(p string) bool {
	if strings.HasSuffix(p, "/") || strings.HasSuffix(p, `\`) || p == "." {
		return true
	}
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

func nameFrom(locator string) string {
	if i := strings.Index(locator, "?"); i >= 0 {
		locator = locator[:i]
	}
	name := path.Base(locator)
	if name == "" || name == "/" || name == "." {
		return "download.bin"
	}
	return name
}

func schemeFrom(locator string) string {
	if i := strings.Index(locator, "://"); i > 0 {
		return locator[:i]
	}
	return "https"
}
