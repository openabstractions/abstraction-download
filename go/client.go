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
// Applications get one from abstraction.Discover; a program that only
// downloads calls Open.
type Client interface {
	// Get fetches source to destination. If destination is a directory, the
	// name is taken from the source.
	//
	// Returns immediately with a handle. The work outlives this call, and may
	// outlive this process.
	Get(source, destination string) (Handle, error)

	// Submit is Get for a caller that knows more: a digest to verify against,
	// several sources, capabilities an implementation must have to qualify.
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

	// Where names what would answer, for a status line: "nas", "bits", "the
	// system downloader", "here". It is display text, never a branch — an
	// application can tell a person where their bytes are going without knowing
	// what any of those are.
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

// Options is everything a Client can be given. Exported so an adopter can hold
// an Option of its own.
type Options struct {
	// Storage is optional: without one the client always fetches, and the
	// caller must name a destination. See store.go.
	Storage storage.Store
}

// Option is one setting, applied by NewClient.
type Option func(*Options)

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
			return rec, fmt.Errorf("%s", rec.Error)
		case rec.State.Terminal():
			return rec, nil
		case rec.Error != "" && s.runner.Store.Claimable(rec):
			// An attempt that failed and let go of the job. Not terminal — the
			// partial is still there and a successor will resume it — but it is
			// the end of THIS request.
			return rec, fmt.Errorf("%s", rec.Error)
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
	if sup, live := SupervisorOf(s.runner.Store); live {
		if sup.Tier != "" {
			return sup.Tier
		}
		return "the system downloader"
	}
	return s.runner.Tier()
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
	dest := s.destinationOf(spec.Sink)
	if dest == "" {
		return ""
	}
	for _, src := range proven(s.runner.Store, spec.Artifact.Digest, "") {
		if canonicalPath(src.Locator) == dest {
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
// Compared normalised, never as stored. Portable fixes what THIS submission
// spells, and the record on disk was written by whatever wrote it — an older
// version of this layer, another language, an adopter that joined a native
// directory to a file name with a hardcoded `/`. Comparing the spellings makes
// two names for one file two pieces of work, which is the duplicate fetch this
// whole layer exists to stop.
func (s *client) inFlight(spec Spec) string {
	if len(spec.Sources) == 0 {
		return ""
	}
	all, err := s.runner.Store.List()
	if err != nil {
		return ""
	}
	want := comparablePath(spec.Sink.Final)
	for _, rec := range all {
		if rec.Kind != Kind || rec.State.Terminal() {
			continue
		}
		got, err := SpecOf(rec)
		if err != nil || comparablePath(got.Sink.Final) != want || len(got.Sources) == 0 {
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
	if sup, live := SupervisorOf(s.runner.Store); live && (!boundHere(spec) || onThisMachine(sup)) {
		// Ask it to look now rather than at its next sweep. Best effort: if the
		// nudge goes nowhere the sweep still finds the work.
		Nudge(s.runner.Store)
		return
	}
	go s.runHere(id)
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
		r.Error = ""
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
