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

	job "github.com/ReinisLusis/abstraction-job"
	storage "github.com/ReinisLusis/abstraction-storage"
)

// Service is downloading, for an application that holds no store, no runner and
// no opinion about who does the work.
//
// # What this deletes, and why it had to go
//
// The integration used to be three concepts and a branch:
//
//	r, _ := download.Discover()              // application holds a Runner
//	id, _ := download.Submit(r.Store, spec)  // ...and a *job.FileStore
//	switch r.Handoff() { case RunHere: ... } // ...and branches on the answer
//
// Every line of that is the application being handed a decision the abstraction
// exists to make. Handoff in particular was documented as "the only question an
// application gets to ask" three lines above a doc comment claiming applications
// branch on none of it. Nobody calls LoggerFactory.discover(), and nobody asks a
// logger whether an appender is attached before deciding whether to log.
//
// So: submit, and who executes is settled below this line. If a supervisor is
// watching, it takes the work and this process may exit. If not, this process
// does it — and if this process then exits mid-transfer, that is not a bug. The
// record and the partial are durable, the lease lapses, and the next supervisor
// or the next launch adopts it. That case is the reason the project exists.
type Service interface {
	// Get fetches source to destination. If destination is a directory, the
	// name is taken from the source.
	//
	// Returns immediately with a handle. The work outlives this call, and may
	// outlive this process.
	Get(source, destination string) (job.Job, error)

	// Submit is Get for a caller that knows more: a digest to verify against,
	// several sources, capabilities an implementation must have to qualify.
	Submit(spec Spec, requires ...string) (job.Job, error)

	// Jobs is a live collection of every download on this machine — including
	// ones that were in flight before this process started, and ones some other
	// program submitted. Bind a UI to it.
	Jobs() job.Subscription

	// Open is a handle to one job by id, for an application that stored the id
	// and came back later.
	Open(id string) job.Job

	// Where names what would answer, for a status line: "nas", "bits", "the
	// system downloader", "here". It is display text, never a branch — an
	// application can tell a person where their bytes are going without knowing
	// what any of those are.
	Where() string

	// TakeDelivery is the requester saying "I have it", and it is the second
	// half of a two-phase completion rather than bookkeeping.
	//
	// A transferred job is finished and proven but not collected. Without this
	// call it waits in the store forever for somebody who never comes: BITS
	// holds such a job for 90 days and every list of downloads fills with
	// transfers that ended days ago.
	TakeDelivery(id string) error
}

// NewService wraps a runner and its store. Applications get one from the
// abstraction root rather than building it; this exists for the supervisor and
// for tests, which legitimately work a layer down.
func NewService(r *Runner, opts ...func(*service)) Service {
	s := &service{runner: r}
	for _, o := range opts {
		o(s)
	}
	return s
}

type service struct {
	runner *Runner
	// store is optional: without one the service always fetches, and the caller
	// must name a destination. See store.go.
	store storage.Store
}

func (s *service) Open(id string) job.Job { return job.Open(s.runner.Store, id, Owner()) }

func (s *service) TakeDelivery(id string) error { return s.runner.TakeDelivery(id) }

// Open is discovery plus a service, for a program that only downloads.
//
// It hands back the store as well, and that is deliberate rather than sloppy:
// the callers are programs INSIDE this layer — the reference CLI, the supervisor
// — which read and render records rather than only submitting work. What they
// get is the job.Store INTERFACE, never the binding, and that is the whole
// difference from what this used to be. A caller can list and watch; it cannot
// learn whether there is a directory behind it.
//
// Applications above this layer call abstraction.Discover instead and are handed
// no store at all.
func Open() (Service, job.Store, error) {
	r, err := Discover()
	if err != nil {
		return nil, nil, err
	}
	// Whatever content-addressed stores this machine has, so a download that is
	// already on the disk becomes a local copy instead of a transfer.
	return NewService(r, WithStorage(storage.New(storage.Discover()...))), r.Store, nil
}

func (s *service) Jobs() job.Subscription { return job.Watch(s.runner.Store, Kind) }

func (s *service) Where() string {
	if sup, live := SupervisorOf(s.runner.Store); live {
		if sup.Tier != "" {
			return sup.Tier
		}
		return "the system downloader"
	}
	return s.runner.Tier()
}

func (s *service) Get(source, destination string) (job.Job, error) {
	if strings.TrimSpace(source) == "" {
		return nil, fmt.Errorf("download: no source")
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
		return nil, err
	}
	return s.Submit(Spec{
		Sources: []Source{{Scheme: schemeFrom(source), Locator: source}},
		Sink:    Sink{Final: abs},
	})
}

func (s *service) Submit(spec Spec, requires ...string) (job.Job, error) {
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
		s.begin(existing)
		return s.Open(existing), nil
	}
	id, err2 := Submit(s.runner.Store, spec, requires...)
	if err2 != nil {
		return nil, err2
	}
	s.begin(id)
	return s.Open(id), nil
}

// inFlight returns the id of unfinished work for the same artifact and
// destination, or "".
//
// Identity is the destination plus the source. Not the digest — a caller often
// does not know one, which is exactly the case that needs this most.
func (s *service) inFlight(spec Spec) string {
	if len(spec.Sources) == 0 {
		return ""
	}
	all, err := s.runner.Store.List()
	if err != nil {
		return ""
	}
	for _, rec := range all {
		if rec.Kind != Kind || rec.State.Terminal() {
			continue
		}
		got, err := SpecOf(rec)
		if err != nil || got.Sink.Final != Portable(spec.Sink.Final) || len(got.Sources) == 0 {
			continue
		}
		if got.Sources[0].Locator == spec.Sources[0].Locator {
			return rec.ID
		}
	}
	return ""
}

// begin is the decision that used to be the application's.
//
// The in-process case is fire and forget on purpose: there is no error to return
// to a caller that has already been handed a durable handle, and any failure
// belongs on the record, where a different process — or this one after a restart
// — can still see it. A returned error would be visible only to whoever happened
// to still be running, which is the audience that does not need telling.
func (s *service) begin(id string) {
	if _, live := SupervisorOf(s.runner.Store); live {
		// Ask it to look now rather than at its next sweep. Best effort: if the
		// nudge goes nowhere the sweep still finds the work.
		Nudge(s.runner.Store)
		return
	}
	go s.runHere(id)
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
func (s *service) runHere(id string) {
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
