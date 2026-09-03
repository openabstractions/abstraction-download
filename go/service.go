package download

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	job "github.com/ReinisLusis/abstraction-job"
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
func NewService(r *Runner) Service { return &service{runner: r} }

type service struct{ runner *Runner }

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
	return NewService(r), r.Store, nil
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
	id, err := Submit(s.runner.Store, spec, requires...)
	if err != nil {
		return nil, err
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
	go s.runner.Run(context.Background(), id)
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
