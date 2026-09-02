package download

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	job "github.com/ReinisLusis/abstraction-job"
)

// Is a system downloader running on this machine, and can this application just
// hand work to it?
//
// # The mistake this fixes
//
// dl linked every tier — bits and nas both — so an application knew a NAS
// existed. That is backwards, and it contradicts the design the project started
// from: an application's downloader gives way to the SYSTEM downloader if one is
// present, and the system downloader gives way to a NAS if one is configured.
// Two hops, each ignorant of the one after it.
//
// The logging comparison is exact. A library that logs knows about a facade and
// a default; it does not know which file, which socket or which remote collector
// the sinks are pointing at, and it would be a bad library if it did. An
// application that downloads should know about "here" and "the system service",
// and nothing else. Whether that service then uses BITS, a NAS or a carrier
// pigeon is its business, configured once, in one place.
//
// # How presence is detected
//
// The supervisor writes a heartbeat into the store it watches. An application
// reads it. That is the whole protocol — no socket, no port, no registry key —
// and it works for the same reason the rest of this does: the store is the
// interface, and a file both processes can see is the cheapest possible way for
// one to know the other is alive.
//
// The heartbeat is deliberately not a pid file. A pid proves a process existed
// when the file was written; a timestamp refreshed on every sweep proves one is
// alive now. A supervisor that was killed leaves a stale heartbeat, and an
// application that trusted it would submit work into a store nobody is watching
// — which looks exactly like a download that started and never progressed.
type Supervisor struct {
	Owner string `json:"owner"`
	Host  string `json:"host"`
	PID   int    `json:"pid"`
	// Seen is refreshed every sweep.
	Seen job.Timestamp `json:"seen"`
	// Every is how often the supervisor promises to refresh it, so a reader can
	// decide what counts as stale without guessing.
	Every string `json:"every"`
	// Tier is what the supervisor itself would delegate to, purely so a human
	// running a status command can see the whole chain at once.
	Tier string `json:"tier,omitempty"`
}

// heartbeatName sits beside jobs/ and work/ in the store.
const heartbeatName = "supervisor.json"

func heartbeatPath(store *job.FileStore) string {
	return filepath.Join(store.Root(), heartbeatName)
}

// Heartbeat records that a supervisor is alive and watching this store. jobd
// calls it on every sweep.
func Heartbeat(store *job.FileStore, owner, tier string, every time.Duration) error {
	host, _ := os.Hostname()
	s := Supervisor{
		Owner: owner, Host: host, PID: os.Getpid(),
		Seen: job.At(time.Now()), Every: every.String(), Tier: tier,
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	// Write and rename, so a reader never sees half a heartbeat and concludes
	// the supervisor is dead.
	tmp := heartbeatPath(store) + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, heartbeatPath(store))
}

// StopHeartbeat removes the heartbeat on a clean exit, so applications stop
// handing work to a supervisor that has gone away without waiting out the
// staleness window.
func StopHeartbeat(store *job.FileStore) error {
	err := os.Remove(heartbeatPath(store))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// SupervisorOf reports the live supervisor for this store, if there is one.
//
// "Live" means the heartbeat is younger than a few of its own intervals. Three
// is enough to survive a slow sweep or a clock that jitters, and short enough
// that a killed supervisor stops attracting work within a minute or two rather
// than forever.
func SupervisorOf(store *job.FileStore) (Supervisor, bool) {
	b, err := os.ReadFile(heartbeatPath(store))
	if err != nil {
		return Supervisor{}, false
	}
	var s Supervisor
	if err := json.Unmarshal(b, &s); err != nil {
		return Supervisor{}, false
	}
	every, err := time.ParseDuration(s.Every)
	if err != nil || every <= 0 {
		every = 30 * time.Second
	}
	if time.Since(s.Seen.Time) > 3*every {
		return s, false // stale: something died without tidying up
	}
	return s, true
}
