package download

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

func serveOnce(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Every way Run can end, and what the lease looks like afterwards.
//
// A terminal ending is its own release — nothing may claim a finished job — and
// the store refuses to write the lease back, so what is asserted there is that
// nobody can take the record, not that the field was cleared.
func TestRunLeavesNoJobHeldByAnOwnerThatHasStopped(t *testing.T) {
	body, digest := payload(t, 4096)

	for _, c := range []struct {
		name  string
		state job.State
		setup func(t *testing.T, store job.Store, root string) string
	}{
		{
			name:  "transferred",
			state: job.StateTransferred,
			setup: func(t *testing.T, store job.Store, root string) string {
				srv := serveOnce(t, body)
				return submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})
			},
		},
		{
			name:  "paused",
			state: job.StatePending,
			setup: func(t *testing.T, store job.Store, root string) string {
				srv := serveOnce(t, body)
				id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})
				if _, err := store.SetIntent(id, job.WantPause, "a person"); err != nil {
					t.Fatal(err)
				}
				return id
			},
		},
		{
			name:  "cancelled",
			state: job.StateCancelled,
			setup: func(t *testing.T, store job.Store, root string) string {
				srv := serveOnce(t, body)
				id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})
				if _, err := store.SetIntent(id, job.WantCancel, "a person"); err != nil {
					t.Fatal(err)
				}
				return id
			},
		},
		{
			name:  "failed",
			state: job.StateFailed,
			setup: func(t *testing.T, store job.Store, root string) string {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "gone", http.StatusNotFound)
				}))
				t.Cleanup(srv.Close)
				return submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, store, root := newRunner(t)
			id := c.setup(t, store, root)

			ran := r.Run(context.Background(), id)
			if (ran != nil) != (c.state == job.StateFailed) {
				t.Fatalf("Run returned %v for a job that ends %s", ran, c.state)
			}

			rec, err := store.Load(id)
			if err != nil {
				t.Fatal(err)
			}
			if rec.State != c.state {
				t.Fatalf("state %s, want %s", rec.State, c.state)
			}
			if c.state.Terminal() {
				if _, err := store.Claim(id, "somebody-else", time.Second); !errors.Is(err, job.ErrTerminal) {
					t.Errorf("a %s job was claimable: %v", c.state, err)
				}
				return
			}
			if rec.Lease.Owner != "" || rec.Lease.Held(time.Now()) {
				t.Errorf("lease still held by %q until %s", rec.Lease.Owner, rec.Lease.ExpiresAt)
			}
			if _, err := store.Claim(id, "somebody-else", time.Second); err != nil {
				t.Errorf("the next owner could not take it: %v", err)
			}
		})
	}
}

// refusingRelease is a store that will not give a lease back for a reason the
// job layer has no name for — a disk, a socket, a remote store saying no.
type refusingRelease struct {
	job.Store
	why error
}

func (s refusingRelease) Release(id string, epoch int64) error { return s.why }

// The regression this whole file exists for: the release refusal was thrown
// away, so four days of jobs held leases they had finished with and nothing
// anywhere said a word.
func TestRunReportsAReleaseItCouldNotPerform(t *testing.T) {
	body, digest := payload(t, 4096)
	srv := serveOnce(t, body)

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})

	refused := errors.New("the store said no")
	r.Store = refusingRelease{Store: store, why: refused}

	if err := r.Run(context.Background(), id); !errors.Is(err, refused) {
		t.Fatalf("Run returned %v, want the release refusal", err)
	}
}

// A terminal record is already released, and the store saying so is not news.
func TestRunTreatsATerminalRecordAsAlreadyReleased(t *testing.T) {
	body, digest := payload(t, 4096)
	srv := serveOnce(t, body)

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})
	r.Store = refusingRelease{Store: store, why: job.ErrTerminal}

	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("Run returned %v for a job the store called finished", err)
	}
}

// corrupt writes a record file nothing can decode, which is the situation
// job.List and job.Orphans now answer with records AND a refusal.
func corrupt(t *testing.T, root, id string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, "jobs", id+".json"), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// One unreadable file must not deny service to every job beside it, and must
// not vanish either.
func TestSweepsWorkAroundAnUnreadableRecordAndSayThatTheyDid(t *testing.T) {
	body, digest := payload(t, 4096)
	srv := serveOnce(t, body)

	for _, c := range []struct {
		name  string
		sweep func(r *Runner) (int, error)
	}{
		{"adopt", func(r *Runner) (int, error) { return r.Adopt(context.Background()) }},
		{"takedelivery", func(r *Runner) (int, error) {
			if _, err := r.Adopt(context.Background()); err == nil {
				t.Fatal("the setup sweep should have reported the unreadable record")
			}
			return r.TakeDeliveryAll(context.Background())
		}},
		{"reconcile", func(r *Runner) (int, error) { return r.ReconcileAll(context.Background()) }},
		{"delegate", func(r *Runner) (int, error) {
			r.Delegators = NewDelegators(newFakeDelegate(body))
			return r.DelegateAll(context.Background())
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, store, root := newRunner(t)
			id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})
			corrupt(t, root, "unreadable-"+c.name)

			n, err := c.sweep(r)
			var unread *job.ErrUnreadable
			if !errors.As(err, &unread) {
				t.Fatalf("sweep returned %v, want the unreadable record named", err)
			}
			if len(unread.IDs) != 1 || unread.IDs[0] != "unreadable-"+c.name {
				t.Errorf("named %v", unread.IDs)
			}
			if c.name == "adopt" || c.name == "takedelivery" {
				if n != 1 {
					t.Errorf("worked %d jobs, want the one that decoded", n)
				}
				rec, err := store.Load(id)
				if err != nil {
					t.Fatal(err)
				}
				want := job.StateTransferred
				if c.name == "takedelivery" {
					want = job.StateComplete
				}
				if rec.State != want {
					t.Errorf("state %s, want %s", rec.State, want)
				}
			}
		})
	}
}
