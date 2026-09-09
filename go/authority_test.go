package download

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// The rules these exercise, one scenario each:
//
//	A1  a supervisor on this machine and this account is handed a job whose sink
//	    only this machine's filesystem has
//	A2  a supervisor on this machine running as another account is not
//	A3  a supervisor that does not say which account it runs as is not
//	A4  a supervisor on another machine is not
//	A5  a heartbeat this layer writes says which account wrote it, or A1 can
//	    never hold and the layer has quietly stopped delegating anything
//
// A1 is the one the ComfyUI adopter needs: without it a download does not
// survive the application closing, which is the claim this project makes.

func absoluteSink(t *testing.T) Spec {
	t.Helper()
	return Spec{
		Sources: []Source{{Scheme: "http", Locator: "http://example.invalid/m.safetensors"}},
		Sink:    Sink{Final: filepath.Join(t.TempDir(), "models", "m.safetensors")},
	}
}

// rewriteHeartbeat is a supervisor whose heartbeat says something Heartbeat
// would not say about this process, which is the whole point: every case below
// is about a supervisor that is NOT this test.
func rewriteHeartbeat(t *testing.T, store job.Store, change func(*Supervisor)) {
	t.Helper()
	sup, live := SupervisorOf(store)
	if !live {
		t.Fatal("no supervisor to rewrite")
	}
	change(&sup)
	b, err := json.Marshal(sup)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(heartbeatPath(store), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A5. Every case below reads this field, so a heartbeat that does not carry it
// makes all of them vacuous — and makes the layer refuse every delegation
// forever, which is the failure that would look like nothing being wrong.
func TestAHeartbeatSaysWhichAccountWroteIt(t *testing.T) {
	_, store, _ := withDelegate(t)
	watching(t, store, "fake-service")
	sup, live := SupervisorOf(store)
	if !live {
		t.Fatal("the heartbeat this layer just wrote is not live")
	}
	if sup.User == "" {
		t.Fatal("Heartbeat wrote no account, so no supervisor can ever be trusted with an absolute sink")
	}
	if sup.User != Account() {
		t.Fatalf("Heartbeat wrote account %q, this process is %q", sup.User, Account())
	}
}

// A1. The defect this file exists for. A ComfyUI sink is an absolute path in
// ComfyUI's own models tree, and a jobd on this machine under this account can
// write it perfectly well — so the work goes to it, and the download outlives
// the application that asked for it.
func TestASupervisorHereUnderThisAccountIsHandedAnAbsoluteSink(t *testing.T) {
	r, store, _ := withDelegate(t)
	watching(t, store, "fake-service")
	svc := NewClient(r).(*client)

	name, here := svc.performer(absoluteSink(t))
	if here || name != "fake-service" {
		t.Fatalf("performer(absolute sink, supervisor here and ours) = %q here=%v, "+
			"want \"fake-service\" false — the transfer dies with this process", name, here)
	}
}

// A2. Same filesystem, different rights. A machine-wide supervisor running as a
// service account would be writing into somebody's own tree, which is a
// permissions problem before it is a delivery one.
func TestASupervisorUnderAnotherAccountIsNotHandedAnAbsoluteSink(t *testing.T) {
	r, store, _ := withDelegate(t)
	watching(t, store, "fake-service")
	rewriteHeartbeat(t, store, func(s *Supervisor) {
		// The spelling a Windows service account has, and never this process's.
		s.User = "S-1-5-18"
		s.Owner = "jobd@" + s.Host + ":1"
	})
	svc := NewClient(r).(*client)

	if name, here := svc.performer(absoluteSink(t)); !here || name != "here" {
		t.Fatalf("performer(absolute sink, supervisor here as another account) = %q here=%v, "+
			"want \"here\" true", name, here)
	}
	// And nothing about the sink's own machine changed: a portable sink is still
	// that supervisor's to take. The refusal is about the path, not about it.
	portable := absoluteSink(t)
	portable.Sink = Sink{Final: "out/m.safetensors"}
	if name, here := svc.performer(portable); here || name != "fake-service" {
		t.Fatalf("performer(portable sink) = %q here=%v — the account check leaked "+
			"into a job that never needed it", name, here)
	}
}

// A3. Absence is reported, never passed. A heartbeat written by a build that
// never heard of the field says nothing about who wrote it, and nothing is not
// a yes.
func TestASupervisorThatDoesNotSayItsAccountIsNotHandedAnAbsoluteSink(t *testing.T) {
	r, store, _ := withDelegate(t)
	watching(t, store, "fake-service")
	rewriteHeartbeat(t, store, func(s *Supervisor) { s.User = "" })
	svc := NewClient(r).(*client)

	if name, here := svc.performer(absoluteSink(t)); !here || name != "here" {
		t.Fatalf("performer(absolute sink, supervisor that named no account) = %q here=%v, "+
			"want \"here\" true — an unanswered question was read as a yes", name, here)
	}
}

// A4. The rule the original refusal was written for, and it must still hold: a
// supervisor on a NAS handed `C:\ComfyUI\models\x.safetensors` writes to a
// directory that exists here and not there.
func TestASupervisorOnAnotherMachineIsNotHandedAnAbsoluteSink(t *testing.T) {
	r, store, _ := withDelegate(t)
	watching(t, store, "fake-service")
	rewriteHeartbeat(t, store, func(s *Supervisor) {
		s.Host = "a-machine-that-is-not-this-one"
		// Same account id, which on two machines is two different accounts. The
		// host is what has to refuse this, and it does.
	})
	svc := NewClient(r).(*client)

	if name, here := svc.performer(absoluteSink(t)); !here || name != "here" {
		t.Fatalf("performer(absolute sink, supervisor elsewhere) = %q here=%v, want \"here\" true", name, here)
	}
}

// The published string and the decision are one rule, and this is the case that
// would have let them drift: Where asks about a job with no sink at all.
func TestWhereStillNamesTheSupervisorForAJobThatNamesNoPath(t *testing.T) {
	r, store, _ := withDelegate(t)
	watching(t, store, "fake-service")
	if got := NewClient(r).Where(); got != "fake-service" {
		t.Fatalf("Where() = %q with a supervisor here under this account, want %q", got, "fake-service")
	}
}

// A1, end to end rather than through the rule: the record is left for the
// supervisor and this process does not work it.
//
// The source refuses a connection immediately, so had this process taken the
// job the record would carry an error within one attempt. Bounded on purpose —
// nothing here waits for something to happen; it samples a fixed number of
// times and reports what it saw.
func TestAnAbsoluteSinkIsLeftForASupervisorSharingThisAccount(t *testing.T) {
	r, store, _ := withDelegate(t)
	watching(t, store, "fake-service")
	spec := absoluteSink(t)
	spec.Sources = []Source{{Scheme: "http", Locator: "http://127.0.0.1:1/m.safetensors"}}
	h, err := NewClient(r).Submit(spec)
	if err != nil {
		t.Fatal(err)
	}
	const samples = 20
	for i := 0; i < samples; i++ {
		rec, err := store.Load(h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if rec.Error != "" {
			t.Fatalf("this process worked a job a supervisor here was watching for: %q", rec.Error)
		}
		if rec.Lease.Held(time.Now()) {
			t.Fatalf("this process claimed a job a supervisor here was watching for: %s", rec.Lease.Owner)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
