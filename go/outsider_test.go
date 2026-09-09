package download

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// withDelegate is a machine that has a tier and may or may not have anything
// watching the store, which is the difference the first outsider measured and
// the layer could not report.
func withDelegate(t *testing.T) (*Runner, job.Store, string) {
	t.Helper()
	r, store, root := newRunner(t)
	r.Delegators = NewDelegators(newFakeDelegate(nil))
	return r, store, root
}

func watching(t *testing.T, store job.Store, tier string) {
	t.Helper()
	if err := Heartbeat(store, "test-supervisor@host:1", tier, "", time.Minute); err != nil {
		t.Fatal(err)
	}
}

// watchingElsewhere is a supervisor on another machine, which is what makes an
// absolute sink one it could not deliver.
func watchingElsewhere(t *testing.T, store job.Store, tier string) {
	t.Helper()
	watching(t, store, tier)
	b, err := json.Marshal(Supervisor{
		Owner: "test-supervisor@elsewhere:1", Host: "elsewhere", PID: 1,
		Seen: job.At(time.Now()), Every: time.Minute.String(), Tier: tier,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(heartbeatPath(store), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A delegate is reached only by a supervisor. Naming one when nothing is
// watching the store tells a person their download survives closing the app,
// and then this process runs it and the download dies with the app.
func TestWhereNamesNoDelegateNothingWouldReach(t *testing.T) {
	r, _, _ := withDelegate(t)
	if got := NewClient(r).Where(); got != "here" {
		t.Fatalf("Where() = %q with nothing watching the store, want %q", got, "here")
	}
}

func TestWhereNamesTheSupervisorThatWouldTakeIt(t *testing.T) {
	r, store, _ := withDelegate(t)
	watching(t, store, "fake-service")
	if got := NewClient(r).Where(); got != "fake-service" {
		t.Fatalf("Where() = %q with a supervisor watching, want %q", got, "fake-service")
	}
}

// Where is published and begin dispatches, and they must not be able to
// disagree: the string an application shows a person is the string that decides.
func TestWhereAndBeginAgreeAboutWhoPerforms(t *testing.T) {
	r, store, root := withDelegate(t)
	svc := NewClient(r).(*client)
	watchingElsewhere(t, store, "fake-service")

	// A sink only this machine can resolve, and a supervisor that is not on
	// this machine. begin runs it here; Where must say so.
	local := Spec{
		Sources: []Source{{Scheme: "http", Locator: "http://127.0.0.1:1/x.bin"}},
		Sink:    Sink{Final: filepath.Join(root, "x.bin")},
	}
	name, here := svc.performer(local)
	if !here || name != "here" {
		t.Fatalf("performer(absolute sink, supervisor elsewhere) = %q here=%v, want \"here\" true", name, here)
	}
	portable := local
	portable.Sink = Sink{Final: "out/x.bin"}
	if name, here := svc.performer(portable); here || name != "fake-service" {
		t.Fatalf("performer(portable sink) = %q here=%v, want \"fake-service\" false", name, here)
	}
}

// Performer answers from the record, which is the only state that describes the
// job it is asked about.
func TestPerformerReadsTheRecordRatherThanTheMachine(t *testing.T) {
	if got := Performer(nil); got != "nobody" {
		t.Fatalf("Performer(nil) = %q", got)
	}
	if got := Performer(&job.Record{}); got != "nobody" {
		t.Fatalf("Performer(untouched) = %q, want %q", got, "nobody")
	}
	held := &job.Record{}
	held.Lease = job.Lease{Owner: "jobd@host:7", ExpiresAt: job.At(time.Now().Add(time.Minute))}
	if got := Performer(held); got != "jobd@host:7" {
		t.Fatalf("Performer(leased) = %q", got)
	}
	held.Delegation = &job.Delegation{System: "bits", ExternalID: "{guid}"}
	if got := Performer(held); got != "bits" {
		t.Fatalf("Performer(delegated) = %q, want the delegate over the lease", got)
	}
}

// An owner that was killed never releases, so its name outlives it. The record
// says when to stop believing it.
func TestPerformerDoesNotNameADeadOwner(t *testing.T) {
	lapsed := &job.Record{}
	lapsed.Lease = job.Lease{Owner: "fetchit@host:9", ExpiresAt: job.At(time.Now().Add(-time.Minute))}
	if got := Performer(lapsed); got != "nobody" {
		t.Fatalf("Performer(lapsed lease) = %q, want %q", got, "nobody")
	}
}

// The two endings are the whole retry model, and an application only ever sees
// the error Wait returns.
func TestWaitCarriesThePermanenceOfARefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	r, store, root := newRunner(t)
	h := submitTo(t, NewClient(r, WithExecution(ExecuteHere)), srv.URL+"/p.bin", filepath.Join(root, "p.bin"))
	_, err := h.Wait(context.Background())
	if err == nil {
		t.Fatal("a 404 delivered bytes")
	}
	if !Permanent(err) {
		t.Fatalf("Wait returned %v with Permanent=false; the record was written failed BECAUSE the class said forever", err)
	}
	rec, lerr := store.Load(h.ID())
	if lerr != nil {
		t.Fatal(lerr)
	}
	if _, ok := rec.Extensions[FailureExtension]; !ok {
		t.Fatalf("record %s carries the sentence and not the class", rec.ID)
	}
	if !Permanent(LastFailure(rec)) {
		t.Fatal("LastFailure lost the class the record carries")
	}
}

// A dropped connection is the case this project exists for: the record keeps
// its error, the partial stays, and a successor resumes.
func TestWaitCallsADroppedConnectionRetryable(t *testing.T) {
	r, store, root := newRunner(t)
	h := submitTo(t, NewClient(r, WithExecution(ExecuteHere)), "http://127.0.0.1:1/p.bin", filepath.Join(root, "p.bin"))
	_, err := h.Wait(context.Background())
	if err == nil {
		t.Fatal("a closed port delivered bytes")
	}
	if Permanent(err) {
		t.Fatalf("Wait called %v permanent; nothing about a refused connection says never", err)
	}
	rec, lerr := store.Load(h.ID())
	if lerr != nil {
		t.Fatal(lerr)
	}
	if Permanent(LastFailure(rec)) {
		t.Fatal("the record says a refused connection is forever")
	}
}

// A record from a writer that never heard of the key reads as retryable, which
// is the answer every reader gave before the key existed.
func TestARecordWithoutTheKeyIsRetryable(t *testing.T) {
	rec := &job.Record{Error: "something went wrong"}
	err := LastFailure(rec)
	if err == nil || err.Error() != "something went wrong" {
		t.Fatalf("LastFailure = %v, want the record's own sentence", err)
	}
	if Permanent(err) {
		t.Fatal("an unclassed failure must not claim to be permanent")
	}
	if LastFailure(&job.Record{}) != nil {
		t.Fatal("a record that has not failed reported a failure")
	}
}

// Clearing a failure takes the class with it. A stale class outliving its
// sentence answers "is this over" about an attempt nobody can read.
func TestClearingAFailureTakesTheClassWithIt(t *testing.T) {
	rec := &job.Record{}
	if err := setFailure(rec, ErrRefused); err != nil {
		t.Fatal(err)
	}
	if !Permanent(LastFailure(rec)) {
		t.Fatal("setFailure did not record the class")
	}
	clearFailure(rec)
	if rec.Error != "" || len(rec.Extensions[FailureExtension]) != 0 {
		t.Fatalf("clearFailure left %q and %d bytes of class", rec.Error, len(rec.Extensions[FailureExtension]))
	}
	if LastFailure(rec) != nil {
		t.Fatal("a cleared record still reports a failure")
	}
}

// A guarantee nothing on this machine promises is refused before a record
// exists. Four of the first outsider's ten records were job-shaped garbage
// created by asking for one.
func TestSubmitRefusesACapabilityNothingPromises(t *testing.T) {
	r, store, root := newRunner(t)
	_, err := NewClient(r).Submit(Spec{
		Sources: []Source{{Scheme: "http", Locator: "http://example.invalid/x.bin"}},
		Sink:    Sink{Final: filepath.Join(root, "x.bin")},
	}, string(CapVerifies))
	if err == nil {
		t.Fatal("Submit accepted a capability nothing registered promises")
	}
	if !Permanent(err) {
		t.Fatalf("refusal %v is not permanent; asking again unchanged cannot help", err)
	}
	if !strings.Contains(err.Error(), string(CapVerifies)) {
		t.Fatalf("refusal %q does not name what was unsatisfied", err)
	}
	if strings.Contains(err.Error(), `scheme "http"`) {
		t.Fatalf("refusal %q blames the scheme, which the HTTP fetcher serves", err)
	}
	all, lerr := store.List()
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(all) != 0 {
		t.Fatalf("%d records written for a job that was refused", len(all))
	}
}

// An unknown word never matches, so accepting it refuses the job later for the
// wrong reason. A caller who typos is told about their typo.
func TestSubmitRefusesAWordThatIsNotACapability(t *testing.T) {
	r, store, root := newRunner(t)
	_, err := NewClient(r).Submit(Spec{
		Sources: []Source{{Scheme: "http", Locator: "http://example.invalid/x.bin"}},
		Sink:    Sink{Final: filepath.Join(root, "x.bin")},
	}, "survives_process_exits")
	if err == nil {
		t.Fatal("Submit accepted a word that is not a capability")
	}
	if !strings.Contains(err.Error(), "survives_process_exits") || !strings.Contains(err.Error(), "not a capability") {
		t.Fatalf("refusal %q does not say the word is not a capability", err)
	}
	all, _ := store.List()
	if len(all) != 0 {
		t.Fatalf("%d records written for a word that is not a capability", len(all))
	}
}

// The same application, machine and request is served or refused depending on
// whether a second process is running, and the refusal has to say so — this one
// is try later, not never.
func TestADelegateOnlyCapabilityIsRefusedAsRetryableWithNoSupervisor(t *testing.T) {
	r, _, root := withDelegate(t)
	_, err := NewClient(r).Submit(Spec{
		Sources: []Source{{Scheme: "http", Locator: "http://example.invalid/x.bin"}},
		Sink:    Sink{Final: filepath.Join(root, "x.bin")},
	}, string(CapSurvivesProcessExit))
	if err == nil {
		t.Fatal("Submit accepted work only an unreachable delegate could do")
	}
	if !errors.Is(err, ErrNoDelegator) {
		t.Fatalf("refusal %v is not ErrNoDelegator", err)
	}
	if Permanent(err) {
		t.Fatalf("refusal %v says never; starting a supervisor makes this exact request work", err)
	}
	if !strings.Contains(err.Error(), "fake-service") {
		t.Fatalf("refusal %q does not name the delegate that could have taken it", err)
	}
}

func TestADelegateOnlyCapabilityIsAcceptedWhenASupervisorIsWatching(t *testing.T) {
	r, store, _ := withDelegate(t)
	watching(t, store, "fake-service")
	if _, err := NewClient(r).Submit(Spec{
		Sources: []Source{{Scheme: "http", Locator: "http://example.invalid/x.bin"}},
		Sink:    Sink{Final: "out/x.bin"},
	}, string(CapSurvivesProcessExit)); err != nil {
		t.Fatalf("Submit refused work a watching supervisor's delegate promises: %v", err)
	}
}

// offRoster is a plugin promising something this core has never named, which
// Assured deliberately ignores rather than refuses so that a newer plugin
// talking to an older core simply gets less.
type offRoster struct{ *fakeDelegate }

func (offRoster) System() string { return "plugin" }
func (offRoster) Capabilities() []Capability {
	return []Capability{"nas_native_copy", CapSurvivesProcessExit}
}

// A word off the roster is a typo only when nothing registered claims it. A
// plugin's own promise is not a typo.
func TestAWordAPluginPromisesIsNotATypo(t *testing.T) {
	r, store, _ := newRunner(t)
	r.Delegators = NewDelegators(offRoster{newFakeDelegate(nil)})
	watching(t, store, "plugin")
	spec := Spec{
		Sources: []Source{{Scheme: "http", Locator: "http://example.invalid/x.bin"}},
		Sink:    Sink{Final: "out/x.bin"},
	}
	if _, err := NewClient(r).Submit(spec, "nas_native_copy"); err != nil {
		t.Fatalf("Submit refused a capability a registered plugin promises: %v", err)
	}
	if _, err := NewClient(r).Submit(spec, "nas_native_kopy"); err == nil {
		t.Fatal("Submit accepted a word nothing has heard of")
	}
}

// Fetchers.For asks two questions and used to report one, so a source whose
// scheme was served by something lacking the capability read as a scheme
// nothing serves.
func TestNoFetcherSaysWhichHalfFailed(t *testing.T) {
	fs := DefaultFetchers()
	src := Source{Scheme: "http", Locator: "http://example.invalid/x.bin"}
	served := noFetcherFor(fs, src, []string{string(CapSurvivesProcessExit)}).Error()
	if !strings.Contains(served, string(CapSurvivesProcessExit)) {
		t.Fatalf("%q does not name the capability that was unsatisfied", served)
	}
	unserved := noFetcherFor(fs, Source{Scheme: "gopher"}, nil).Error()
	if !strings.Contains(unserved, `scheme "gopher"`) {
		t.Fatalf("%q does not name the scheme nothing serves", unserved)
	}
}

func submitTo(t *testing.T, svc Client, locator, final string) Handle {
	t.Helper()
	_, digest := payload(t, 512)
	h, err := svc.Submit(Spec{
		Artifact: Artifact{Digest: digest, Size: 512},
		Sources:  []Source{{Scheme: "http", Locator: locator}},
		Sink:     Sink{Final: final},
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}
