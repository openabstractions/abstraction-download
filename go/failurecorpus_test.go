package download

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// The corpus and what each record must still MEAN, read off the table that
// ships beside it rather than restated here: a second copy of the answers is a
// second thing to keep true.
func corpus(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "testdata", "failures", "expect.txt"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for _, line := range strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n") {
		if line = strings.TrimSpace(line); line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		want[f[0]] = f[1]
	}
	if len(want) == 0 {
		t.Fatal("the corpus table names no record, so this test asserts nothing")
	}
	return want
}

func class(err error) string {
	switch {
	case err == nil:
		return "none"
	case Permanent(err):
		return "permanent"
	}
	return "retryable"
}

// A failure written by any of the three implementations is recovered here with
// its class intact, and so is one written before the class existed.
//
// This is the half of a failure that no error object carries across a process.
// `Record.Error` is prose; rebuilding from it alone answers "retryable" about
// every refusal this layer declares forever, and a job that stays adoptable is
// fetched again on every sweep for as long as the store exists.
func TestAFailureFromAnyLanguageKeepsItsClass(t *testing.T) {
	for name, want := range corpus(t) {
		b, err := os.ReadFile(filepath.Join("..", "testdata", "failures", name+".json"))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		rec, err := job.Decode(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := class(LastFailure(rec)); got != want {
			t.Errorf("%s means %q, and every reader of it must say %q", name, got, want)
		}
	}
}

// The same corpus, through a store this process owns and through one it reaches
// over a socket.
//
// Service availability changes coordination and isolation. It does not change
// what a failure means, and nothing but a test that reads the same record both
// ways can say so: the two bindings share the record's codec and share nothing
// else, so a class that survived one and not the other would look like a
// working system from either side alone.
func TestAFailureMeansTheSameWithAndWithoutAService(t *testing.T) {
	want := corpus(t)

	embedded, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	behind, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go job.Serve(ln, behind)
	served := job.NewRemoteStore(ln.Addr().Network(), ln.Addr().String())

	for _, mode := range []struct {
		name  string
		store job.Store
	}{{"embedded", embedded}, {"service", served}} {
		for name, class := range want {
			b, err := os.ReadFile(filepath.Join("..", "testdata", "failures", name+".json"))
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			rec, err := job.Decode(b)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			// A fresh id per mode: the record is submitted twice and a store
			// refuses to be handed an id it already holds.
			rec.ID = ""
			id, err := mode.store.Submit(*rec)
			if err != nil {
				t.Fatalf("%s via %s: %v", name, mode.name, err)
			}
			back, err := mode.store.Load(id)
			if err != nil {
				t.Fatalf("%s via %s: %v", name, mode.name, err)
			}
			if got := classOf(back); got != class {
				t.Errorf("%s means %q through %s and %q on the page",
					name, got, mode.name, class)
			}
		}
	}
}

func classOf(rec *job.Record) string { return class(LastFailure(rec)) }

// Nothing an application calls lets it write the bookkeeping by hand.
//
// `content` is derived from what the record carries, on every write, by the job
// layer -- so a failure appears there because the payload is there and for no
// other reason. The one way to put the payload in is to fail, and the one way
// to take it out is to stop having failed.
func TestBookkeepingIsNotTheApplicationsToWrite(t *testing.T) {
	rec := &job.Record{Kind: Kind, Spec: []byte(`{}`)}
	if err := setFailure(rec, ErrRefused); err != nil {
		t.Fatal(err)
	}
	if got := class(LastFailure(rec)); got != "permanent" {
		t.Fatalf("a refusal recorded as %q", got)
	}
	if len(rec.Content) != 0 {
		t.Fatalf("setFailure wrote the declaration itself: %v", rec.Content)
	}
	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id, err := store.Submit(*rec)
	if err != nil {
		t.Fatal(err)
	}
	back, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if !has(back.Content, FailureExtension) {
		t.Fatalf("the store did not declare the payload it carries: %v", back.Content)
	}
	clearFailure(rec)
	id2, err := store.Submit(*rec)
	if err != nil {
		t.Fatal(err)
	}
	cleared, err := store.Load(id2)
	if err != nil {
		t.Fatal(err)
	}
	if has(cleared.Content, FailureExtension) {
		t.Fatalf("the declaration outlived the payload: %v", cleared.Content)
	}
}

// The corpus is read by three languages and every one of them has to find the
// same files. A record added to the directory and left out of the table is
// asserted about by nothing, which is the shape of every instrument that
// quietly stopped measuring.
func TestTheCorpusTableNamesEveryRecordInTheDirectory(t *testing.T) {
	want := corpus(t)
	entries, err := os.ReadDir(filepath.Join("..", "testdata", "failures"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := strings.TrimSuffix(e.Name(), ".json")
		if name == e.Name() {
			continue
		}
		if _, ok := want[name]; !ok {
			t.Errorf("%s is in the corpus and no line of expect.txt says what it means", e.Name())
		}
	}
	for name := range want {
		if _, err := os.Stat(filepath.Join("..", "testdata", "failures", name+".json")); err != nil {
			t.Errorf("expect.txt names %s and the directory does not carry it", name)
		}
	}
}
