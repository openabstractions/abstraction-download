package download

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// A store several machines write is the case the refusal exists for, and it
// still holds: an absolute sink names the submitter's filesystem, and a
// supervisor elsewhere writing it is the confused deputy.
//
// But it declines the job rather than failing it. The record is legitimate on
// the machine that wrote it, and a supervisor that claims it only to record an
// error takes the destination away there too: the submitter's own client reads
// that error and hands it to the caller.
func TestASharedSupervisorLeavesAnAbsoluteSinkAlone(t *testing.T) {
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	src := []Source{{Scheme: "https", Locator: "https://example.invalid/x.bin"}}
	native := filepath.Join(t.TempDir(), "x.bin")
	id, err := Submit(store, Spec{Sources: src, Sink: Sink{Final: native}})
	if err != nil {
		t.Fatal(err)
	}

	shared := NewRunner(store, "nas")
	shared.SharedStore = true
	if n, err := shared.Adopt(context.Background()); n != 0 || err != nil {
		t.Fatalf("a shared supervisor took a job it cannot write: adopted=%d err=%v", n, err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Error != "" {
		t.Fatalf("a machine's own policy was written onto a record every other machine reads: %q", rec.Error)
	}
	if rec.Lease.Epoch != 0 {
		t.Fatalf("declining cost the job an attempt: epoch %d", rec.Lease.Epoch)
	}

	// Asked directly, it still refuses out loud. Declining a sweep is not
	// pretending the job is servable here.
	if err := shared.Run(context.Background(), id); !errors.Is(err, ErrUnportableSink) {
		t.Fatalf("a shared store wrote an absolute sink a record chose: %v", err)
	}
}

// The same relative string means two places, and only one of them is written
// down. Get's destination is a shell path — the caller's working directory.
// Submit's sink is a record's — under the store root, on whichever machine
// resolves it. This is the test that stops the two drifting back together.
func TestARelativeDestinationMeansTheCallersDirectory(t *testing.T) {
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	svc := idleClient(NewRunner(store, "app"))

	viaGet, err := svc.Get("https://example.invalid/x.bin", "out/x.bin")
	if err != nil {
		t.Fatal(err)
	}
	here, err := filepath.Abs("out/x.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := sinkOf(t, store, viaGet.ID()); comparablePath(got) != comparablePath(here) {
		t.Fatalf("Get resolved %q against something other than the working directory: %q", "out/x.bin", got)
	}

	viaSubmit, err := svc.Submit(Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/y.bin"}},
		Sink:    Sink{Final: "out/y.bin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sinkOf(t, store, viaSubmit.ID()); got != "out/y.bin" {
		t.Fatalf("Submit rewrote a store-relative sink: %q", got)
	}
	_, final, err := LocalSink(store, viaSubmit.ID(), Sink{Final: "out/y.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if !under(root, final) {
		t.Fatalf("a record's relative sink resolved outside the store root: %q", final)
	}
}

func sinkOf(t *testing.T, store job.Store, id string) string {
	t.Helper()
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := SpecOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	return spec.Sink.Final
}

// Sharedness is a property of the store, and a UNC root is the one piece of
// evidence a program has without being told.
func TestSharedStoreRoot(t *testing.T) {
	for root, want := range map[string]bool{
		`\\nas\share\store`: true,
		"//nas/share/store": true,
		`C:\Users\me\.abs`:  false,
		"/home/me/.abs":     false,
		"":                  false,
		"/mnt/nas/store":    false,
	} {
		if got := SharedStoreRoot(root); got != want {
			t.Errorf("SharedStoreRoot(%q) = %v, want %v", root, got, want)
		}
	}
}

// One root, written once, read by two machines. The two spellings below name the
// same share, so an answer that differs between them is an answer about the
// reader rather than about the store.
func TestSharedStoreRootIsAPropertyOfTheRootNotOfTheReader(t *testing.T) {
	for _, pair := range [][2]string{
		{`\\nas\share\store`, "//nas/share/store"},
		{`\\nas\share`, "//nas/share"},
		{`C:\Users\me\.abs`, "C:/Users/me/.abs"},
	} {
		if SharedStoreRoot(pair[0]) != SharedStoreRoot(pair[1]) {
			t.Errorf("%q is %v and %q is %v: the classification follows the host",
				pair[0], SharedStoreRoot(pair[0]), pair[1], SharedStoreRoot(pair[1]))
		}
	}
}
