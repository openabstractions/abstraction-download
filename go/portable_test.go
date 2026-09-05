package download

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// A job record is read by machines other than the one that wrote it. The moment
// a store lives on a share, an absolute path in the record names a location only
// the author can see — so Submit must not write one.
func TestSubmitWritesNoMachineSpecificPath(t *testing.T) {
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id, err := Submit(store, Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/x.gguf"}},
		Sink:    Sink{Final: "models/x.gguf"},
	})
	if err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(filepath.Join(root, "jobs", id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), root) {
		t.Fatalf("the record names this machine's store root, so no other machine can act on it:\n%s", raw)
	}
}

// The same record, resolved by two machines that mount the store at different
// paths, must point at the same two files within it. This is the whole reason a
// NAS container can finish a download a PC started.
func TestSameRecordResolvesOnEitherMachine(t *testing.T) {
	sink := Sink{Partial: "work/abc", Final: "models/x.gguf"}

	winPartial, winFinal := sink.Resolve(`\nas\models\store`)
	nasPartial, nasFinal := sink.Resolve("/store")

	for _, c := range []struct{ got, want string }{
		{winPartial, `\nas\models\store\work\abc`},
		{winFinal, `\nas\models\store\models\x.gguf`},
		{nasPartial, "/store/work/abc"},
		{nasFinal, "/store/models/x.gguf"},
	} {
		want := filepath.FromSlash(strings.ReplaceAll(c.want, `\`, "/"))
		if filepath.FromSlash(strings.ReplaceAll(c.got, `\`, "/")) != want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
}

// A path that is absolute under EITHER convention is left alone. Joining
// "D:\models\x.gguf" onto a Linux store root would silently create a directory
// named `D:\models` on the NAS instead of failing.
func TestAbsolutePathsAreNotRebased(t *testing.T) {
	for _, p := range []string{`D:\models\x.gguf`, "/mnt/models/x.gguf", `\nas\share\x.gguf`, "c:/models/x.gguf"} {
		if got := resolveUnder("/store", p); got != p {
			t.Errorf("resolveUnder(%q) = %q, want it untouched", p, got)
		}
	}
	if got := resolveUnder("/store", ""); got != "" {
		t.Errorf("empty path should stay empty, got %q", got)
	}
}

// A Windows caller uses filepath.Join, which produces `models\x.gguf`. Written
// into a record verbatim that is not a directory and a file on Linux — it is one
// file whose name contains a backslash, so the job "succeeds" and puts 40 GB of
// weights somewhere nobody will ever look. Found by actually submitting from
// Windows into a store on a NAS.
func TestSubmitNormalisesWindowsSeparators(t *testing.T) {
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id, err := Submit(store, Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/x.gguf"}},
		Sink:    Sink{Final: filepath.Join("models", "x.gguf")},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := SpecOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Sink.Final != "models/x.gguf" {
		t.Fatalf("record holds %q; a machine with the other separator cannot read that as a path", spec.Sink.Final)
	}
	// And it still resolves to the right place on a POSIX machine.
	if _, final := spec.Sink.Resolve("/store"); final != filepath.Join("/store", "models", "x.gguf") {
		t.Fatalf("resolved to %q", final)
	}
}

// Records written before Submit normalised separators are already on disk, so
// resolving must forgive them rather than fail.
func TestResolveForgivesLegacyBackslashes(t *testing.T) {
	sink := Sink{Partial: `work\abc`, Final: `models\x.gguf`}
	partial, final := sink.Resolve("/store")
	if partial != filepath.Join("/store", "work", "abc") {
		t.Errorf("partial resolved to %q", partial)
	}
	if final != filepath.Join("/store", "models", "x.gguf") {
		t.Errorf("final resolved to %q", final)
	}
}
