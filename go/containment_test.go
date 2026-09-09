package download

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// The confused deputy, pinned.
//
// A PC submits the record and a NAS adopts it, so the sink is a destination the
// SUBMITTER chose and the ADOPTER writes to, with the adopter's authority.
// filepath.Join cleans a `..` away lexically and reports nothing, so these three
// strings resolved to real files outside the store — an ssh authorized_keys and
// a Startup directory among them. Reproduced against a store root of
// `C:\store\jobs`, which is where the resolved paths below came from.
func TestSinkMayNotEscapeTheStoreRoot(t *testing.T) {
	const root = `C:\store\jobs`

	for _, p := range []string{
		"../../../Users/victim/.ssh/authorized_keys",
		`..\..\..\Startup\evil.bat`,
		"..",
		"a/../../b",           // clean on its own and still one level out
		"models/../../x.gguf", // the climb is not at the front
	} {
		if _, err := resolveUnder(root, p); !errors.Is(err, ErrEscapesRoot) {
			t.Errorf("resolveUnder(%q, %q) did not refuse: %v", root, p, err)
		}
	}

	// And the paths that are the point of the feature still work.
	for _, p := range []string{
		"models/x.gguf",
		"work/2f8a-b1",
		"models/org/repo/rev/x.gguf",
		"models/./x.gguf",
		"models/tmp/../x.gguf", // clean it and it never leaves
	} {
		if _, err := resolveUnder(root, p); err != nil {
			t.Errorf("resolveUnder(%q, %q) refused a contained path: %v", root, p, err)
		}
	}

	// The nesting that must keep working, checked against the actual answer
	// rather than only against the absence of an error.
	got, err := resolveUnder(root, "models/org/repo/x.gguf")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, "models", "org", "repo", "x.gguf"); got != want {
		t.Fatalf("resolved to %q, want %q", got, want)
	}
}

// The refusal is an error the caller can recognise and a message a person can
// read, and it names the path that was refused rather than the one it resolved
// to. A caller has to be able to find the field in its own record.
func TestRefusalNamesThePathFromTheRecord(t *testing.T) {
	_, err := resolveUnder(`C:\store\jobs`, "../../../Users/victim/.ssh/authorized_keys")
	if err == nil {
		t.Fatal("no error")
	}
	// Spelled out in full, because Python and C++ must print this same string:
	// see download/python/test_abstraction_download.py and
	// download/cpp/test/test_sink_containment.cpp.
	const want = "download: sink path escapes the store root: ../../../Users/victim/.ssh/authorized_keys"
	if err.Error() != want {
		t.Fatalf("refusal reads\n  %s\nwant\n  %s", err, want)
	}
	if !errors.Is(err, ErrEscapesRoot) {
		t.Fatal("the refusal is not recognisable as ErrEscapesRoot")
	}
}

// The classic way this fix ships broken: `C:\store2` starts with `C:\store`, so
// a prefix test on the raw strings calls a different directory contained. The
// comparison has to end on a separator boundary.
func TestContainmentIsNotAPrefixTest(t *testing.T) {
	cases := []struct {
		root, resolved string
		want           bool
	}{
		{`C:\store`, `C:\store2\x.gguf`, false},
		{`C:\store`, `C:\store\x.gguf`, true},
		{`C:\store`, `C:\store`, true},
		{`C:\store\`, `C:\store\x.gguf`, true}, // a trailing separator on the root
		{"/store", "/store2/x.gguf", false},
		{"/store", "/store/x.gguf", true},
		{"/", "/x.gguf", true},

		// A UNC root: the share is part of the root, so a sibling share is out.
		{`\\nas\models\store`, `\\nas\models\store\x.gguf`, true},
		{`\\nas\models\store`, `\\nas\models\store2\x.gguf`, false},
		{`\\nas\models\store`, `\\nas\other\store\x.gguf`, false},

		// Nothing to be under: the store's binding is not a filesystem, so all
		// that can be said is whether the path climbed out of wherever it lands.
		{"", "models/x.gguf", true},
		{"", "../x.gguf", false},
		{"", "..", false},
	}
	for _, c := range cases {
		if got := under(c.root, c.resolved); got != c.want {
			t.Errorf("under(%q, %q) = %v, want %v", c.root, c.resolved, got, c.want)
		}
	}

	// Windows ignores case in a path and POSIX does not, so this is asked of
	// the platform the test is running on rather than assumed either way.
	if got := under(`C:\Store`, `C:\store\x.gguf`); got != (runtime.GOOS == "windows") {
		t.Errorf("under(`C:\\Store`, `C:\\store\\x.gguf`) = %v on %s", got, runtime.GOOS)
	}
}

// Refused at submission too, so a record naming a path outside the root never
// reaches a store that another machine reads.
func TestSubmitRefusesAnEscapingSink(t *testing.T) {
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	src := []Source{{Scheme: "https", Locator: "https://example.invalid/x.gguf"}}

	for _, sink := range []Sink{
		{Final: "../../../Users/victim/.ssh/authorized_keys"},
		{Final: `..\..\..\Startup\evil.bat`},
		{Final: "models/x.gguf", Partial: "../../../Startup/evil.bat"},
	} {
		if _, err := Submit(store, Spec{Sources: src, Sink: sink}); !errors.Is(err, ErrEscapesRoot) {
			t.Errorf("Submit accepted %+v: %v", sink, err)
		}
	}

	if _, err := Submit(store, Spec{Sources: src, Sink: Sink{Final: "models/org/repo/x.gguf"}}); err != nil {
		t.Errorf("Submit refused a legitimately nested path: %v", err)
	}
}

// And refused on the side that does the writing, which is the side that
// matters: a record can arrive in a shared store without passing through any
// Submit of ours.
func TestLocalSinkRefusesARecordThatEscaped(t *testing.T) {
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	sink := Sink{Partial: "work/abc", Final: "../../../Users/victim/.ssh/authorized_keys"}
	if _, _, err := LocalSink(store, "abc", sink); !errors.Is(err, ErrEscapesRoot) {
		t.Fatalf("LocalSink resolved an escaping record: %v", err)
	}

	partial, final, err := LocalSink(store, "abc", Sink{Partial: "work/abc", Final: "models/x.gguf"})
	if err != nil {
		t.Fatal(err)
	}
	for _, got := range []string{partial, final} {
		if !strings.HasPrefix(got, root) {
			t.Fatalf("%q is not under the store root %q", got, root)
		}
	}
}

// EscapesRoot answers about a RECORD, which names no root — that is what lets
// the three readers refuse the same records. An absolute path answers nil: it
// is never joined onto the root, and what a delegate should do with one is a
// separate question this does not decide.
func TestEscapesRootAnswersWithoutARoot(t *testing.T) {
	for _, p := range []string{"../../../Users/victim/.ssh/authorized_keys", `..\..\..\Startup\evil.bat`} {
		if EscapesRoot(p) == nil {
			t.Errorf("EscapesRoot(%q) = nil", p)
		}
	}
	for _, p := range []string{"", "models/x.gguf", "work/abc", `D:\models\x.gguf`, "/mnt/models/x.gguf", `\\nas\share\x.gguf`} {
		if err := EscapesRoot(p); err != nil {
			t.Errorf("EscapesRoot(%q) = %v", p, err)
		}
	}
}

// Contained, and still aimed at the store.
//
// Containment stopped a sink climbing OUT of the root and never stopped one
// naming what is IN it. A final of `jobs/<id>.json` overwrites a job record; a
// final of `work/<other>` overwrites another job's partial. Both are inside the
// root and both passed every check that existed.
func TestSinkMayNotNameTheStoresOwnFiles(t *testing.T) {
	const me = "1757000000000-deadbeef"
	const other = "1757000000001-cafebabe"

	for _, p := range []string{
		"jobs/" + other + ".json",
		"jobs/" + me + ".json",
		"jobs/" + me + ".epoch.7",
		"jobs",
		"work",
		"work/" + other,
		"work/" + other + "/part",
		"services.json",
		"supervisor.json",
		"supervisor.json.tmp",
		"supervisor.sock",

		// The spellings a filesystem folds into the ones above. NTFS lands
		// `Jobs/x.json` in `jobs/`, so a rule that folded case only where the
		// host does would refuse this on Windows and accept it on Linux.
		"Jobs/" + other + ".json",
		`jobs\` + other + ".json",
		"JOBS/" + other + ".json",
		"models/../jobs/" + other + ".json",
		"Supervisor.json",
		"WORK/" + other,
	} {
		if err := ReservedSink(me, p); !errors.Is(err, ErrReservedPath) {
			t.Errorf("ReservedSink(%q) did not refuse: %v", p, err)
		}
	}

	// A job's own scratch is the one reserved path it may write — the default
	// partial goes there — and nothing else about the store is spellable.
	for _, p := range []string{
		"",
		"work/" + me,
		"work/" + me + "/part",
		"models/x.gguf",
		"jobsy/x.json",
		"a/jobs/x.json",
		"services.json.bak",
		`D:\models\x.gguf`,
		"/mnt/models/x.gguf",
	} {
		if err := ReservedSink(me, p); err != nil {
			t.Errorf("ReservedSink(%q, %q) refused a legitimate sink: %v", me, p, err)
		}
	}
}

// The wording, spelled out because Python and C++ print this same string.
func TestReservedRefusalNamesThePathFromTheRecord(t *testing.T) {
	err := ReservedSink("1757000000000-deadbeef", "jobs/1757000000001-cafebabe.json")
	if err == nil {
		t.Fatal("no error")
	}
	const want = "download: sink path is reserved by the store: jobs/1757000000001-cafebabe.json"
	if err.Error() != want {
		t.Fatalf("refusal reads\n  %s\nwant\n  %s", err, want)
	}
}

// Refused at submission, and on the side that does the writing, exactly like an
// escaping sink: a record can arrive in a shared store without passing through
// any Submit of ours.
func TestSubmitAndLocalSinkRefuseAReservedSink(t *testing.T) {
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	src := []Source{{Scheme: "https", Locator: "https://example.invalid/x.gguf"}}

	for _, sink := range []Sink{
		{Final: "jobs/1757000000001-cafebabe.json"},
		{Final: "services.json"},
		{Final: "models/x.gguf", Partial: "jobs/1757000000001-cafebabe.json"},
		{Final: "models/x.gguf", Partial: "work/1757000000001-cafebabe"},
	} {
		if _, err := Submit(store, Spec{Sources: src, Sink: sink}); !errors.Is(err, ErrReservedPath) {
			t.Errorf("Submit accepted %+v: %v", sink, err)
		}
	}

	// The id Submit chose owns its own scratch, so the partial it invents for
	// itself is accepted — this is the case a blanket ban on work/ would break.
	id, err := Submit(store, Spec{Sources: src, Sink: Sink{Final: "models/x.gguf"}})
	if err != nil {
		t.Fatalf("Submit refused a job the partial it chose itself: %v", err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := SpecOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Sink.Partial != "work/"+id {
		t.Fatalf("partial is %q, want work/%s", spec.Sink.Partial, id)
	}
	if _, _, err := LocalSink(store, id, spec.Sink); err != nil {
		t.Fatalf("LocalSink refused the job its own partial: %v", err)
	}
	// And the same record, read on behalf of any other job, is refused.
	if _, _, err := LocalSink(store, "1757000000001-cafebabe", spec.Sink); !errors.Is(err, ErrReservedPath) {
		t.Fatalf("another job was allowed to write this one's partial: %v", err)
	}
}

// An absolute sink is honoured only by a machine whose convention it is written
// in.
//
// The contract claimed a foreign absolute path "fails with no such file rather
// than quietly creating a directory". That is true of reading and false of a
// sink: O_CREAT on `D:\models\x.gguf` under Linux makes a file of that literal
// name in the working directory, with a `.part` beside it.
func TestAbsoluteSinkIsRefusedOnTheOtherPlatform(t *testing.T) {
	windows := []string{`D:\models\x.gguf`, `\nas\share\x.gguf`, `c:\models\x.gguf`}
	posix := []string{"/mnt/models/x.gguf", "/models/x.gguf"}

	native, foreign := posix, windows
	if runtime.GOOS == "windows" {
		native, foreign = windows, posix
	}
	for _, p := range foreign {
		if err := ForeignPath(p); !errors.Is(err, ErrForeignPath) {
			t.Errorf("ForeignPath(%q) on %s did not refuse: %v", p, runtime.GOOS, err)
		}
	}
	for _, p := range native {
		if err := ForeignPath(p); err != nil {
			t.Errorf("ForeignPath(%q) on %s refused this machine's own convention: %v", p, runtime.GOOS, err)
		}
	}
	// Relative paths are the portable form and are never this check's business.
	for _, p := range []string{"", "models/x.gguf", "work/abc"} {
		if err := ForeignPath(p); err != nil {
			t.Errorf("ForeignPath(%q) = %v", p, err)
		}
	}

	// The refusal reaches the side that writes, which is the point: a record
	// naming a drive letter is valid on the machine that wrote it and must not
	// be acted on by one that cannot reach it.
	if _, _, err := (Sink{Final: foreign[0]}).Resolve(`C:\store`, "abc"); !errors.Is(err, ErrForeignPath) {
		t.Errorf("Resolve accepted a foreign absolute sink: %v", err)
	}
	if err := (Sink{Final: foreign[0]}).errorFromValidate(); err != nil {
		t.Errorf("a foreign absolute sink is a valid RECORD and Validate refused it: %v", err)
	}
}

// A record has no platform, so submission may not refuse a path for being
// absolute somewhere else — only the machine about to write it may.
func (s Sink) errorFromValidate() error {
	return Spec{
		Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/x"}},
		Sink:    s,
	}.Validate()
}
