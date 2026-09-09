package download

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// The pairs our three implementations were measured disagreeing about, on
// three filesystems that also disagree with each other. Written as escapes
// because two of these pairs are indistinguishable on screen, and that is the
// point of them.
//
// No expected column: the filesystem under the test is asked in the same run,
// because that is the contract. SamePath answers what the volume answers, and
// nothing here knows in advance what this volume is.
var spellings = []struct {
	name, a, b string
}{
	{"ascii case", "Kelvin.gguf", "kelvin.gguf"},
	{"kelvin sign", "Kelvin.gguf", "kelvin.gguf"},
	{"latin case", "café.gguf", "CAFÉ.gguf"},
	{"nfc vs nfd", "café.gguf", "café.gguf"},
	{"dotted I", "İ.gguf", "i.gguf"},
	{"sharp s", "straße.gguf", "strasse.gguf"},
	{"unrelated", "llama.gguf", "mistral.gguf"},
}

// merges reports what the volume holding dir does with the two names: it makes
// one and looks for the other.
func merges(t *testing.T, dir, a, b string) bool {
	t.Helper()
	made := filepath.Join(dir, a)
	f, err := os.OpenFile(made, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Skipf("this volume will not hold %q: %v", a, err)
	}
	f.Close()
	defer os.Remove(made)
	mine, err := os.Stat(made)
	if err != nil {
		t.Fatalf("stat %q: %v", made, err)
	}
	other, err := os.Stat(filepath.Join(dir, b))
	return err == nil && os.SameFile(mine, other)
}

// TestSamePathAgreesWithTheVolume is the whole rule. A destination that does not
// exist yet is the normal case — that is what a download is — so the pair is
// asked about before either file is created, and checked against what the volume
// then does with them.
func TestSamePathAgreesWithTheVolume(t *testing.T) {
	for _, c := range spellings {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			got := SamePath(filepath.Join(dir, c.a), filepath.Join(dir, c.b))
			want := merges(t, dir, c.a, c.b)
			if got != want {
				t.Fatalf("SamePath(%q, %q) = %v; the volume says %v", c.a, c.b, got, want)
			}
		})
	}
}

// TestSamePathAgreesWhenTheFileIsThere covers the same pairs once one of them
// exists, which is the case os.SameFile answers outright.
func TestSamePathAgreesWhenTheFileIsThere(t *testing.T) {
	for _, c := range spellings {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			want := merges(t, dir, c.a, c.b)
			made := filepath.Join(dir, c.a)
			f, err := os.OpenFile(made, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				t.Skipf("this volume will not hold %q: %v", c.a, err)
			}
			f.Close()
			if got := SamePath(made, filepath.Join(dir, c.b)); got != want {
				t.Fatalf("SamePath(%q, %q) = %v; the volume says %v", c.a, c.b, got, want)
			}
		})
	}
}

// A directory that does not exist yet either. The spelling of the missing
// directory is settled by the nearest ancestor that does exist, because that is
// the directory whose rule the new one will inherit.
func TestSamePathThroughAMissingDirectory(t *testing.T) {
	dir := t.TempDir()
	want := merges(t, dir, "Models", "models")
	got := SamePath(filepath.Join(dir, "Models", "x.gguf"), filepath.Join(dir, "models", "x.gguf"))
	if got != want {
		t.Fatalf("SamePath through a missing directory = %v; the volume says %v", got, want)
	}
}

func TestSamePathLeavesNothingBehind(t *testing.T) {
	dir := t.TempDir()
	SamePath(filepath.Join(dir, "Kelvin.gguf"), filepath.Join(dir, "kelvin.gguf"))
	left, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("asking left %d entries behind: %v", len(left), left)
	}
}

func TestSamePathOnObviousCases(t *testing.T) {
	dir := t.TempDir()
	if !SamePath(filepath.Join(dir, "a.gguf"), filepath.Join(dir, "a.gguf")) {
		t.Fatal("one spelling of one path is not the same file as itself")
	}
	if SamePath("", filepath.Join(dir, "a.gguf")) {
		t.Fatal("no destination matched a destination")
	}
	if !SamePath("", "") {
		t.Fatal("no destination did not match no destination")
	}
	if !SamePath(filepath.Join(dir, "a", "..", "b.gguf"), filepath.Join(dir, "b.gguf")) {
		t.Fatal("a path that climbs back to itself is a different file")
	}
}

func BenchmarkSamePathSameSpelling(b *testing.B) {
	p := filepath.Join(b.TempDir(), "llama-7b-q4.gguf")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SamePath(p, p)
	}
}

func BenchmarkSamePathUnrelated(b *testing.B) {
	dir := b.TempDir()
	x := filepath.Join(dir, "llama-7b-q4.gguf")
	y := filepath.Join(dir, "mistral-7b-q4.gguf")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SamePath(x, y)
	}
}

func BenchmarkSamePathCandidateCached(b *testing.B) {
	dir := b.TempDir()
	x := filepath.Join(dir, "Kelvin.gguf")
	y := filepath.Join(dir, "kelvin.gguf")
	SamePath(x, y)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SamePath(x, y)
	}
}

func BenchmarkSamePathProbe(b *testing.B) {
	dir := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := strconv.Itoa(i)
		foldsTogether(dir, "Kelvin"+n+".gguf", "kelvin"+n+".gguf")
	}
}

func BenchmarkComparablePathPair(b *testing.B) {
	dir := b.TempDir()
	x := filepath.Join(dir, "llama-7b-q4.gguf")
	y := filepath.Join(dir, "mistral-7b-q4.gguf")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = comparablePath(x) == comparablePath(y)
	}
}
