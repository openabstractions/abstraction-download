package download

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseWantedRefusals(t *testing.T) {
	cases := map[string]string{
		"":                                 "nothing to fetch",
		"# only a comment\n":               "nothing to fetch",
		"not-a-url\n":                      "line 1: not a URL",
		"https://x/a sha256:abc\n":         "digest is not sha256",
		"https://x/a one two\n":            "one destination per line",
		"https://x/a ../out.bin\n":         "escapes the store",
		"https://x/a /etc/cron.d/x\n":      "outside the store",
		"https://x/a C:/Users/x\n":         "outside the store",
		"https://x/a jobs/x.json\n":        "reserved by the store",
		"https://x/a work/other\n":         "reserved by the store",
		"https://x/a supervisor.json\n":    "reserved by the store",
		"https://x/a wanted/again.txt\n":   "the drop folder itself",
		"https://x/a a/../Wanted./b\n":     "the drop folder itself",
		"file:///etc/shadow\n":             "only http and https",
		"smb://nas/share/x\n":              "only http and https",
		"https://ok/a\nhttps://x/b ../c\n": "line 2: destination escapes",
		"{not json":                        "not a spec",
		`{"sources":[{"scheme":"file","locator":"/etc/shadow"}],"sink":{"final":"files/x"}}`:                              "only http and https",
		`{"sources":[{"scheme":"https","locator":"https://x/a","attrs":{"credential":"hf"}}],"sink":{"final":"files/x"}}`: "names no credential",
		`{"sources":[{"scheme":"https","locator":"https://x/a","headers":{"X-A":"b"}}],"sink":{"final":"files/x"}}`:       "sets no header",
		`{"sources":[{"scheme":"https","locator":"https://x/a"}],"sink":{"final":"/store/files/x"}}`:                      "outside the store",
		`{"sources":[{"scheme":"https","locator":"https://x/a"}],"sink":{"final":"files/x","partial":"jobs/y"}}`:          "reserved by the store",
		`{"sources":[{"scheme":"https","locator":"https://x/a"}],"sink":{"final":""}}`:                                    "final path is required",
	}
	for text, want := range cases {
		_, err := ParseWanted(text)
		if !errors.Is(err, ErrRequestRefused) || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: want refusal containing %q, got %v", text, want, err)
		}
	}
}

func TestParseWantedLines(t *testing.T) {
	digest := "sha256:" + strings.Repeat("ab", 32)
	specs, err := ParseWanted("# a request\r\n\r\nhttps://x/dir/model.gguf?dl=1\n" +
		"http://y/b.bin " + strings.ToUpper(digest) + " isos/\n" +
		"https://z/c.bin models\\c.bin " + strings.Repeat("ab", 32) + "\n")
	if err != nil {
		t.Fatal(err)
	}
	want := []struct{ scheme, locator, final, digest string }{
		{"https", "https://x/dir/model.gguf?dl=1", "files/model.gguf", ""},
		{"http", "http://y/b.bin", "isos/b.bin", digest},
		{"https", "https://z/c.bin", "models/c.bin", digest},
	}
	if len(specs) != len(want) {
		t.Fatalf("got %d specs, want %d", len(specs), len(want))
	}
	for i, w := range want {
		s := specs[i]
		if s.Sources[0].Scheme != w.scheme || s.Sources[0].Locator != w.locator || s.Sink.Final != w.final || s.Artifact.Digest != w.digest {
			t.Errorf("line %d: got %+v, want %+v", i+1, s, w)
		}
	}
}

func TestParseWantedSpec(t *testing.T) {
	specs, err := ParseWanted("# a spec, commented\n{\n \"artifact\": {\"size\": 5},\n \"sources\": [{\"scheme\": \"https\", \"locator\": \"https://x/a\"}],\n \"sink\": {\"final\": \"files/a\"}\n}\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 || specs[0].Artifact.Size != 5 || specs[0].Sink.Final != "files/a" {
		t.Fatalf("got %+v", specs)
	}
}

// folder is a drop folder over a store whose Submit records a job with a
// file: source, which the folder itself would refuse: the folder's rules are
// tested above, and this tests what it does with a job once it has one.
func folder(t *testing.T, r *Runner, root string) (Wanted, string) {
	t.Helper()
	body := []byte("hello, wanted")
	src := filepath.Join(root, "artifact.bin")
	if err := os.WriteFile(src, body, 0o644); err != nil {
		t.Fatal(err)
	}
	w := Wanted{Store: r.Store, Submit: func(s Spec) (string, error) {
		s.Sources = []Source{{Scheme: "file", Locator: src}}
		return Submit(r.Store, s)
	}}
	dir, err := w.Dir()
	if err != nil {
		t.Fatal(err)
	}
	return w, dir
}

func names(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return strings.Join(out, " ")
}

func TestWantedAnswersInPlace(t *testing.T) {
	r, _, root := newRunner(t)
	w, dir := folder(t, r, root)
	drop := func(name, text string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	drop("good.txt", "https://x/one.bin\r\nhttps://x/two.bin models/two.bin\r\n")
	drop("bad.txt", "https://x/a ../escape\n")
	drop(".hidden", "https://x/a\n")
	drop("desktop.ini", "[.ShellClassInfo]\n")
	drop("big.txt", strings.Repeat("https://x/a\n", requestLimit/8))

	ids, err := w.TakeIn()
	if err != nil || len(ids) != 2 {
		t.Fatalf("took %v, %v", ids, err)
	}
	if got := names(t, dir); got != ".hidden bad.txt.refused big.txt.refused desktop.ini good.txt.accepted" {
		t.Fatalf("after take-in: %s", got)
	}
	refused, _ := os.ReadFile(filepath.Join(dir, "bad.txt.refused"))
	if !strings.HasPrefix(string(refused), "https://x/a ../escape\n# refused ") || !strings.Contains(string(refused), "line 1: destination escapes the store") {
		t.Fatalf("refused answer:\n%s", refused)
	}
	big, _ := os.ReadFile(filepath.Join(dir, "big.txt.refused"))
	if strings.Contains(string(big), "#") {
		t.Fatal("an oversized file was rewritten")
	}
	accepted, _ := os.ReadFile(filepath.Join(dir, "good.txt.accepted"))
	if !strings.Contains(string(accepted), "# job "+ids[0]+" -> files/one.bin\n# job "+ids[1]+" -> models/two.bin\n") {
		t.Fatalf("accepted answer:\n%s", accepted)
	}

	if err := w.Answer(); err != nil {
		t.Fatal(err)
	}
	accepted, _ = os.ReadFile(filepath.Join(dir, "good.txt.accepted"))
	if !strings.HasSuffix(string(accepted), "# pending\n# pending\n") {
		t.Fatalf("progress:\n%s", accepted)
	}
	if _, err := r.Adopt(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.Answer(); err != nil {
		t.Fatal(err)
	}
	if got := names(t, dir); got != ".hidden bad.txt.refused big.txt.refused desktop.ini good.txt.done" {
		t.Fatalf("after delivery: %s", got)
	}
	done, _ := os.ReadFile(filepath.Join(dir, "good.txt.done"))
	if !strings.Contains(string(done), ": files/one.bin, 13 bytes\n") || !strings.Contains(string(done), ": models/two.bin, 13 bytes\n") {
		t.Fatalf("done answer:\n%s", done)
	}
	if _, err := os.Stat(filepath.Join(root, "models", "two.bin")); err != nil {
		t.Fatal(err)
	}

	if ids, _ := w.TakeIn(); len(ids) != 0 {
		t.Fatalf("a second sweep took %v", ids)
	}
}

func TestWantedFailed(t *testing.T) {
	r, _, root := newRunner(t)
	w, dir := folder(t, r, root)
	w.Submit = func(s Spec) (string, error) {
		s.Sources = []Source{{Scheme: "gopher", Locator: "gopher://example.invalid/x"}}
		return Submit(r.Store, s)
	}
	os.WriteFile(filepath.Join(dir, "gone.txt"), []byte("https://x/gone.bin\n"), 0o644)
	if ids, _ := w.TakeIn(); len(ids) != 1 {
		t.Fatal("not taken")
	}
	r.Adopt(context.Background())
	w.Answer()
	if got := names(t, dir); got != "gone.txt.failed" {
		t.Fatalf("after failure: %s", got)
	}
	failed, _ := os.ReadFile(filepath.Join(dir, "gone.txt.failed"))
	if !strings.Contains(string(failed), "# failed ") {
		t.Fatalf("failed answer:\n%s", failed)
	}
}

func TestWantedRetryByRename(t *testing.T) {
	r, _, root := newRunner(t)
	w, dir := folder(t, r, root)
	os.WriteFile(filepath.Join(dir, "r.txt"), []byte("https://x/a ../out\n"), 0o644)
	w.TakeIn()
	os.WriteFile(filepath.Join(dir, "r.txt"), []byte("https://x/a\n# refused 2026-09-06T00:00:00Z: line 1: destination escapes the store\n"), 0o644)
	os.Remove(filepath.Join(dir, "r.txt.refused"))
	if ids, _ := w.TakeIn(); len(ids) != 1 {
		t.Fatal("a corrected request was not taken")
	}
	accepted, _ := os.ReadFile(filepath.Join(dir, "r.txt.accepted"))
	if strings.Contains(string(accepted), "# refused 2026") {
		t.Fatalf("the old answer survived:\n%s", accepted)
	}
}
