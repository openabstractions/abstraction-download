package download

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// The corpus is records as they were WRITTEN, half of them by the published
// go/v0.1.0 tag and half by this tree, and this asks whether this code still
// makes the same thing of all of them. It is the half of the question a test in
// this module can answer; the other half — whether v0.1.0 still reads what we
// write now — needs v0.1.0 running, and lives in research/wire-compat.
func TestRecordCorpus(t *testing.T) {
	dir := filepath.Join("..", "testdata", "records")
	for _, want := range readExpectations(t, filepath.Join(dir, "expect.txt")) {
		t.Run(want["name"], func(t *testing.T) {
			b, err := os.ReadFile(filepath.Join(dir, want["name"]+".json"))
			if err != nil {
				t.Fatal(err)
			}
			rec, err := job.Decode(b)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			spec, err := SpecOf(rec)
			if err != nil {
				t.Fatalf("spec: %v", err)
			}
			cp, err := CheckpointOf(rec)
			if err != nil {
				t.Fatalf("checkpoint: %v", err)
			}
			got := map[string]string{
				"state":    string(rec.State),
				"partial":  spec.Sink.Partial,
				"final":    spec.Sink.Final,
				"prefix":   fmt.Sprint(cp.VerifiedPrefix),
				"verified": strings.ReplaceAll(fmt.Sprint(cp.Verified), " ", ";"),
				"resolve":  resolveVerdict(spec.Sink, rec.ID),
			}
			for _, field := range []string{"state", "partial", "final", "prefix", "verified", "resolve"} {
				if got[field] != want[field] {
					t.Errorf("%s: written by %s as %q, read now as %q", field, want["by"], want[field], got[field])
				}
			}
		})
	}
}

func resolveVerdict(sink Sink, owner string) string {
	_, _, err := sink.Resolve("/probe-root", owner)
	switch {
	case err == nil:
		return "ok"
	case strings.Contains(err.Error(), ErrEscapesRoot.Error()):
		return "escapes-root"
	case strings.Contains(err.Error(), ErrReservedPath.Error()):
		return "reserved"
	case strings.Contains(err.Error(), ErrForeignPath.Error()):
		return "foreign"
	}
	return err.Error()
}

func readExpectations(t *testing.T, path string) []map[string]string {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fields := []string{"name", "by", "state", "partial", "final", "prefix", "verified", "resolve"}
	var out []map[string]string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		cols := strings.Fields(line)
		if len(cols) != len(fields) {
			t.Fatalf("%q: %d columns, want %d", line, len(cols), len(fields))
		}
		row := map[string]string{}
		for i, name := range fields {
			row[name] = cols[i]
		}
		out = append(out, row)
	}
	if len(out) == 0 {
		t.Fatal("the corpus is empty")
	}
	return out
}
