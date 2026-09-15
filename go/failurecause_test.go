package download

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

// causes reads testdata/failures/causes.txt: the typed cause each record's
// failure@2 payload carries. A record the table does not name carries none.
func causes(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "testdata", "failures", "causes.txt"))
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
		t.Fatal("causes.txt names no record, so this test asserts nothing")
	}
	return want
}

// The runner as the service runs it writes failure@2 beside failure@1, and
// LastFailureCause reads the typed cause back without touching the class.
func TestTheServiceRunnerWritesACauseThisReaderReads(t *testing.T) {
	body, digest := payload(t, 512)
	wrong := append([]byte(nil), body...)
	wrong[0] ^= 0xff
	srv := serveOnce(t, wrong)
	r, store, root := newRunner(t)
	r.RecordCause = true
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/p.bin"})
	if err := r.Run(context.Background(), id); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Run = %v, want the digest mismatch", err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rec.Extensions[FailureCauseExtension]; !ok {
		t.Fatalf("the runner wrote no %s: %v", FailureCauseExtension, rec.Extensions)
	}
	if got := LastFailureCause(rec); got != "digest_mismatch" {
		t.Fatalf("LastFailureCause = %q, want digest_mismatch", got)
	}
	if got := class(LastFailure(rec)); got != "retryable" {
		t.Fatalf("the cause changed the class: %s", got)
	}
}

// Every record in the failure corpus yields the cause causes.txt names, or ""
// when it names none -- including the record the Go service runner wrote.
func TestAFailureCauseFromTheCorpusReadsTheSameHere(t *testing.T) {
	want := causes(t)
	for name := range corpus(t) {
		b, err := os.ReadFile(filepath.Join("..", "testdata", "failures", name+".json"))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		rec, err := job.Decode(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := LastFailureCause(rec); got != want[name] {
			t.Errorf("%s carries cause %q, and causes.txt says %q", name, got, want[name])
		}
	}
	for name := range want {
		if _, ok := corpus(t)[name]; !ok {
			t.Errorf("causes.txt names %s and expect.txt does not", name)
		}
	}
}
