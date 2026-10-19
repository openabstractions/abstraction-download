package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	archive "github.com/openabstractions/abstraction-download/go/archive"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// archiveFolder points the archive at a fresh state folder and a fake
// archive folder with body under its content name.
func archiveFolder(t *testing.T, entry []byte, sha string) string {
	t.Helper()
	archive.UseState(filepath.Join(t.TempDir(), "state"))
	t.Cleanup(func() { archive.UseState("") })
	folder := filepath.Join(t.TempDir(), "nas", "archive")
	path := filepath.Join(folder, "sha256", sha)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, entry, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := archive.SetFolder(context.Background(), folder); err != nil {
		t.Fatal(err)
	}
	return path
}

// countingServer serves body and counts every request.
func countingServer(t *testing.T, body []byte) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.ServeContent(w, r, "model.gguf", time.Unix(1, 0), bytes.NewReader(body))
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

func completed(t *testing.T, p *acceptanceprovider.Provider, key string, body []byte, url string, declare bool) []byte {
	t.Helper()
	id, _ := submitDownload(t, p, key, 0, body, declare, url)
	waitFor(t, key+" to complete", 20*time.Second, func() bool {
		return observe(t, p, id).State.String() == "complete"
	})
	return readAll(t, p, id)
}

// The download service started on its job store fills a download that
// names its SHA-256 from the archive, with no request to its source; a
// download of unknown size is found by its content name too
// [DL-R44].
func TestHTTPExecutionServesAnArchivedFileWithoutTheNetwork(t *testing.T) {
	body := testBody(3<<20 + 17)
	sum := sha256.Sum256(body)
	archiveFolder(t, body, hex.EncodeToString(sum[:]))
	server, hits := countingServer(t, body)
	for _, declare := range []bool{true, false} {
		// Each download gets its own job store, so no earlier result serves it.
		p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{Archive: archive.Service{}})
		if err != nil {
			t.Fatal(err)
		}
		execute(t, p)
		key := "archived-sized"
		if !declare {
			key = "archived-unsized"
		}
		if got := completed(t, p, key, body, server.URL+"/model.gguf", declare); !bytes.Equal(got, body) {
			t.Fatalf("%s: the result is not the archived bytes", key)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("an archived download reached its source %d times", n)
	}
}

// An archive entry whose bytes differ from the digest moves to
// sha256\.bad\<hash> with one log line, and the download comes from its
// source with the source's bytes.
func TestHTTPExecutionFallsBackFromADifferingArchiveFile(t *testing.T) {
	body := testBody(2<<20 + 5)
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	wrong := bytes.Clone(body)
	wrong[1000] ^= 0xff
	entry := archiveFolder(t, wrong, sha)
	server, hits := countingServer(t, body)
	var logged bytes.Buffer
	var mu = make(chan struct{}, 1)
	mu <- struct{}{}
	saved := log.Writer()
	log.SetOutput(lockedWriter{&logged, mu})
	t.Cleanup(func() { log.SetOutput(saved) })
	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{Archive: archive.Service{}})
	if err != nil {
		t.Fatal(err)
	}
	execute(t, p)
	if got := completed(t, p, "differing", body, server.URL+"/model.gguf", true); !bytes.Equal(got, body) {
		t.Fatal("the result is not the source's bytes")
	}
	if hits.Load() == 0 {
		t.Fatal("the download did not go to its source")
	}
	<-mu
	text := logged.String()
	mu <- struct{}{}
	if n := strings.Count(text, "differ from the wanted SHA-256"); n != 1 {
		t.Fatalf("want one log line about the differing file, got %q", text)
	}
	bad := filepath.Join(filepath.Dir(entry), ".bad", sha)
	if got, err := os.ReadFile(bad); err != nil || !bytes.Equal(got, wrong) {
		t.Fatalf("the differing entry was not moved to %s: %v", bad, err)
	}
}

// No app-visible answer of the download service names the archive folder:
// the receipt, progress, failure and result of a job served from the
// archive, of one that fell back from a differing entry, and of one whose
// source then failed. OA holds no NAS credential; Windows authenticates the
// share with the person's logon session.
func TestArchiveFolderStaysOutOfEveryAppVisibleAnswer(t *testing.T) {
	const marker = "nas-private-marker"
	body := testBody(1<<20 + 3)
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	archive.UseState(filepath.Join(t.TempDir(), "state"))
	t.Cleanup(func() { archive.UseState("") })
	folder := filepath.Join(t.TempDir(), marker, "archive")
	if err := archive.SetFolder(context.Background(), folder); err != nil {
		t.Fatal(err)
	}
	put := func(content []byte) {
		path := filepath.Join(folder, "sha256", sha)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	missing := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(missing.Close)
	server, _ := countingServer(t, body)
	wrong := bytes.Clone(body)
	wrong[7] ^= 0xff
	for _, c := range []struct {
		name, url string
		entry     []byte
		state     string
	}{
		{"served", missing.URL + "/model.gguf", body, "complete"},
		{"fallback", server.URL + "/model.gguf", wrong, "complete"},
		{"failed", missing.URL + "/model.gguf", wrong, "failed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			put(c.entry)
			p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{Archive: archive.Service{}})
			if err != nil {
				t.Fatal(err)
			}
			execute(t, p)
			id, _ := submitDownload(t, p, c.name, 0, body, true, c.url)
			var answers []any
			waitFor(t, c.name+" to end", 20*time.Second, func() bool {
				v, err := p.BindOperations("alice").ObserveWork(id)
				answers = append(answers, v, fmt.Sprint(err))
				return v.Snapshot != nil && v.Snapshot.State.String() == c.state
			})
			read, err := p.BindOperations("alice").ReadResult(id, 0, 1<<20)
			answers = append(answers, read, fmt.Sprint(err))
			window, err := p.Bind("alice").GetHistoryWindow()
			answers = append(answers, window, fmt.Sprint(err))
			raw, err := json.Marshal(answers)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), marker) || strings.Contains(strings.ToLower(string(raw)), strings.ToLower(filepath.ToSlash(folder))) {
				t.Fatalf("an app-visible answer names the archive folder: %s", raw)
			}
		})
	}
}

type lockedWriter struct {
	buf *bytes.Buffer
	mu  chan struct{}
}

func (w lockedWriter) Write(p []byte) (int, error) {
	<-w.mu
	defer func() { w.mu <- struct{}{} }()
	return w.buf.Write(p)
}

// recordedHash is the SHA-256 the stored listing records for the archive
// path rel, or "".
func recordedHash(t *testing.T, rel string) string {
	t.Helper()
	listing, ok := archive.ReadListing()
	if !ok {
		t.Fatal("the archive has no stored listing")
	}
	for _, file := range listing.Files {
		if file.Path == rel {
			return file.SHA256
		}
	}
	t.Fatalf("the listing has no %s", rel)
	return ""
}

// The owner's NAS case through the download service: a model kept under its
// plain name is served by size, verified, and its SHA-256 recorded against
// the file. The next download finds it by that recorded hash before a
// same-size file that sorts first, which is never read. A changed file
// loses its record and the download goes to its source.
func TestHTTPExecutionServesAPlainNamedArchiveFileAndRecordsItsHash(t *testing.T) {
	body := testBody(2<<20 + 11)
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	archive.UseState(filepath.Join(t.TempDir(), "state"))
	t.Cleanup(func() { archive.UseState("") })
	folder := filepath.Join(t.TempDir(), "AI Models")
	const rel = "OA-models/comfyui/wan2.2-ti2v-5b/wan2.2_ti2v_5B_fp16.safetensors"
	model := filepath.Join(folder, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(model), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(model, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := archive.SetFolder(context.Background(), folder); err != nil {
		t.Fatal(err)
	}
	server, hits := countingServer(t, body)
	download := func(key string) []byte {
		p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{Archive: archive.Service{}})
		if err != nil {
			t.Fatal(err)
		}
		execute(t, p)
		return completed(t, p, key, body, server.URL+"/wan2.2_ti2v_5B_fp16.safetensors", true)
	}

	if got := download("plain-first"); !bytes.Equal(got, body) || hits.Load() != 0 {
		t.Fatalf("the plain-named file was not served from the archive (%d source requests)", hits.Load())
	}
	if got := recordedHash(t, rel); got != sha {
		t.Fatalf("the served file's SHA-256 is %q in the listing, want %s", got, sha)
	}

	decoyRel := "OA-models/a-decoy.safetensors"
	decoy := bytes.Clone(body)
	decoy[5] ^= 0xff
	if err := os.WriteFile(filepath.Join(folder, filepath.FromSlash(decoyRel)), decoy, 0o600); err != nil {
		t.Fatal(err)
	}
	<-archive.Look()
	if got := download("plain-second"); !bytes.Equal(got, body) || hits.Load() != 0 {
		t.Fatalf("the recorded file was not served from the archive (%d source requests)", hits.Load())
	}
	if got := recordedHash(t, decoyRel); got != "" {
		t.Fatalf("the lookup read a same-size file before the recorded one: %s", got)
	}

	changed := bytes.Clone(body)
	changed[9] ^= 0xff
	if err := os.WriteFile(model, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(model, later, later); err != nil {
		t.Fatal(err)
	}
	if got := download("plain-changed"); !bytes.Equal(got, body) || hits.Load() == 0 {
		t.Fatal("a changed archive file was trusted by its old record")
	}
	if got := recordedHash(t, rel); got == sha {
		t.Fatal("the changed file keeps the old SHA-256")
	}
}
