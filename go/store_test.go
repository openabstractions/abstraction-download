package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	job "github.com/ReinisLusis/abstraction/job/go"
	storage "github.com/ReinisLusis/abstraction/storage/go"
)

// The 116 GB question, answered: bytes another tool on this machine already
// holds must not be fetched again.
//
// The server counts requests. If it is touched at all, the dedup did not
// happen — and the point is not that it was fast, it is that the network was
// never involved.
func TestBytesAnotherToolAlreadyHasAreNotDownloaded(t *testing.T) {
	payload := make([]byte, 1<<20)
	rand.New(rand.NewSource(3)).Read(payload)
	sum := sha256.Sum256(payload)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Write(payload)
	}))
	defer srv.Close()

	// Somebody else's content-addressed cache, with these exact bytes in it.
	theirs := t.TempDir()
	if err := os.WriteFile(filepath.Join(theirs, "sha256-"+hex.EncodeToString(sum[:])), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	stores := storage.New(storage.NewForeign("ollama", theirs, "sha256-"))

	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(store, "worker@host:1")
	svc := NewService(r, WithStorage(stores))

	dest := filepath.Join(t.TempDir(), "model.gguf")
	h, err := svc.Submit(Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(payload))},
		Sources:  []Source{{Scheme: "https", Locator: srv.URL + "/model.gguf"}},
		Sink:     Sink{Final: dest},
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.After(60 * time.Second)
	for {
		rec, err := store.Load(h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if rec.State == job.StateTransferred || rec.State == job.StateComplete {
			break
		}
		if rec.State.Terminal() {
			t.Fatalf("ended %s: %s", rec.State, rec.Error)
		}
		select {
		case <-deadline:
			t.Fatal("never finished")
		case <-time.After(50 * time.Millisecond):
		}
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sum {
		t.Fatal("delivered bytes do not match the digest")
	}
	if n := atomic.LoadInt64(&hits); n != 0 {
		t.Fatalf("the network was used %d time(s) for bytes already on this disk", n)
	}
	t.Logf("delivered %d bytes from ollama's cache, %d network requests", len(got), hits)
}

// Without a digest there is nothing to match against, so the network is used —
// which is correct, and worth pinning so the dedup is never mistaken for
// "downloads sometimes do not happen".
func TestWithoutADigestItStillFetches(t *testing.T) {
	payload := []byte("some bytes nobody has indexed")
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Write(payload)
	}))
	defer srv.Close()

	store, _ := job.NewFileStore(t.TempDir())
	svc := NewService(NewRunner(store, "worker@host:1"),
		WithStorage(storage.New(storage.NewForeign("ollama", t.TempDir(), "sha256-"))))

	dest := filepath.Join(t.TempDir(), "thing.bin")
	h, err := svc.Submit(Spec{
		Sources: []Source{{Scheme: "https", Locator: srv.URL + "/thing.bin"}},
		Sink:    Sink{Final: dest},
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(30 * time.Second)
	for {
		rec, _ := store.Load(h.ID())
		if rec != nil && (rec.State == job.StateTransferred || rec.State == job.StateComplete) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("never finished")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if atomic.LoadInt64(&hits) == 0 {
		t.Fatal("nothing was fetched, but nothing on this machine could have matched")
	}
}

var _ = context.Background
