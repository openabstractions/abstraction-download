package download

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// TestRefusalLeavesNoEmptyPartial covers the litter a permanent refusal used to
// leave: the working file is opened before the first byte arrives, so every
// status dl records as failed left a zero-byte .part beside the destination
// that no record explained and no run would ever resume.
func TestRefusalLeavesNoEmptyPartial(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusTooManyRequests} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
		}))
		defer srv.Close()

		r, store, root := newRunner(t)
		id := submit(t, store, root, "", 0, Source{Scheme: "https", Locator: srv.URL})
		if err := r.Run(context.Background(), id); err == nil {
			t.Fatalf("status %d: Run succeeded, want a refusal", status)
		}
		if _, err := os.Stat(partialOf(t, store, id)); !os.IsNotExist(err) {
			t.Fatalf("status %d: a partial of no bytes survived the refusal", status)
		}
	}
}

// A partial that holds bytes is never taken away, whatever the refusal.
func TestRefusalKeepsAProvenPrefix(t *testing.T) {
	body, digest := payload(t, 8<<10)
	served := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served++
		if served > 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Length", "16384")
		w.Write(body[:2048])
	}))
	defer srv.Close()

	r, store, root := newRunner(t)
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "https", Locator: srv.URL})
	if err := r.Run(context.Background(), id); err == nil {
		t.Fatal("Run succeeded, want a short transfer")
	}
	fi, err := os.Stat(partialOf(t, store, id))
	if err != nil {
		t.Fatalf("the partial is gone: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("the partial holds no bytes")
	}
}
