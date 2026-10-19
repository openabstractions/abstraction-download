package download

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

// TestAWeakLastModifiedResumeNeverSplices is the DL-V4 falsifier at the
// runner: two versions within one second never splice. Neither version has an
// ETag. The prefix was taken under version A's Last-Modified, which its own
// Date made weak; by the resume the source serves version B. The resume must
// deliver exactly B and never send a Range without If-Range.
func TestAWeakLastModifiedResumeNeverSplices(t *testing.T) {
	const T = "Wed, 21 Oct 2015 07:28:00 GMT"
	const T60 = "Wed, 21 Oct 2015 07:29:00 GMT"
	for _, c := range []struct {
		name       string
		aLM, aDate string
		bLM, bDate string
	}{
		{"same second", T, T, T, T},
		{"weak then later version", T, T, T60, T60},
		{"no date then later version", T, "", T60, T60},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := []byte(strings.Repeat("A", 40<<10))
			b := []byte(strings.Repeat("B", 40<<10))
			var mu sync.Mutex
			var seen []*http.Request
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				seen = append(seen, r.Clone(context.Background()))
				mu.Unlock()
				w.Header().Set("Last-Modified", c.bLM)
				w.Header().Set("Date", c.bDate)
				w.Header().Set("Accept-Ranges", "bytes")
				// If-Range is matched by exact Last-Modified, as an origin
				// does; a bare Range gets a range of what is held now.
				if ir := r.Header.Get("If-Range"); ir != "" && ir != c.bLM {
					w.Header().Set("Content-Length", fmt.Sprint(len(b)))
					w.Write(b)
					return
				}
				var from int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &from); err == nil {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, len(b)-1, len(b)))
					w.Header().Set("Content-Length", fmt.Sprint(len(b)-from))
					w.WriteHeader(http.StatusPartialContent)
					w.Write(b[from:])
					return
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(b)))
				w.Write(b)
			}))
			t.Cleanup(srv.Close)

			aHeader := http.Header{"Last-Modified": {c.aLM}}
			if c.aDate != "" {
				aHeader.Set("Date", c.aDate)
			}
			r, store, root := newRunner(t)
			id := submit(t, store, root, "", 0, Source{Scheme: "https", Locator: srv.URL})
			stageResume(t, store, id, a[:1000], StrongValidators(aHeader))

			if err := r.Run(context.Background(), id); err != nil {
				t.Fatalf("Run: %v", err)
			}
			got, err := os.ReadFile(finalOf(t, store, id))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(b) {
				t.Fatalf("delivered %d bytes, first byte %q: version A was spliced onto version B", len(got), got[0])
			}
			mu.Lock()
			defer mu.Unlock()
			for _, q := range seen {
				if q.Header.Get("Range") != "" && q.Header.Get("If-Range") == "" {
					t.Fatalf("sent Range %q without If-Range", q.Header.Get("Range"))
				}
			}
		})
	}
}
