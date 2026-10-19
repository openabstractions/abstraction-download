package download

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// TestDeliverEachClosesAJobBeforeTheNextRuns: Adopt runs a sweep's jobs one
// after another. With DeliverEach a finished job is COMPLETE while the next
// one transfers; without it every finished job waits as TRANSFERRED until the
// sweep ends. Ten picture-model files submitted together once showed one of
// ten complete for five minutes while a second sat finished on disk behind a
// 3.8 GB third.
func TestDeliverEachClosesAJobBeforeTheNextRuns(t *testing.T) {
	for _, each := range []bool{true, false} {
		t.Run(fmt.Sprintf("DeliverEach=%v", each), func(t *testing.T) {
			r, store, root := newRunner(t)
			r.DeliverEach = each
			bodies := map[string][]byte{}
			var mu sync.Mutex
			var waiting []string // jobs seen TRANSFERRED while another job fetched
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				recs, _ := store.List()
				mu.Lock()
				for _, rec := range recs {
					if rec.State == job.StateTransferred {
						waiting = append(waiting, rec.ID)
					}
				}
				mu.Unlock()
				http.ServeContent(w, req, "blob.bin", time.Unix(0, 0), strings.NewReader(string(bodies[req.URL.Path])))
			}))
			t.Cleanup(srv.Close)
			var ids []string
			for i := 0; i < 3; i++ {
				body, digest := payload(t, 4<<10)
				path := fmt.Sprintf("/file%d", i)
				bodies[path] = body
				id, err := Submit(store, Spec{
					Artifact: Artifact{Digest: digest, Size: int64(len(body))},
					Sources:  []Source{{Scheme: "https", Locator: srv.URL + path}},
					Sink:     Sink{Final: filepath.Join(root, fmt.Sprintf("final%d.bin", i))},
				})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			n, err := r.Adopt(context.Background())
			if err != nil || n != 3 {
				t.Fatalf("Adopt = %d, %v; want 3 jobs run", n, err)
			}
			want := job.StateComplete
			if !each {
				want = job.StateTransferred
			}
			for _, id := range ids {
				rec, err := store.Load(id)
				if err != nil {
					t.Fatal(err)
				}
				if rec.State != want {
					t.Fatalf("%s after the sweep is %s, want %s", id, rec.State, want)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if each && len(waiting) != 0 {
				t.Fatalf("jobs %v waited TRANSFERRED while a later job fetched", waiting)
			}
			if !each && len(waiting) == 0 {
				t.Fatal("without DeliverEach a finished job waits TRANSFERRED behind the next; the test no longer exposes the wait")
			}
		})
	}
}
