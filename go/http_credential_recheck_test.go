package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type revocableSourceCredentials struct {
	mu      sync.Mutex
	revoked bool
	applied int
}

func (*revocableSourceCredentials) Lookup(string, string) (map[string]string, bool) {
	panic("service source credentials used opaque lookup")
}

func (c *revocableSourceCredentials) LookupSource(src Source, _ string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applied++
	if c.revoked {
		return nil, CredentialRefusal(src.Attrs[CredentialAttr], "revoked")
	}
	return map[string]string{"Authorization": "Bearer current", "X-Account-Credential": "current"}, nil
}

// A 416 or a 206 with the wrong offset asks HTTP.Fetch to open a fresh full
// GET. Both service routes recheck current permission before that request.
func TestCredentialAuthorizationRecheckedForRestartedFullGET(t *testing.T) {
	for _, route := range []string{CredentialRouteOriginal, CredentialRouteScoped} {
		for _, status := range []int{http.StatusRequestedRangeNotSatisfiable, http.StatusPartialContent} {
			for _, revoked := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%d/revoked=%t", route, status, revoked), func(t *testing.T) {
					creds := &revocableSourceCredentials{}
					var mu sync.Mutex
					requests := 0
					origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						mu.Lock()
						requests++
						mu.Unlock()
						if r.Header.Get("Authorization") != "Bearer current" || r.Header.Get("X-Account-Credential") != "current" {
							t.Errorf("unexpected request headers: %v", r.Header)
						}
						if r.Header.Get("Range") == "" {
							w.Write([]byte("full"))
							return
						}
						if r.Header.Get("Range") != "bytes=1-" {
							t.Errorf("unexpected initial range: %v", r.Header)
						}
						creds.mu.Lock()
						creds.revoked = revoked
						creds.mu.Unlock()
						if status == http.StatusPartialContent {
							w.Header().Set("Content-Range", "bytes 2-3/4")
						}
						w.WriteHeader(status)
						if status == http.StatusPartialContent {
							w.Write([]byte("ll"))
						}
					}))
					defer origin.Close()
					src := Source{Scheme: "http", Locator: origin.URL + "/file", Attrs: map[string]string{CredentialAttr: "hf", CredentialScopeAttr: "owner"}}
					if route == CredentialRouteScoped {
						src.Attrs[CredentialRouteAttr] = route
					}
					headers, err := headersFor(src, creds)
					if err != nil {
						t.Fatal(err)
					}
					ctx := withCredentialHops(context.Background(), src, creds, headers)
					var out bytes.Buffer
					_, err = (HTTP{}).Fetch(ctx, Request{Source: src, Headers: headers, From: 1, Out: &out, Restart: func() error { out.Reset(); return nil }})
					var refusal *CredentialError
					if revoked && (!errors.As(err, &refusal) || refusal.Outcome != "revoked") {
						t.Fatalf("restart reused admission: %v", err)
					}
					if !revoked && err != nil {
						t.Fatalf("permitted restart refused: %v", err)
					}
					mu.Lock()
					defer mu.Unlock()
					creds.mu.Lock()
					defer creds.mu.Unlock()
					wantRequests, wantBytes := 2, "full"
					if revoked {
						wantRequests, wantBytes = 1, ""
					}
					if requests != wantRequests || creds.applied != 2 || out.String() != wantBytes {
						t.Fatalf("restart: requests=%d applied=%d bytes=%q", requests, creds.applied, out.String())
					}
				})
			}
		}
	}
}
