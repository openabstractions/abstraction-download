package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// secureFixture is the credential scenario's real loopback TLS origin.
// httptest supplies its test certificate and a client that trusts that CA;
// credentials still pass through the runner's host and transport checks.
type secureFixture struct {
	*httptest.Server
	requests atomic.Int64
}

func newSecureFixture() *secureFixture {
	f := &secureFixture{}
	f.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.Add(1)
		n, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/gated/"), 10, 64)
		if err != nil || n < 0 || n > 1<<20 {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer hf_thisMustNeverAppearOnDisk_EXAMPLE" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.ServeContent(w, r, "artifact.bin", time.Time{}, bytes.NewReader(artifact(n)))
	}))
	return f
}
