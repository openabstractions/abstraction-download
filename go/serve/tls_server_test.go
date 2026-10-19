package serve

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// newTrustedTLSServer serves h over https and points http.DefaultClient, which
// HTTP{} uses, at a client trusting httptest's certificate until the test
// ends. A named credential travels only over https [DL-K5].
func newTrustedTLSServer(t testing.TB, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	old := http.DefaultClient
	http.DefaultClient = srv.Client()
	t.Cleanup(func() { http.DefaultClient = old })
	return srv
}
