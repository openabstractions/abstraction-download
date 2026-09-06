package download

import (
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPlatformTrustStoreIsNotReportedMissing(t *testing.T) {
	if rootsEmpty(x509.SystemCertPool()) {
		t.Fatal("this machine's trust store was read as absent; on Windows and macOS the pool is empty by design and the OS verifies")
	}
}

func TestNoRootsAtAllIsReportedMissing(t *testing.T) {
	if !rootsEmpty(x509.NewCertPool(), nil) {
		t.Fatal("a pool with no roots and no platform verifier behind it should read as absent")
	}
}

func TestUntrustedCertificateIsNotBlamedOnTheMachine(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	hreq, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = HTTP{}.do(hreq, nil)
	if err == nil {
		t.Fatal("a self-signed test server was accepted")
	}
	if errors.Is(err, ErrNoTrustStore) {
		t.Fatalf("a bad certificate was reported as a missing trust store: %v", err)
	}
}

func TestTheRefusalNamesBothTheCauseAndTheFix(t *testing.T) {
	err := noTrustStore(io.ErrUnexpectedEOF)
	if !errors.Is(err, ErrNoTrustStore) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("lost half the chain: %v", err)
	}
	if !strings.Contains(err.Error(), "SSL_CERT_FILE") {
		t.Fatalf("nothing in this tells the operator what to do: %v", err)
	}
	if Permanent(err) {
		t.Fatal("a machine with no bundle installed yet would end the job for good")
	}
}
