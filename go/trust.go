package download

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
)

// ErrNoTrustStore means TLS failed because this machine holds no root
// certificates at all, rather than because the source presented a bad one.
//
// Nothing here compiles a CA bundle in. A root list baked into a binary is a
// trust decision taken on the adopter's behalf, it is frozen at the version we
// shipped, and no revocation ever reaches it. So the platform's store is the
// only store, and a machine without one is told so instead of being connected
// anyway.
//
// Deliberately not Permanent: an empty trust store is a property of the machine,
// not of the job. The operator installs a bundle and the next sweep resumes, and
// on a shared store a machine that has one can adopt the job untouched.
var ErrNoTrustStore = errors.New("download: no TLS trust store on this machine")

func (h HTTP) do(hreq *http.Request, reach Reach) (*http.Response, error) {
	if err := reach.check(hreq.URL.Hostname()); err != nil {
		return nil, err
	}
	c := *h.client()
	next := c.CheckRedirect
	c.CheckRedirect = func(to *http.Request, via []*http.Request) error {
		if err := reach.check(to.URL.Hostname()); err != nil {
			return err
		}
		if next != nil {
			return next(to, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	resp, err := c.Do(hreq)
	if err == nil {
		return resp, nil
	}
	var unknown x509.UnknownAuthorityError
	if errors.As(err, &unknown) && rootsEmpty(x509.SystemCertPool()) {
		return nil, noTrustStore(err)
	}
	return nil, err
}

func noTrustStore(cause error) error {
	return fmt.Errorf("%w: install the platform's CA bundle, or point SSL_CERT_FILE at one: %w", ErrNoTrustStore, cause)
}

// rootsEmpty separates a machine with no roots from one whose roots live inside
// a platform verifier. Windows and macOS answer with a pool that holds no
// certificates and defers to the OS; only a pool that holds none AND defers to
// nothing compares equal to a fresh one, and that is the scratch container.
func rootsEmpty(roots *x509.CertPool, err error) bool {
	return err != nil || roots.Equal(x509.NewCertPool())
}
