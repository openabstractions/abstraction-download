package download

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"strings"
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

// do sends hreq, asking reach about every host including each redirect. A
// redirect to a host that is neither the first request's host nor one of its
// subdomains goes without any header the caller supplied: net/http strips only
// Authorization, Cookie and WWW-Authenticate there, and a credential applied as
// another header must not follow the source to a host it was not applied for.
// A request carrying a credential applies it again for every redirect host,
// same host included, and the credential's own hosts decide [DL-K2].
func (h HTTP) do(hreq *http.Request, reach Reach, supplied map[string]string) (*http.Response, error) {
	if err := reach.checkURL(hreq.URL); err != nil {
		return nil, err
	}
	hop := credentialHops(hreq.Context())
	var credentialNames []string
	if hop != nil {
		credentialNames = append(credentialNames, hop.applied...)
	}
	c := *h.client()
	next := c.CheckRedirect
	c.CheckRedirect = func(to *http.Request, via []*http.Request) error {
		if err := reach.checkURL(to.URL); err != nil {
			return err
		}
		if len(via) > 0 && !sameOrSubdomain(to.URL.Hostname(), via[0].URL.Hostname()) {
			for name := range supplied {
				to.Header.Del(name)
			}
		}
		if hop != nil {
			for _, name := range credentialNames {
				to.Header.Del(name)
			}
			applied, err := hop.apply(to.URL.Hostname())
			if err != nil {
				return err
			}
			for name, value := range applied {
				to.Header.Set(name, value)
				credentialNames = append(credentialNames, name)
			}
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

func sameOrSubdomain(host, origin string) bool {
	host, origin = strings.ToLower(host), strings.ToLower(origin)
	return host == origin || strings.HasSuffix(host, "."+origin)
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
