package download

import (
	"fmt"
	"os"
	"strings"
)

// Credentials turns a NAME into a secret, at the moment the secret is needed.
//
// The rule this exists to enforce: **a job record stores a reference, never a
// value.** A record is a file on disk that other processes read — a supervisor,
// a service running as another account, whatever holds the lease next — and it
// is deliberately readable, because that is what makes progress observable from
// outside. Putting a bearer token in it would hand that token to everything
// that can list a directory.
//
// This was a real leak, not a hypothetical one: the HuggingFace resolver used to
// put `Authorization: Bearer <token>` straight into a Source's Attrs, and Attrs
// are written to the record verbatim. The token would have sat in
// ~/.modelget/jobs/*.json in plain text, and been copied into every transcript
// anyone pasted.
//
// The same rule covers the NAS. Reaching it over SSH needs a key, not a password
// in a config file; reaching Download Station's API would need a DSM account
// password held somewhere a background service could read, which is exactly what
// this refuses to do and one of the reasons that route was rejected.
type Credentials interface {
	// Lookup returns the headers to attach for a named credential. Returning
	// false for an unknown name is normal — a public artifact needs none.
	Lookup(name string) (map[string]string, bool)
}

// CredentialAttr is the Source attribute naming a credential. Its value is a
// name like "hf", never a secret.
const CredentialAttr = "credential"

// CredentialHeaderAttr optionally names the header the secret goes into.
// Defaults to Authorization with a Bearer prefix, which is what every registry
// this project has met actually wants.
const CredentialHeaderAttr = "credential_header"

// EnvCredentials reads secrets from the environment: the credential named "hf"
// comes from $ABSTRACTION_CRED_HF.
//
// The environment is not a strong secret store — it is inherited by children and
// visible to anything that can read the process. It is used here because it is
// the one mechanism present everywhere, and because it is still enormously
// better than the file on disk it replaces. A Windows build should prefer
// Credential Manager (DPAPI), which is the same "use the operating system's own
// mechanism" call that made us wrap BITS and use a scheduled task; that is worth
// doing and is not done.
type EnvCredentials struct {
	// Prefix is the environment variable prefix. Empty means the default.
	Prefix string
}

func (e EnvCredentials) prefix() string {
	if e.Prefix != "" {
		return e.Prefix
	}
	return "ABSTRACTION_CRED_"
}

func (e EnvCredentials) Lookup(name string) (map[string]string, bool) {
	if name == "" {
		return nil, false
	}
	key := e.prefix() + strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(name))
	v := os.Getenv(key)
	if v == "" {
		return nil, false
	}
	return map[string]string{"Authorization": "Bearer " + v}, true
}

// headersFor resolves whatever a source needs, keeping the secret out of the
// record and out of the Fetcher, which never learns that a credential exists.
func headersFor(src Source, creds Credentials) (map[string]string, error) {
	out := map[string]string{}
	for k, v := range src.Attrs {
		switch k {
		case CredentialAttr, CredentialHeaderAttr, "store":
			// Not headers. "store" is a note about where a local copy came
			// from, for humans reading the record.
			continue
		default:
			out[k] = v
		}
	}

	name := src.Attrs[CredentialAttr]
	if name == "" {
		return out, nil
	}
	if creds == nil {
		return nil, fmt.Errorf("download: source needs credential %q but none are configured", name)
	}
	got, ok := creds.Lookup(name)
	if !ok {
		// Refuse rather than fetch unauthenticated. A 401 halfway through a
		// 40 GB transfer is a worse way to find this out, and an anonymous
		// request to a gated repo can silently return a login page that is the
		// wrong bytes rather than an error.
		return nil, fmt.Errorf("download: credential %q not found — set %s%s",
			name, EnvCredentials{}.prefix(), strings.ToUpper(name))
	}
	if h := src.Attrs[CredentialHeaderAttr]; h != "" {
		// The credential names a different header; take the single value.
		for _, v := range got {
			out[h] = v
			break
		}
		return out, nil
	}
	for k, v := range got {
		out[k] = v
	}
	return out, nil
}
