package download

import (
	"context"
	"errors"
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
	// Lookup returns the headers to attach for a named credential about to be
	// sent to host. Returning false is normal and covers two cases a caller does
	// not need to tell apart: no such credential (a public artifact needs none),
	// and a credential that exists but is not bound to this host.
	//
	// The host is not decoration. A record is written by anyone who can write
	// the store and it chooses the source's host, so a credential resolved
	// without regard to where it is going is a credential the record can point
	// at a server the attacker controls. The secret is bound to its hosts here,
	// on the fetching machine, and refused for every other — see EnvCredentials.
	Lookup(name, host string) (map[string]string, bool)
}

// CredentialAttr is the Source attribute naming a credential. Its value is a
// name like "hf", never a secret.
const CredentialAttr = "credential"

// CredentialScopeAttr is the Source attribute naming the authenticated caller
// scope that submitted a service-executed source. It is the job service's
// opaque caller namespace, grants nothing, and is never sent on the wire.
const CredentialScopeAttr = "credential_scope"

// SourceCredentials resolves a source's credential for one request to host,
// with the source's attributes, for a service that applies a credential for the
// caller that submitted the work (abstraction.credentials/applier@1). When a
// Credentials value implements it, the runner calls LookupSource instead of
// Lookup, once for every request. A refusal is a *CredentialError; the headers
// are sent once and kept out of every record.
type SourceCredentials interface {
	LookupSource(src Source, host string) (map[string]string, error)
}

// CredentialError is a named credential the service could not apply to one
// request. Outcome is the applier's word (unknown, expired, revoked, lost,
// not_permitted, target_refused, consumer_refused, unsupported_kind, invalid,
// forbidden or unavailable). Every outcome but unavailable ends the attempt:
// the same request cannot succeed until the credential or its rule changes, and
// the operation stays retryable through a new attempt. The message names the
// credential and never carries a secret.
type CredentialError struct {
	Name, Outcome string
}

func (e *CredentialError) Error() string {
	return "download: credential:" + e.Outcome + ":" + e.Name
}

// Retryable reports whether the applier could not answer, rather than refused.
func (e *CredentialError) Retryable() bool { return e.Outcome == "unavailable" }

// CredentialRefusal builds the error for an applier outcome other than applied:
// permanent for a refusal, retryable for unavailable.
func CredentialRefusal(name, outcome string) error {
	e := &CredentialError{Name: name, Outcome: outcome}
	if e.Retryable() {
		return e
	}
	return permanent{e}
}

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

func (e EnvCredentials) Lookup(name, host string) (map[string]string, bool) {
	if name == "" {
		return nil, false
	}
	v := os.Getenv(e.prefix() + credEnvName(name))
	if v == "" || !e.boundTo(name, host) {
		return nil, false
	}
	return map[string]string{"Authorization": "Bearer " + v}, true
}

// boundTo reports whether credential name may be sent to host, per
// $ABSTRACTION_CRED_<NAME>_HOSTS — a comma-separated list of host patterns, each
// covering its subdomains the way Refusals does, so binding huggingface.co binds
// cdn-lfs.huggingface.co.
//
// The binding lives beside the secret, on the machine, and nowhere else. Not in
// the record, because the record is the thing an attacker writes; not in the
// store's configuration, because a shared store is configured by whoever can
// write it. A machine that holds a token decides who may receive it, and an
// unbound credential is refused for every host — fail closed, because the cost
// of the other default is the owner's token on a server the record chose.
func (e EnvCredentials) boundTo(name, host string) bool {
	host = strings.ToLower(host)
	if host == "" {
		return false
	}
	for _, pat := range strings.Split(os.Getenv(e.prefix()+credEnvName(name)+"_HOSTS"), ",") {
		pat = strings.ToLower(strings.TrimSpace(pat))
		if pat != "" && (host == pat || strings.HasSuffix(host, "."+pat)) {
			return true
		}
	}
	return false
}

func credEnvName(name string) string {
	return strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(name))
}

// hopCredential applies a source's credential again for each host a redirect
// leads to [DL-K2]. It rides the request context, so every fetcher sending the
// request through HTTP.do meets it without learning that a credential exists.
type hopCredential struct {
	// applied names the headers the credential set on the first request.
	applied []string
	// apply returns the credential's headers for host, nil when the
	// credential is not bound to host, or the refusal that ends the request.
	apply func(host string) (map[string]string, error)
}

type hopCredentialKey struct{}

// withCredentialHops returns ctx carrying the per-redirect application of the
// credential src names, given the headers headersFor resolved for its first
// host. A source naming no credential returns ctx unchanged.
func withCredentialHops(ctx context.Context, src Source, creds Credentials, headers map[string]string) context.Context {
	name := src.Attrs[CredentialAttr]
	if name == "" || creds == nil {
		return ctx
	}
	hop := &hopCredential{}
	for k, v := range headers {
		if supplied, ok := src.Headers[k]; !ok || supplied != v {
			hop.applied = append(hop.applied, k)
		}
	}
	hop.apply = func(host string) (map[string]string, error) {
		host = strings.ToLower(host)
		if bound, ok := creds.(SourceCredentials); ok {
			got, err := bound.LookupSource(src, host)
			var refusal *CredentialError
			if errors.As(err, &refusal) && refusal.Outcome == "target_refused" {
				return nil, nil
			}
			return got, err
		}
		got, ok := creds.Lookup(name, host)
		if !ok {
			return nil, nil
		}
		if h := src.Attrs[CredentialHeaderAttr]; h != "" {
			for _, v := range got {
				return map[string]string{h: v}, nil
			}
		}
		return got, nil
	}
	return context.WithValue(ctx, hopCredentialKey{}, hop)
}

func credentialHops(ctx context.Context) *hopCredential {
	hop, _ := ctx.Value(hopCredentialKey{}).(*hopCredential)
	return hop
}

// headersFor is everything that goes on the wire for one source: what the
// record asked for, plus the secret resolved here and now.
//
// Attrs are not consulted. They describe the source to this layer, and the
// exclusion list that used to keep them off the wire had four entries and no
// rule — a fifth attribute would have been sent to the server by default.
// Nothing reads Attrs here, so there is no case left to forget.
func headersFor(src Source, creds Credentials) (map[string]string, error) {
	out := make(map[string]string, len(src.Headers)+1)
	for k, v := range src.Headers {
		out[k] = v
	}

	name := src.Attrs[CredentialAttr]
	if name == "" {
		return out, nil
	}
	if creds == nil {
		return nil, fmt.Errorf("download: source needs credential %q but none are configured", name)
	}
	// The host the secret is about to be sent to, read by the same parser the
	// transport will use, because binding the secret to where it is going is
	// the whole point — see Credentials.Lookup. An authority this machine
	// cannot read the same way the transport does gets no secret at all.
	host, err := hostOf(src.Locator)
	if err != nil {
		return nil, err
	}
	if bound, ok := creds.(SourceCredentials); ok {
		got, err := bound.LookupSource(src, host)
		if err != nil {
			return nil, err
		}
		for k, v := range got {
			out[k] = v
		}
		return out, nil
	}
	got, ok := creds.Lookup(name, host)
	if !ok {
		// Refuse rather than fetch unauthenticated OR send the secret onward. A
		// 401 halfway through a 40 GB transfer is a worse way to learn a
		// credential is missing, and an anonymous request to a gated repo can
		// return a login page that is the wrong bytes rather than an error.
		// When the credential exists but is not bound to this host, the refusal
		// is what stops a record pointing the owner's token at a server it
		// chose — not now, not a failure of the record, so the job stays
		// adoptable by a machine that binds the credential to this host.
		return nil, fmt.Errorf("download: credential %q is not configured for host %q — bind it on the fetching machine with %s%s and %s%s_HOSTS, never in the record",
			name, host, EnvCredentials{}.prefix(), credEnvName(name), EnvCredentials{}.prefix(), credEnvName(name))
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
