package download

import (
	"errors"
	"strings"
	"testing"
)

// The confused deputy, at the credential field.
//
// A record on a shared store carries attrs.credential="hf". The fetching
// machine holds the owner's Hugging Face token in $ABSTRACTION_CRED_HF. Nothing
// bound that token to a host, so a record naming any locator it liked had the
// token attached and sent there — the owner's secret to a server the attacker
// chose. This pins that the token goes to the bound host and to no other.
func TestCredentialIsBoundToItsHosts(t *testing.T) {
	t.Setenv("ABSTRACTION_CRED_HF", "hf_thisMustNeverAppearOnDisk_EXAMPLE")
	t.Setenv("ABSTRACTION_CRED_HF_HOSTS", "huggingface.co")

	creds := EnvCredentials{}

	attacker := Source{
		Scheme:  "https",
		Locator: "https://attacker.example/models/x.gguf",
		Attrs:   map[string]string{CredentialAttr: "hf"},
	}
	if _, err := headersFor(attacker, creds); err == nil {
		t.Fatal("the token was attached to a host the owner never chose")
	} else if errors.Is(err, ErrEscapesRoot) || Permanent(err) {
		t.Fatalf("a credential refused for a host is this machine's policy, so it must be adoptable (not permanent): %v", err)
	}

	// The bound host, and a subdomain of it, still get the token — binding must
	// not break the download it exists to protect.
	for _, loc := range []string{
		"https://huggingface.co/org/repo/resolve/main/x.gguf",
		"https://cdn-lfs.huggingface.co/org/repo/x.gguf",
	} {
		got, err := headersFor(Source{Scheme: "https", Locator: loc, Attrs: map[string]string{CredentialAttr: "hf"}}, creds)
		if err != nil {
			t.Fatalf("the bound host was refused its credential: %v", err)
		}
		if !strings.HasPrefix(got["Authorization"], "Bearer hf_") {
			t.Fatalf("the bound host did not receive the credential: %v", got)
		}
	}
}

// A credential the machine does not hold at all is refused for every host, and
// the refusal names no secret and is adoptable rather than fatal.
func TestUnconfiguredCredentialIsRefusedNotSent(t *testing.T) {
	got, err := headersFor(Source{
		Scheme:  "https",
		Locator: "https://huggingface.co/x",
		Attrs:   map[string]string{CredentialAttr: "hf"},
	}, EnvCredentials{})
	if err == nil {
		t.Fatalf("an unconfigured credential fetched anyway with headers %v", got)
	}
	if Permanent(err) {
		t.Fatalf("a missing credential is fixed by configuring it, so the job stays adoptable: %v", err)
	}
}

// An unbound credential — held by the machine but with no host list — reaches
// nobody. Fail closed: the alternative default is the token on whatever host a
// record names.
func TestCredentialWithNoHostBindingReachesNobody(t *testing.T) {
	t.Setenv("ABSTRACTION_CRED_HF", "hf_thisMustNeverAppearOnDisk_EXAMPLE")

	if _, ok := (EnvCredentials{}).Lookup("hf", "huggingface.co"); ok {
		t.Fatal("a credential with no ABSTRACTION_CRED_HF_HOSTS was sent anyway")
	}
}
