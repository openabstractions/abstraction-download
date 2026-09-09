package download

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

func TestHostOf(t *testing.T) {
	cases := map[string]string{
		"https://Huggingface.co/x/y.bin": "huggingface.co",
		"http://127.0.0.1:8080/plain/64": "127.0.0.1",
		"http://[::1]:9000/a":            "::1",
		"https://huggingface.co./x":      "huggingface.co",
		"file:///C:/models/x.bin":        "",
		"file://nas/share/x.bin":         "nas",
		`\\nas\share\x.bin`:              "nas",
		"//nas/share/x.bin":              "nas",
		`C:\models\x.bin`:                "",
		"/mnt/models/x.bin":              "",
		"gopher://example.invalid/x":     "example.invalid",
	}
	for locator, want := range cases {
		if got := HostOf(locator); got != want {
			t.Errorf("HostOf(%q) = %q, want %q", locator, got, want)
		}
	}
}

// The authority a transport reads differently from us is refused rather than
// guessed at. Every locator here was measured against net/url, urllib.parse and
// WinHttpCrackUrl, and no two of the three agreed on all of them.
func TestAnAuthorityTheTransportReadsDifferentlyIsRefused(t *testing.T) {
	for _, locator := range []string{
		`https://hf.co\@evil.com/x`,
		`https://evil.com\@hf.co/x`,
		`https://hf.co\.evil.com/x`,
		"https://hf.co\t.evil.com/x",
		"https://hf.co\n.evil.com/x",
		"https://hf.co%40evil.com/x",
		"https://hf.co%2f.evil.com/x",
		"http://user:pw@cdn.example:8443/a",
		"https://user@evil.com@hf.co/x",
		"https://ｈｕｇｇｉｎｇｆａｃｅ.ｃｏ/x",
		"https://huggingface。co/x",
		"https://☃.example/x",
	} {
		host, err := hostOf(locator)
		if err == nil {
			t.Errorf("hostOf(%q) = %q with no refusal", locator, host)
			continue
		}
		if !errors.Is(err, ErrUnreachable) {
			t.Errorf("hostOf(%q): %v, want ErrUnreachable so the job stays adoptable", locator, err)
		}
		if Permanent(err) {
			t.Errorf("hostOf(%q) ended the job; a record naming a plain host runs unchanged", locator)
		}
	}
}

// The rule, not the parser: for every locator this layer accepts, the host it
// authorised is the host net/http opens the socket to. A parser that merely
// agrees with another parser proves nothing — the transport is the authority.
func TestTheHostAuthorisedIsTheHostDialled(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer origin.Close()

	var dialled string
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			dialled = strings.ToLower(host)
			return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(origin.URL, "http://"))
		},
	}}

	for _, locator := range []string{
		"http://Huggingface.CO/x",
		"http://huggingface.co./x",
		"http://hf.co:8443/x",
		"http://127.0.0.1:8080/x",
		"http://[::1]:9000/x",
		"http://xn--n3h.example/x",
		"http://sub.hf.co/x",
	} {
		authorised, err := hostOf(locator)
		if err != nil {
			t.Errorf("hostOf(%q) refused a plain host: %v", locator, err)
			continue
		}
		dialled = ""
		req, err := http.NewRequest(http.MethodGet, locator, nil)
		if err != nil {
			t.Errorf("http.NewRequest(%q): %v", locator, err)
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Errorf("%q: %v", locator, err)
			continue
		}
		resp.Body.Close()
		if want := strings.TrimSuffix(dialled, "."); authorised != want {
			t.Errorf("%q: authorised %q, dialled %q", locator, authorised, dialled)
		}
	}
}

// The confused deputy, in the direction that costs a token rather than a
// download: our parser named the host the credential is bound to and the
// transport opened a socket somewhere else. Measured on WinHttpCrackUrl, which
// answers evil.com here and hands that exact string to WinHttpConnect.
func TestACredentialIsNotAttachedToAnAuthorityWeCannotRead(t *testing.T) {
	t.Setenv("ABSTRACTION_CRED_HF", "Authorization: Bearer secret")
	t.Setenv("ABSTRACTION_CRED_HF_HOSTS", "hf.co")
	src := Source{
		Scheme:  "https",
		Locator: `https://hf.co\@evil.com/model.bin`,
		Attrs:   map[string]string{CredentialAttr: "hf"},
	}
	out, err := headersFor(src, EnvCredentials{})
	if err == nil {
		t.Fatalf("the credential was resolved for a locator that connects elsewhere: %v", out)
	}
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("got %v, want ErrUnreachable", err)
	}
}

func TestARefusedHostLeavesTheJobAdoptableWithTheReason(t *testing.T) {
	body, digest := payload(t, 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
	defer srv.Close()

	r, store, root := newRunner(t)
	refused := Refusals{Path: filepath.Join(t.TempDir(), "refused.json")}
	r.Reach = refused.Check
	if err := refused.Refuse("127.0.0.1", "not on this machine's list"); err != nil {
		t.Fatal(err)
	}
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: srv.URL + "/x.bin"})

	err := r.Run(context.Background(), id)
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("got %v, want ErrUnreachable", err)
	}
	if Permanent(err) {
		t.Fatal("a host refusal ended the job; it is this machine's policy, not the job's fault")
	}
	rec, _ := store.Load(id)
	if rec.State != job.StatePending {
		t.Fatalf("state %s, want pending", rec.State)
	}
	if !strings.Contains(rec.Error, "127.0.0.1") || !strings.Contains(rec.Error, "not on this machine's list") {
		t.Fatalf("the record does not carry the host and the reason: %q", rec.Error)
	}
	if !store.Claimable(rec) {
		t.Fatal("the lease was not released")
	}

	if err := refused.Allow("127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("turning the host back on did not let the same job run: %v", err)
	}
	rec, _ = store.Load(id)
	if rec.State != job.StateTransferred {
		t.Fatalf("state %s, want transferred", rec.State)
	}
}

func TestARedirectToARefusedHostIsRefusedWhereTheSocketOpens(t *testing.T) {
	body, digest := payload(t, 4096)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
	defer origin.Close()
	elsewhere := "http://localhost" + strings.TrimPrefix(origin.URL, "http://127.0.0.1")
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere+"/x.bin", http.StatusFound)
	}))
	defer front.Close()

	r, store, root := newRunner(t)
	r.Reach = func(host string) error {
		if host == "localhost" {
			return errors.New("switched off")
		}
		return nil
	}
	id := submit(t, store, root, digest, int64(len(body)), Source{Scheme: "http", Locator: front.URL + "/x.bin"})
	err := r.Run(context.Background(), id)
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("the record named an allowed host and the redirect reached a refused one: %v", err)
	}
	rec, _ := store.Load(id)
	if !strings.Contains(rec.Error, "localhost: switched off") {
		t.Fatalf("reason not carried: %q", rec.Error)
	}
}

func TestRefusalsCoverSubdomainsAndFailClosedOnAnUnreadableFile(t *testing.T) {
	f := Refusals{Path: filepath.Join(t.TempDir(), "refused.json")}
	if err := f.Check("huggingface.co"); err != nil {
		t.Fatalf("no file means nothing refused: %v", err)
	}
	if err := f.Refuse("huggingface.co", "not this week"); err != nil {
		t.Fatal(err)
	}
	if err := f.Check("cdn-lfs.huggingface.co"); err == nil || err.Error() != "not this week" {
		t.Fatalf("subdomain: %v", err)
	}
	if err := f.Check("huggingface.com"); err != nil {
		t.Fatalf("a different name was refused: %v", err)
	}
	if err := os.WriteFile(f.Path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.Check("anything.example"); err == nil {
		t.Fatal("an unreadable list let a connection through")
	}
}
