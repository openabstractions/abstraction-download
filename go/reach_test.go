package download

import (
	"context"
	"errors"
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
		"https://Huggingface.co/x/y.bin":    "huggingface.co",
		"http://127.0.0.1:8080/plain/64":    "127.0.0.1",
		"http://user:pw@cdn.example:8443/a": "cdn.example",
		"http://[::1]:9000/a":               "::1",
		"file:///C:/models/x.bin":           "",
		"file://nas/share/x.bin":            "nas",
		`\\nas\share\x.bin`:                 "nas",
		"//nas/share/x.bin":                 "nas",
		`C:\models\x.bin`:                   "",
		"/mnt/models/x.bin":                 "",
		"gopher://example.invalid/x":        "example.invalid",
	}
	for locator, want := range cases {
		if got := HostOf(locator); got != want {
			t.Errorf("HostOf(%q) = %q, want %q", locator, got, want)
		}
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
