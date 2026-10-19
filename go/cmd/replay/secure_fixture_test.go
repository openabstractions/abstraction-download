package main

import (
	"bytes"
	"context"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	job "github.com/openabstractions/abstraction-job/go"
)

func TestCredentialScenariosUseVerifiedTLSAndRealRunner(t *testing.T) {
	for _, tc := range []struct {
		name, hosts, want string
		requests          int64
	}{
		{"bound", "127.0.0.1", "ok state=transferred", 1},
		{"wrong host", "huggingface.co", "transfer-failed state=pending", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ABSTRACTION_CRED_HF", "hf_thisMustNeverAppearOnDisk_EXAMPLE")
			t.Setenv("ABSTRACTION_CRED_HF_HOSTS", tc.hosts)
			work := t.TempDir()
			store, err := job.NewFileStore(filepath.Join(work, "store"))
			if err != nil {
				t.Fatal(err)
			}
			p := &replay{store: store, work: work, ids: map[string]string{},
				epochs: map[string]int64{}, holds: map[string]*job.Hold{}, refused: map[string]string{}}
			if got := p.submit("A", map[string]string{"size": "64", "digest": "good", "src": "https:gated", "cred": "hf"}); !strings.HasPrefix(got, "ok ") {
				t.Fatal(got)
			}
			defer p.secure.Close()
			if got := p.run("A", "alpha"); !strings.HasPrefix(got, tc.want+" ") {
				t.Fatalf("run: %s; want %s", got, tc.want)
			}
			if got := p.secure.requests.Load(); got != tc.requests {
				t.Fatalf("TLS origin received %d requests, want %d", got, tc.requests)
			}
			if tc.requests != 0 {
				got, err := os.ReadFile(filepath.Join(work, "store", "models", "A.bin"))
				if err != nil || !bytes.Equal(got, artifact(64)) {
					t.Fatalf("transferred artifact: %x, %v", got, err)
				}
			}
			if err := filepath.WalkDir(work, func(path string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				b, err := os.ReadFile(path)
				if bytes.Contains(b, []byte("hf_thisMustNeverAppearOnDisk_EXAMPLE")) {
					t.Errorf("credential persisted in %s", path)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSecureFixtureRequiresItsCA(t *testing.T) {
	f := newSecureFixture()
	defer f.Close()
	transport := f.Client().Transport.(*http.Transport)
	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("fixture client disabled certificate verification")
	}
	// A fresh transport has no fixture CA and must fail before the handler.
	client := &http.Client{Transport: &http.Transport{}}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, f.URL+"/gated/64", nil)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.Do(req); err == nil {
		if err := response.Body.Close(); err != nil {
			t.Error(err)
		}
		t.Fatal("an untrusted client accepted the fixture certificate")
	}
	client.CloseIdleConnections()
	if f.requests.Load() != 0 {
		t.Fatal("untrusted request reached the TLS origin handler")
	}
}
