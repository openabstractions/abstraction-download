package download

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// ProvenFile copies a file the store offered by digest and refuses any other
// file source, whoever wrote it into a record [DL-R28].
func TestProvenFileCopiesOnlyWhatTheStoreProved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proved.bin")
	if err := os.WriteFile(path, []byte("proved bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	named := Source{Scheme: "file", Locator: path, Attrs: map[string]string{"store": "job", "job": "x"}}
	if _, err := (ProvenFile{}).Fetch(context.Background(), Request{Source: named, Out: &out}); !errors.Is(err, errNotProven) || out.Len() != 0 || Permanent(err) {
		t.Fatalf("a file source the store did not offer: %v, %d bytes", err, out.Len())
	}
	if _, _, err := (ProvenFile{}).Ranged(context.Background(), named, nil); !errors.Is(err, errNotProven) {
		t.Fatalf("ranged: %v", err)
	}
	if err := (ProvenFile{}).FetchRange(context.Background(), RangeRequest{Source: named}); !errors.Is(err, errNotProven) {
		t.Fatalf("range: %v", err)
	}
	offered := named
	offered.proven = true
	if _, err := (ProvenFile{}).Fetch(context.Background(), Request{Source: offered, Out: &out}); err != nil || out.String() != "proved bytes" {
		t.Fatalf("an offered file: %v %q", err, out.String())
	}
}
