package archive

import (
	"bytes"
	"errors"
	"os"
	"testing"

	storage "github.com/openabstractions/abstraction-storage/go"
)

var (
	_ storage.Store = Store{}
	_ storage.Local = Store{}
)

// The archive as a storage store: Find answers a located reference to the
// content-name entry, and to a person's file named by its hex in the
// stored listing; Path projects it; Place is refused; an absent digest is not found.
func TestStoreFindsTheArchivesCopies(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("located model bytes "), 2048)
	w := wantOf(body)
	digest := "sha256:" + w.SHA256
	var s Store
	if _, ok := s.Find(digest); ok {
		t.Fatal("an empty archive found the digest")
	}
	named := putNamed(t, folder, "ollama/blobs/sha256-"+w.SHA256, body)
	look(t)
	ref, ok := s.Find(digest)
	if !ok || s.Path(ref) != named || ref.Size != int64(len(body)) || ref.Store != StoreName || ref.Digest != digest {
		t.Fatalf("the listed file was not found by the hex in its name: %+v %q", ref, s.Path(ref))
	}
	entry := putEntry(t, folder, w.SHA256, body)
	ref, ok = s.Find(digest)
	if !ok || s.Path(ref) != entry {
		t.Fatalf("the content-name entry was not found first: %q", s.Path(ref))
	}
	if got, err := os.ReadFile(s.Path(ref)); err != nil || !bytes.Equal(got, body) {
		t.Fatal("the projected path does not hold the bytes")
	}
	if _, err := s.Place(digest, int64(len(body))); !errors.Is(err, storage.ErrReadOnly) {
		t.Fatalf("Place: %v", err)
	}
	if s.Path(storage.NewRef("other", digest, entry, 0)) != "" {
		t.Fatal("the archive projected another store's reference")
	}
	if _, ok := s.Find("sha256:" + string(bytes.Repeat([]byte("0"), 64))); ok {
		t.Fatal("an absent digest was found")
	}
}

// A reader that refuses a content-name entry's bytes moves it aside, as a
// download's copy does; a refused person's file is not offered again.
func TestStoreRefusedEntryMovesAside(t *testing.T) {
	folder := fixture(t)
	body := []byte("the bytes a fill proved wrong")
	w := wantOf(body)
	entry := putEntry(t, folder, w.SHA256, sameSizeWrong(body, 2))
	var s Store
	ref, ok := s.Find("sha256:" + w.SHA256)
	if !ok {
		t.Fatal("the entry was not found")
	}
	s.Refused(ref, errDiffers)
	if _, err := os.Stat(entry); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the refused entry stayed in place: %v", err)
	}
	bad, _ := badPath(folder, w.SHA256)
	if _, err := os.Stat(bad); err != nil {
		t.Fatalf("the refused entry was not moved aside: %v", err)
	}
	if _, ok := s.Find("sha256:" + w.SHA256); ok {
		t.Fatal("a refused entry is found again")
	}
}
