package archive

import (
	"context"
	"errors"
	"strings"

	storage "github.com/openabstractions/abstraction-storage/go"
)

// StoreName is the archive's name as a storage store.
const StoreName = "download-archive"

// Store is the download archive as a storage store (storage.thrift Ref,
// content.thrift Store with local placement): the source a fill reads a
// model from, on a NAS share or a local folder, with no local copy. Only
// the archive knows where its folder is (DL-R44).
//
// Find looks for the digest as Fill does, by the content name, then the
// stored listing's files by recorded SHA-256 and by the hex in their names,
// within the probe bound; it never hashes, and it never lists the folder
// again. The reference's locator is the candidate's path, which only this
// store projects (Local). Whoever reads the bytes proves them against the
// digest before anything uses them, as the copy into a download does, and
// reports a refusal through Refused. Place answers storage.ErrReadOnly: the
// archive is written only by its own write-back (Keep).
type Store struct{}

// Name is StoreName.
func (Store) Name() string { return StoreName }

// Find answers a reference to a candidate file for digest ("sha256:" and
// 64 lower-case hex digits).
func (Store) Find(digest string) (storage.Ref, bool) {
	sha, ok := strings.CutPrefix(digest, "sha256:")
	folder := Folder()
	if !ok || folder == "" || !validSHA256Hex(sha) {
		return storage.Ref{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	found := sources(ctx, folder, Want{SHA256: sha}, 1)
	if len(found) == 0 {
		return storage.Ref{}, false
	}
	return storage.NewRef(StoreName, digest, found[0].path, found[0].info.Size()), true
}

// Place answers storage.ErrReadOnly.
func (Store) Place(string, int64) (storage.Ref, error) { return storage.Ref{}, storage.ErrReadOnly }

// Path is the candidate file a reference this store issued names.
func (Store) Path(r storage.Ref) string {
	if r.Store != StoreName {
		return ""
	}
	return storage.Locator(r)
}

// Refused applies the archive's rule for bytes that differ from their
// digest to a reference a reader refused: OA's content-name entry moves to
// sha256\.bad\<hex>, and a person's file is not offered again in this
// process; each with one log line.
func (Store) Refused(r storage.Ref, cause error) {
	sha, _ := strings.CutPrefix(r.Digest, "sha256:")
	path := storage.Locator(r)
	folder := Folder()
	if r.Store != StoreName || path == "" || folder == "" {
		return
	}
	if cause == nil {
		cause = errors.New("its bytes were refused")
	}
	if content, err := entryPath(folder, sha); err == nil && path == content {
		ctx := context.Background()
		key, err := currentCorruptMember(ctx, folder, sha, -1)
		if err != nil {
			reject(path, errors.Join(cause, err))
			return
		}
		if key != "" {
			quarantine(ctx, folder, sha, key, cause)
		}
		return
	}
	reject(path, cause)
}
