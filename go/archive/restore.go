package archive

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/openabstractions/abstraction-identity/fileid"
)

// RestoreSource holds the archive owner's exact matching observation. Its
// path never crosses the runtime's Panel boundary.
type RestoreSource struct {
	folder, relative, digest, proof string
	size                            int64
}

func (s RestoreSource) Proof() string { return s.proof }

type restoreFile struct {
	*os.File
	ancestors []*os.File
}

func closeRestoreAncestors(ancestors []*os.File) error {
	var err error
	for i := len(ancestors) - 1; i >= 0; i-- {
		err = errors.Join(err, ancestors[i].Close())
	}
	return err
}

func (f *restoreFile) Close() error {
	return errors.Join(f.File.Close(), closeRestoreAncestors(f.ancestors))
}

// Keep a native metadata request counted until its handles have closed, even
// when its caller has timed out. Each configured folder has one such probe.
var restoreProbes sync.Map

func probeRestore(ctx context.Context, folder string, query func(context.Context) RestoreSource) RestoreSource {
	if ctx.Err() != nil || quiet(folder) {
		return RestoreSource{}
	}
	if _, busy := restoreProbes.LoadOrStore(folder, struct{}{}); busy {
		return RestoreSource{}
	}
	deadline := time.Now().Add(probeTimeout)
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	done := make(chan RestoreSource, 1)
	go func() {
		selected := query(bounded)
		restoreProbes.Delete(folder)
		done <- selected
	}()
	select {
	case selected := <-done:
		if bounded.Err() == nil {
			return selected
		}
	case <-bounded.Done():
	}
	if !time.Now().Before(deadline) {
		silence(folder)
	}
	return RestoreSource{}
}

// SelectRestore observes managed entries and unchanged, previously hashed
// index entries. Only Fill verifies the bytes, on the Download job.
func (Service) SelectRestore(ctx context.Context, digest string, size int64) (RestoreSource, bool) {
	sha, ok := digestHex(digest)
	folder := Folder()
	if !ok || size <= 0 || folder == "" {
		return RestoreSource{}, false
	}
	selected := probeRestore(ctx, folder, func(ctx context.Context) RestoreSource {
		return selectRestore(ctx, folder, digest, sha, size)
	})
	return selected, selected.proof != ""
}

func selectRestore(ctx context.Context, folder, digest, sha string, size int64) RestoreSource {
	entry := "sha256/" + sha
	paths := []File{{Path: entry}}
	index, _ := readIndex(folder)
	for _, file := range listedFor(folder, index, Want{SHA256: sha, Bytes: size}, true) {
		if file.SHA256 == sha && file.Path != entry {
			paths = append(paths, file)
		}
		if len(paths) == 2 {
			break
		}
	}
	for _, candidate := range paths {
		if ctx.Err() != nil {
			break
		}
		observed, err := inspectRestoreFile(ctx, folder, candidate.Path, size, false)
		if err != nil {
			continue
		}
		if candidate.Path == entry || observed.modified == candidate.Modified {
			return RestoreSource{folder, candidate.Path, digest, observed.proof, size}
		}
	}
	return RestoreSource{}
}

type restoreObservation struct {
	file     *restoreFile
	proof    string
	modified int64
}

func inspectRestoreFile(ctx context.Context, folder, relative string, size int64, read bool) (restoreObservation, error) {
	file, err := openRestoreFile(ctx, folder, relative, read)
	if err != nil {
		return restoreObservation{}, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != size {
		return restoreObservation{}, errors.Join(err, errDiffers, file.Close())
	}
	key, err := fileid.FileKeyOf(file.File)
	if err != nil {
		return restoreObservation{}, errors.Join(err, file.Close())
	}
	modified := info.ModTime().UnixNano()
	proof := sha256.New()
	fmt.Fprintf(proof, "oa-archive-restore/%q/%q/%d/%d", filepath.Join(folder, relative), key, size, modified)
	for _, parent := range file.ancestors {
		key, err := fileid.FileKeyOf(parent)
		if err != nil {
			return restoreObservation{}, errors.Join(err, file.Close())
		}
		fmt.Fprintf(proof, "/%q", key)
	}
	observed := restoreObservation{proof: fmt.Sprintf("%x", proof.Sum(nil)), modified: modified}
	if read {
		observed.file = file
	} else if err := file.Close(); err != nil {
		return restoreObservation{}, err
	}
	return observed, nil
}

// open rechecks the exact selected observation. A metadata-only reopen owns
// and closes its copy of the handles, independent of an ongoing source read.
func (s RestoreSource) open(ctx context.Context, read bool) (entryFile, error) {
	if s.folder != Folder() {
		return nil, errDiffers
	}
	ctx, cancel := context.WithTimeout(ctx, stallLimit)
	defer cancel()
	type result struct {
		file *restoreFile
		err  error
	}
	done := make(chan result)
	go func() {
		observed, err := inspectRestoreFile(ctx, s.folder, s.relative, s.size, read)
		if err == nil && observed.proof != s.proof {
			err = errDiffers
			if observed.file != nil {
				err = errors.Join(err, observed.file.Close())
				observed.file = nil
			}
		}
		select {
		case done <- result{observed.file, err}:
		case <-ctx.Done():
			if observed.file != nil {
				_ = observed.file.Close()
			}
		}
	}()
	select {
	case got := <-done:
		if got.file == nil {
			return nil, got.err
		}
		return got.file, got.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Fill uses only the selected entry and verifies the full digest and size.
func (s RestoreSource) Fill(ctx context.Context, digest string, size int64, partial string, progress func(int64, int64)) (int64, bool) {
	if digest != s.digest || size != s.size || s.proof == "" || s.folder != Folder() {
		return 0, false
	}
	sha, _ := digestHex(digest)
	open := func() (entryFile, error) {
		return s.open(ctx, true)
	}
	check := func(entryFile) bool {
		// This also checks that the configured spelling still reaches the
		// held file after a POSIX directory rename or a changed root alias.
		_, err := s.open(ctx, false)
		return err == nil
	}
	_, err := copyArchiveEntry(ctx, open, partial, sha, size, func(done int64) {
		if progress != nil {
			progress(done, size)
		}
	}, check)
	if err != nil {
		_ = os.Remove(partial)
		return 0, false
	}
	return size, true
}

func (RestoreSource) Keep(string, string, string) {}
