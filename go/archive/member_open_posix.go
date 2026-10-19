//go:build linux || darwin

package archive

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/openabstractions/abstraction-identity/fileid"
	"golang.org/x/sys/unix"
)

// Downloads can use the operator's mounted archive. The selected root is
// opened in that namespace and descendants are resolved against held dirfds.
func openDownloadMember(ctx context.Context, folder, relative string, _ bool) (opened *restoreFile, failure error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := unix.Open(folder, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	parents := []*os.File{os.NewFile(uintptr(root), folder)}
	defer func() {
		if opened == nil {
			failure = errors.Join(failure, closeRestoreAncestors(parents))
		}
	}()
	parts := strings.Split(relative, "/")
	for i, part := range parts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fileid.SafeName(part); err != nil {
			return nil, err
		}
		var st unix.Stat_t
		if err := unix.Fstatat(root, part, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return nil, err
		}
		if st.Mode&unix.S_IFMT == unix.S_IFLNK {
			return nil, errLink
		}
		final := i == len(parts)-1
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
		if final {
			if st.Mode&unix.S_IFMT != unix.S_IFREG {
				return nil, errDiffers
			}
		} else {
			flags |= unix.O_DIRECTORY
		}
		child, err := unix.Openat(root, part, flags, 0)
		if errors.Is(err, unix.ELOOP) {
			return nil, errLink
		}
		if err != nil {
			return nil, err
		}
		file := os.NewFile(uintptr(child), part)
		if final {
			info, err := file.Stat()
			if err != nil || !info.Mode().IsRegular() {
				return nil, errors.Join(err, errDiffers, file.Close())
			}
			return &restoreFile{File: file, ancestors: parents}, nil
		}
		parents = append(parents, file)
		root = child
	}
	return nil, errLink
}
