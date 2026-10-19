//go:build linux

package archive

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/openabstractions/abstraction-identity/fileid"
	"golang.org/x/sys/unix"
)

func retireHeldArchiveMember(ctx context.Context, folder, sha, occurrence string, authorize func() error) error {
	member, err := openDownloadMember(ctx, folder, "sha256/"+sha, false)
	if err != nil {
		return err
	}
	defer member.Close()
	key, err := archiveOccurrence(member)
	if err != nil || occurrence == "" || key != occurrence {
		return errors.Join(err, errArchiveOccurrence)
	}
	parent := int(member.ancestors[len(member.ancestors)-1].Fd())
	if !ownedArchiveDirectory(ctx, folder, int(member.ancestors[0].Fd())) ||
		!ownedArchiveDirectory(ctx, filepath.Join(folder, "sha256"), parent) {
		return errArchiveOccurrence
	}
	if err := unix.Mkdirat(parent, ".bad", 0700); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	to, err := unix.Openat(parent, ".bad", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(to)
	if !ownedArchiveDirectory(ctx, filepath.Join(folder, "sha256", ".bad"), to) {
		return errArchiveOccurrence
	}
	var held, named unix.Stat_t
	if err := unix.Fstat(int(member.Fd()), &held); err != nil {
		return err
	}
	if err := unix.Fstatat(parent, sha, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if named.Dev != held.Dev || named.Ino != held.Ino || named.Mode&unix.S_IFMT != unix.S_IFREG {
		return errArchiveOccurrence
	}
	if err := authorize(); err != nil {
		return err
	}
	return unix.Renameat2(parent, sha, to, sha, unix.RENAME_NOREPLACE)
}

// Relative rename is safe only in a namespace other accounts cannot change.
func ownedArchiveDirectory(ctx context.Context, path string, fd int) bool {
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || int(st.Uid) != os.Geteuid() || st.Mode&0022 != 0 {
		return false
	}
	proof, err := fileid.Open(ctx, fileid.Self, path, fileid.Options{})
	if err != nil {
		return false
	}
	defer proof.Close()
	var local unix.Stat_t
	return unix.Fstat(int(proof.File().Fd()), &local) == nil && st.Dev == local.Dev && st.Ino == local.Ino
}
