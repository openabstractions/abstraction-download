//go:build cgo

package archive

/*
#include <fcntl.h>
#include <sys/stat.h>

static int archive_directory_has_no_extended_acl(int fd) {
    struct stat st;
    filesec_t security = filesec_init();
    if (security == NULL) {
        return 0;
    }
    int present;
    int no_acl = fstatx_np(fd, &st, security) == 0 &&
        filesec_query_property(security, FILESEC_ACL, &present) == 0 && !present;
    filesec_free(security);
    return no_acl;
}
*/
import "C"

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
	return unix.RenameatxNp(parent, sha, to, sha, unix.RENAME_EXCL)
}

// Relative rename requires an owner-controlled namespace. Darwin's extended
// ACL can grant mutation independently of BSD mode bits, so uncertain or
// present ACLs preserve the archive entry.
func ownedArchiveDirectory(ctx context.Context, path string, fd int) bool {
	var mount unix.Statfs_t
	if unix.Fstatfs(fd, &mount) != nil || mount.Flags&unix.MNT_LOCAL == 0 || mount.Flags&unix.MNT_IGNORE_OWNERSHIP != 0 {
		return false
	}
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
	return unix.Fstat(int(proof.File().Fd()), &local) == nil && st.Dev == local.Dev && st.Ino == local.Ino &&
		C.archive_directory_has_no_extended_acl(C.int(fd)) != 0
}
