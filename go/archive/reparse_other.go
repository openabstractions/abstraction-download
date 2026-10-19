//go:build !windows

package archive

import (
	"os"

	"golang.org/x/sys/unix"
)

// reparse reports a symlink or other irregular entry the walk never
// follows or lists.
func reparse(info os.FileInfo) bool {
	return info.Mode()&(os.ModeSymlink|os.ModeIrregular) != 0
}

// universalName is folder unchanged: only Windows maps drive letters.
func universalName(folder string) string { return folder }

// diskSpace reports the bytes the caller may use and the total bytes of the
// volume holding folder.
func diskSpace(folder string) (free, total int64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(folder, &st); err != nil {
		return 0, 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize), nil
}
