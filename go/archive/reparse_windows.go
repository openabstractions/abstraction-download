//go:build windows

package archive

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// reparse reports a reparse point: a symlink, junction, mount point
// or cloud placeholder the walk never follows or lists.
func reparse(info os.FileInfo) bool {
	if info.Mode()&(os.ModeSymlink|os.ModeIrregular) != 0 {
		return true
	}
	data, ok := info.Sys().(*syscall.Win32FileAttributeData)
	return ok && data.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0
}

var procWNetGetUniversalNameW = windows.NewLazySystemDLL("mpr.dll").NewProc("WNetGetUniversalNameW")

// universalName is folder with a mapped network drive replaced by its
// UNC path (WNetGetUniversalNameW), so the setting outlives the drive
// letter. Any other folder is returned unchanged.
func universalName(folder string) string {
	volume := filepath.VolumeName(folder)
	if len(volume) != 2 || volume[1] != ':' {
		return folder
	}
	root, err := windows.UTF16PtrFromString(volume + `\`)
	if err != nil || windows.GetDriveType(root) != windows.DRIVE_REMOTE {
		return folder
	}
	local, err := windows.UTF16PtrFromString(folder)
	if err != nil {
		return folder
	}
	const universalNameInfoLevel = 1
	size := uint32(1024)
	for range 2 {
		buffer := make([]byte, size)
		r, _, _ := procWNetGetUniversalNameW.Call(uintptr(unsafe.Pointer(local)), universalNameInfoLevel,
			uintptr(unsafe.Pointer(&buffer[0])), uintptr(unsafe.Pointer(&size)))
		switch syscall.Errno(r) {
		case 0:
			// UNIVERSAL_NAME_INFO's one field points into buffer.
			name := windows.UTF16PtrToString(*(**uint16)(unsafe.Pointer(&buffer[0])))
			if name != "" && validFolder(name) == nil {
				return filepath.Clean(name)
			}
			return folder
		case windows.ERROR_MORE_DATA:
			continue
		default:
			return folder
		}
	}
	return folder
}

// diskSpace reports the bytes the caller may use and the total bytes of the
// volume holding folder, a local path or a share.
func diskSpace(folder string) (free, total int64, err error) {
	path, err := windows.UTF16PtrFromString(folder)
	if err != nil {
		return 0, 0, err
	}
	var available, size, all uint64
	if err := windows.GetDiskFreeSpaceEx(path, &available, &size, &all); err != nil {
		return 0, 0, err
	}
	return int64(available), int64(size), nil
}
