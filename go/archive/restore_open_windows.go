package archive

import (
	"context"
	"errors"
	"os"
	"strings"
	"unsafe"

	"github.com/openabstractions/abstraction-identity/fileid"
	"golang.org/x/sys/windows"
)

// Native relative opens also support an operator's SMB archive, which
// fileid.Open excludes from its local-file identity contract.
func openRestoreFile(ctx context.Context, folder, relative string, read bool) (selected *restoreFile, failure error) {
	path, err := windows.UTF16PtrFromString(folder)
	if err != nil {
		return nil, err
	}
	const share = windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE
	root, err := windows.CreateFile(path, windows.FILE_TRAVERSE|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		share, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.SECURITY_SQOS_PRESENT|windows.SECURITY_IDENTIFICATION, 0)
	if err != nil {
		return nil, err
	}
	ancestors := []*os.File{os.NewFile(uintptr(root), folder)}
	defer func() {
		if selected == nil {
			failure = errors.Join(failure, closeRestoreAncestors(ancestors))
		}
	}()
	var rootInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(root, &rootInfo); err != nil {
		return nil, err
	}
	if rootInfo.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		return nil, errLink
	}
	parts := strings.Split(relative, "/")
	for i, part := range parts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fileid.SafeName(part); err != nil {
			return nil, err
		}
		name, err := windows.NewNTUnicodeString(part)
		if err != nil {
			return nil, err
		}
		attrs := windows.OBJECT_ATTRIBUTES{
			Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
			RootDirectory: root, ObjectName: name, Attributes: windows.OBJ_CASE_INSENSITIVE,
		}
		var child windows.Handle
		var status windows.IO_STATUS_BLOCK
		access := uint32(windows.FILE_READ_ATTRIBUTES | windows.SYNCHRONIZE)
		options := uint32(windows.FILE_OPEN_REPARSE_POINT | windows.FILE_SYNCHRONOUS_IO_NONALERT)
		final := i == len(parts)-1
		if final {
			options |= windows.FILE_NON_DIRECTORY_FILE
			if read {
				access |= windows.FILE_READ_DATA
			}
		} else {
			options |= windows.FILE_DIRECTORY_FILE
			access |= windows.FILE_TRAVERSE
		}
		err = windows.NtCreateFile(&child, access, &attrs, &status, nil, 0, share, windows.FILE_OPEN, options, 0, 0)
		if err != nil {
			return nil, err
		}
		var info windows.ByHandleFileInformation
		err = windows.GetFileInformationByHandle(child, &info)
		if err != nil || info.FileAttributes&(windows.FILE_ATTRIBUTE_REPARSE_POINT|windows.FILE_ATTRIBUTE_DEVICE) != 0 {
			_ = windows.CloseHandle(child)
			if err != nil {
				return nil, err
			}
			return nil, errLink
		}
		if final {
			return &restoreFile{File: os.NewFile(uintptr(child), part), ancestors: ancestors}, nil
		}
		ancestors = append(ancestors, os.NewFile(uintptr(child), part))
		root = child
	}
	return nil, errLink
}
