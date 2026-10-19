package archive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func retireHeldArchiveMember(ctx context.Context, folder, sha, occurrence string, authorize func() error) error {
	member, err := openDownloadMember(ctx, folder, "sha256/"+sha, false)
	if err != nil {
		return fmt.Errorf("open archive member: %w", err)
	}
	defer closeRestoreAncestors(member.ancestors)
	if err := member.File.Close(); err != nil {
		return err
	}
	parent := windows.Handle(member.ancestors[len(member.ancestors)-1].Fd())
	source, err := openRetirementChild(parent, sha, windows.DELETE|windows.FILE_READ_ATTRIBUTES,
		windows.FILE_NON_DIRECTORY_FILE, windows.FILE_OPEN, windows.FILE_SHARE_READ)
	if err != nil {
		return fmt.Errorf("hold archive member: %w", err)
	}
	defer source.Close()
	member.File = source
	key, err := archiveOccurrence(member)
	if err != nil || occurrence == "" || key != occurrence {
		return errors.Join(err, errArchiveOccurrence)
	}
	to, err := openRetirementChild(parent, ".bad", windows.FILE_TRAVERSE|windows.FILE_READ_ATTRIBUTES,
		windows.FILE_DIRECTORY_FILE, windows.FILE_OPEN_IF, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE)
	if err != nil {
		return fmt.Errorf("hold quarantine directory: %w", err)
	}
	defer to.Close()
	return renameRetiredArchive(source, to, sha, authorize)
}

func openRetirementChild(parent windows.Handle, leaf string, access, options, disposition, share uint32) (*os.File, error) {
	name, err := windows.NewNTUnicodeString(leaf)
	if err != nil {
		return nil, err
	}
	attrs := windows.OBJECT_ATTRIBUTES{Length: uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: parent, ObjectName: name, Attributes: windows.OBJ_CASE_INSENSITIVE}
	var held windows.Handle
	var status windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(&held, access|windows.SYNCHRONIZE, &attrs, &status, nil, 0,
		share, disposition,
		options|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT, 0, 0)
	if err != nil {
		return nil, err
	}
	var info windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(held, &info)
	if err != nil || info.FileAttributes&(windows.FILE_ATTRIBUTE_REPARSE_POINT|windows.FILE_ATTRIBUTE_DEVICE) != 0 {
		return nil, errors.Join(err, errArchiveOccurrence, windows.CloseHandle(held))
	}
	return os.NewFile(uintptr(held), leaf), nil
}

func renameRetiredArchive(source, to *os.File, leaf string, authorize func() error) error {
	wide, err := windows.UTF16FromString(leaf)
	if err != nil {
		return err
	}
	info := struct {
		Flags  uint32
		Root   windows.Handle
		Length uint32
		Name   [65]uint16
	}{Root: windows.Handle(to.Fd()), Length: uint32((len(wide) - 1) * 2)}
	if len(wide) > len(info.Name) {
		return errBadDigest
	}
	copy(info.Name[:], wide)
	if err := authorize(); err != nil {
		return err
	}
	var status windows.IO_STATUS_BLOCK
	return windows.NtSetInformationFile(windows.Handle(source.Fd()), &status, (*byte)(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)), windows.FileRenameInformation)
}
