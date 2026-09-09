package nas

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

// shares asks Windows what a host serves, through the API rather than through
// `net view`.
//
// There is no SMB client in the standard library and writing one, or taking a
// package that has, is a dependency every adopter would inherit for a question
// the operating system already answers. But the child process is the wrong way
// to ask it: `net view`'s output is localised — the first parser keyed on the
// literal word "Disk" and would have returned nothing on a German Windows —
// and it exits 2 with "System error 1702" on inputs the API simply refuses.
// NetShareEnum returns the type as a number.
type shareInfo1 struct {
	netname *uint16
	kind    uint32
	remark  *uint16
}

const (
	// STYPE_DISKTREE is a folder. STYPE_SPECIAL and STYPE_TEMPORARY are set in
	// the high bits of the same field for C$, IPC$ and their kind, which are
	// not places a person keeps anything.
	diskTree    = 0
	kindMask    = 0x0000_00ff
	specialMask = 0xC000_0000
	moreData    = 234
)

var (
	netapi32       = syscall.NewLazyDLL("netapi32.dll")
	netShareEnum   = netapi32.NewProc("NetShareEnum")
	netBufferFree  = netapi32.NewProc("NetApiBufferFree")
	preferredBytes = uint32(64 << 10)
)

func shares(ctx context.Context, address string) ([]string, error) {
	if net.ParseIP(address) == nil {
		return nil, fmt.Errorf("share enumeration takes an address, and %q is not one", address)
	}
	// In a goroutine because NetShareEnum has no cancellation and a host that is
	// switched off holds it for the SMB stack's own timeout, which is around ten
	// seconds. The caller gets its context honoured; the call finishes on its own
	// and its result is dropped.
	done := make(chan answer, 1)
	go func() { names, err := enumerate(address); done <- answer{names, err} }()
	select {
	case a := <-done:
		return a.names, a.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type answer struct {
	names []string
	err   error
}

func enumerate(address string) ([]string, error) {
	server, err := syscall.UTF16PtrFromString(`\\` + address)
	if err != nil {
		return nil, err
	}
	var buf *shareInfo1
	var read, total uint32
	status, _, _ := netShareEnum.Call(uintptr(unsafe.Pointer(server)), 1,
		uintptr(unsafe.Pointer(&buf)), uintptr(preferredBytes),
		uintptr(unsafe.Pointer(&read)), uintptr(unsafe.Pointer(&total)), 0)
	if status != 0 && status != moreData {
		return nil, fmt.Errorf("NetShareEnum: %w", syscall.Errno(status))
	}
	if buf == nil {
		return nil, fmt.Errorf("NetShareEnum returned no buffer for %s", address)
	}
	defer netBufferFree.Call(uintptr(unsafe.Pointer(buf)))

	var names []string
	for _, s := range unsafe.Slice(buf, read) {
		if s.kind&kindMask != diskTree || s.kind&specialMask != 0 {
			continue
		}
		if name := printable(utf16At(s.netname)); name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}

func utf16At(p *uint16) string {
	if p == nil {
		return ""
	}
	var out []uint16
	for i := 0; i < 256; i++ {
		c := *(*uint16)(unsafe.Add(unsafe.Pointer(p), uintptr(i)*2))
		if c == 0 {
			break
		}
		out = append(out, c)
	}
	return syscall.UTF16ToString(out)
}
