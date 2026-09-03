//go:build !windows

package main

import "syscall"

// detached puts the child in a new session, so it is not in the terminal's
// process group and does not receive its SIGHUP. Same intent as the Windows
// version; the NAS end of the chain runs this one.
func detached() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
