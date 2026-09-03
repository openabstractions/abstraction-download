//go:build windows

package main

import "syscall"

// detached starts the child in its own process group with no console attached,
// so closing the terminal that ran `jobd start` does not take the supervisor
// with it. That is the entire point: a supervisor that dies with the shell that
// launched it supervises nothing.
func detached() *syscall.SysProcAttr {
	const (
		detachedProcess     = 0x00000008
		createNewProcessGrp = 0x00000200
	)
	return &syscall.SysProcAttr{CreationFlags: detachedProcess | createNewProcessGrp}
}
