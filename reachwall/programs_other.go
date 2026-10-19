//go:build !windows

package main

import "errors"

// windowsPowerShell refuses: Windows Firewall rules load only on Windows.
func windowsPowerShell() (string, error) {
	return "", errors.New("Windows Firewall rules load only on Windows; print them with --os windows instead")
}
