package main

import (
	"fmt"
	"path/filepath"

	winapi "golang.org/x/sys/windows"
)

// windowsPowerShell is Windows PowerShell under the Windows directory the
// system reports (GetSystemWindowsDirectory), never a powershell.exe found
// through PATH or a SystemRoot the caller's environment names.
func windowsPowerShell() (string, error) {
	dir, err := winapi.GetSystemWindowsDirectory()
	if err != nil || !filepath.IsAbs(dir) {
		return "", fmt.Errorf("the Windows directory is unavailable (%v); reachwall does not search PATH", err)
	}
	return firstProgram([]string{filepath.Join(dir, "System32", "WindowsPowerShell", "v1.0", "powershell.exe")})
}
