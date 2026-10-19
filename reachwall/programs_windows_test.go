package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	winapi "golang.org/x/sys/windows"
)

// TestWindowsPowerShellComesFromTheWindowsDirectory: neither a powershell.exe
// on PATH nor a SystemRoot in the environment chooses the elevated shell.
func TestWindowsPowerShellComesFromTheWindowsDirectory(t *testing.T) {
	system, err := winapi.GetSystemWindowsDirectory()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(system, "System32", "WindowsPowerShell", "v1.0", "powershell.exe")
	decoy := t.TempDir()
	if err := os.WriteFile(filepath.Join(decoy, "powershell.exe"), nil, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", decoy)
	t.Setenv("SystemRoot", decoy)
	t.Setenv("windir", decoy)
	cmd, err := loader("windows", "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(cmd.Path, want) || cmd.Args[0] != want {
		t.Fatalf("loader starts %q (args[0] %q), want %q", cmd.Path, cmd.Args[0], want)
	}
}

// TestNftIsRefusedWithoutAFixedPathOnWindows: no nft exists at the fixed
// Linux paths here, so the loader refuses instead of searching PATH.
func TestNftIsRefusedWithoutAFixedPathOnWindows(t *testing.T) {
	if _, err := loader("linux", ""); err == nil || !strings.Contains(err.Error(), "does not search PATH") {
		t.Fatalf("want a refusal, got %v", err)
	}
}
