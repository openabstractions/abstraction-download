package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFirstProgramTakesAnAbsoluteRegularFileOrRefuses: the loader runs
// elevated, so a bare name, a directory or a missing file is never run.
func TestFirstProgramTakesAnAbsoluteRegularFileOrRefuses(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "nft")
	if err := os.WriteFile(file, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := firstProgram([]string{"nft", filepath.Join(dir, "missing"), dir, file})
	if err != nil || got != file {
		t.Fatalf("got %q %v, want %q", got, err, file)
	}
	if got, err := firstProgram([]string{"nft", filepath.Join(dir, "missing"), dir}); err == nil {
		t.Fatalf("got %q, want a refusal", got)
	}
}

// TestNftIsLookedUpOnlyAtFixedAbsolutePaths: a PATH entry holding nft is
// never chosen.
func TestNftIsLookedUpOnlyAtFixedAbsolutePaths(t *testing.T) {
	for _, path := range nftPaths {
		if !strings.HasPrefix(path, "/") || filepath.ToSlash(filepath.Clean(path)) != path {
			t.Fatalf("%q is not an absolute path", path)
		}
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "nft"), nil, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	cmd, err := loader("linux", "")
	if err == nil && filepath.Dir(cmd.Path) == dir {
		t.Fatalf("loader took nft from PATH: %s", cmd.Path)
	}
}
