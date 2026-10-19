package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	download "github.com/openabstractions/abstraction-download/go"
	"github.com/openabstractions/abstraction-download/go/powershell"
)

type refusal struct{ host, reason string }

func main() {
	file := flag.String("file", download.DefaultRefusals().Path, "the refused list to compile")
	target := flag.String("os", runtime.GOOS, "windows or linux: whose rules to write")
	program := flag.String("program", "", "windows: refuse only this executable")
	group := flag.String("group", "", "linux: refuse only sockets owned by this gid")
	apply := flag.Bool("apply", false, "load the rules instead of printing them")
	flag.Parse()

	list, err := load(*file)
	if err != nil {
		fail(err)
	}
	script, warnings, err := render(*target, list, *program, *group)
	if err != nil {
		fail(err)
	}
	for _, w := range warnings {
		fmt.Fprintln(os.Stderr, "reachwall:", w)
	}
	if !*apply {
		fmt.Print(script)
		return
	}
	cmd, err := loader(*target, script)
	if err != nil {
		fail(fmt.Errorf("apply: %w", err))
	}
	if err := cmd.Run(); err != nil {
		fail(fmt.Errorf("apply: %w", err))
	}
}

func load(path string) ([]refusal, error) {
	hosts, err := download.Refusals{Path: path}.List()
	if err != nil {
		return nil, err
	}
	list := make([]refusal, 0, len(hosts))
	for host, reason := range hosts {
		list = append(list, refusal{host, oneLine(reason)})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].host < list[j].host })
	return list, nil
}

func render(target string, list []refusal, program, group string) (string, []string, error) {
	switch target {
	case "windows":
		script, warnings := windows(list, program)
		return script, warnings, nil
	case "linux":
		script, warnings := linux(list, group)
		return script, warnings, nil
	}
	return "", nil, fmt.Errorf("%s has no packet filter this tool can write; windows and linux do", target)
}

// loader runs script. On Windows the script goes through standard input as
// UTF-8 and runs as one script block (powershell.UTF8Stdin): "-Command -"
// would decode it with the console code page, where another character's byte
// can become a curly quote that ends a literal, and would keep running after
// a throw. The loader runs elevated, so it starts its program by absolute
// path and never through PATH.
func loader(target, script string) (*exec.Cmd, error) {
	var cmd *exec.Cmd
	if target == "windows" {
		shell, err := windowsPowerShell()
		if err != nil {
			return nil, err
		}
		cmd = exec.Command(shell, "-NoProfile", "-NonInteractive", "-EncodedCommand", powershell.EncodedCommand(powershell.UTF8Stdin))
	} else {
		nft, err := firstProgram(nftPaths)
		if err != nil {
			return nil, err
		}
		cmd = exec.Command(nft, "-f", "-")
	}
	cmd.Stdin = strings.NewReader(script)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd, nil
}

// nftPaths are the places a distribution installs nft, in order; PATH is
// never searched.
var nftPaths = []string{"/usr/sbin/nft", "/sbin/nft", "/usr/bin/nft", "/bin/nft"}

// firstProgram is the first of paths that is an absolute path to a regular
// file. It refuses when none is.
func firstProgram(paths []string) (string, error) {
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			continue
		}
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			return path, nil
		}
	}
	return "", fmt.Errorf("no program at %s; reachwall does not search PATH", strings.Join(paths, ", "))
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "reachwall:", err)
	os.Exit(1)
}
