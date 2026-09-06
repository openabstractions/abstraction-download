package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"

	download "github.com/openabstractions/abstraction-download/go"
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
	if err := loader(*target, script).Run(); err != nil {
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

func loader(target, script string) *exec.Cmd {
	var cmd *exec.Cmd
	if target == "windows" {
		cmd = exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", "-")
	} else {
		cmd = exec.Command("nft", "-f", "-")
	}
	cmd.Stdin = strings.NewReader(script)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "reachwall:", err)
	os.Exit(1)
}
