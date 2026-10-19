package main

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/openabstractions/abstraction-download/go/powershell"
)

// TestLoaderReadsEveryCharacterBack runs scripts through loader itself, the
// route --apply takes, and compares the UTF-16 code units PowerShell reads
// back. U+F980 is the reviewer's case: its UTF-8 lead byte 0xEF decodes to
// U+2019 in code page 775, so a code-page transport ends the literal and runs
// what follows.
func TestLoaderReadsEveryCharacterBack(t *testing.T) {
	if _, err := windowsPowerShell(); err != nil {
		t.Skip("Windows PowerShell is not installed")
	}
	values := []string{
		`C:\it's\x.exe`,
		"C:\\a\u2018b\\x.exe", "C:\\a\u2019b\\x.exe", "C:\\a\u201Ab\\x.exe", "C:\\a\u201Bb\\x.exe",
		`C:\Users\Zoë Ōsaka\呂\x.exe`,
		"\uF980$(Write-Output INJECTED-BY-REASON)'\uF980",
		"\u2019; Write-Output INJECTED-BY-QUOTE; \u2018",
	}
	var script strings.Builder
	script.WriteString("$ErrorActionPreference = 'Stop'\n")
	for _, value := range values {
		script.WriteString("[Console]::Out.WriteLine((" + powershell.Literal(value) + ").ToCharArray().ForEach({[int]$_}) -join ',')\n")
	}
	var out, errOut bytes.Buffer
	cmd, err := loader("windows", script.String())
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		t.Fatalf("loader: %v\n%s\n%s", err, out.String(), errOut.String())
	}
	if strings.Contains(out.String()+errOut.String(), "INJECTED") {
		t.Fatalf("a value ran as script:\n%s\n%s", out.String(), errOut.String())
	}
	lines := strings.Split(strings.TrimSpace(strings.ReplaceAll(out.String(), "\r\n", "\n")), "\n")
	if len(lines) != len(values) {
		t.Fatalf("PowerShell read %d values, want %d:\n%s", len(lines), len(values), out.String())
	}
	for i, value := range values {
		var want []string
		for _, u := range utf16.Encode([]rune(value)) {
			want = append(want, strconv.Itoa(int(u)))
		}
		if lines[i] != strings.Join(want, ",") {
			t.Errorf("PowerShell read %q back as %s, want %s", value, lines[i], strings.Join(want, ","))
		}
	}
}

// TestLoaderStopsAtTheFirstThrow: the Network Protection guard throws, and
// nothing after it may run. "-Command -" ran the next line and exited 0.
func TestLoaderStopsAtTheFirstThrow(t *testing.T) {
	if _, err := windowsPowerShell(); err != nil {
		t.Skip("Windows PowerShell is not installed")
	}
	var out, errOut bytes.Buffer
	cmd, err := loader("windows", "$ErrorActionPreference = 'Stop'\nthrow 'reachwall: stop'\nWrite-Output AFTER-THROW\n")
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err = cmd.Run()
	if err == nil || strings.Contains(out.String()+errOut.String(), "AFTER-THROW") || !strings.Contains(errOut.String(), "reachwall: stop") {
		t.Fatalf("err %v, output:\n%s\n%s", err, out.String(), errOut.String())
	}
	t.Logf("stderr:\n%s", errOut.String())
}
