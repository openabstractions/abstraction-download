package powershell

import (
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"unicode/utf16"
)

// TestLiteralReadsBackInWindowsPowerShell parses each literal with the
// installed Windows PowerShell and compares the UTF-16 code units it reads
// back, so the console encoding cannot hide a curly quote.
func TestLiteralReadsBackInWindowsPowerShell(t *testing.T) {
	shell, err := exec.LookPath("powershell.exe")
	if err != nil {
		t.Skip("Windows PowerShell is not installed")
	}
	values := []string{`C:\it's`, "C:\\a\u2018b", "C:\\a\u2019b", "C:\\a\u201Ab", "C:\\a\u201Bb", "\u2019; exit 7; \u2018"}
	var script strings.Builder
	for _, value := range values {
		script.WriteString("[Console]::Out.WriteLine((" + Literal(value) + ").ToCharArray().ForEach({[int]$_}) -join ',')\n")
	}
	out, err := exec.Command(shell, "-NoProfile", "-NonInteractive", "-EncodedCommand", EncodedCommand(script.String())).Output()
	if err != nil {
		t.Fatalf("PowerShell refused the literals: %v %s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(strings.ReplaceAll(string(out), "\r\n", "\n")), "\n")
	if len(lines) != len(values) {
		t.Fatalf("PowerShell read %d values, want %d: %q", len(lines), len(values), out)
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
