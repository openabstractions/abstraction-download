// Package powershell writes values into Windows PowerShell scripts. It is the
// one owner of PowerShell quoting for the download module and its consumers
// (reachwall, serve).
package powershell

import (
	"encoding/base64"
	"strings"
	"unicode/utf16"
)

// Literal quotes value as one PowerShell single-quoted string. PowerShell ends
// a single-quoted string at any of five quote characters: ' and the curly
// quotes U+2018, U+2019, U+201A and U+201B. Each of them inside value is
// doubled, the escape PowerShell reads back as that one character, so a value
// holding any of them stays one literal.
func Literal(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('\'')
	for _, r := range value {
		if SingleQuote(r) {
			b.WriteRune(r)
		}
		b.WriteRune(r)
	}
	b.WriteByte('\'')
	return b.String()
}

// SingleQuote reports whether PowerShell's tokenizer treats r as a single
// quote.
func SingleQuote(r rune) bool {
	return r == '\'' || r >= '‘' && r <= '‛'
}

// EncodedCommand is script as the argument of powershell.exe -EncodedCommand:
// UTF-16LE, base64. The script reaches PowerShell without passing through a
// code page, Go's argument quoting or CommandLineToArgvW. A Windows command
// line holds at most 32767 characters, about 12000 script characters.
func EncodedCommand(script string) string {
	u := utf16.Encode([]rune(script))
	b := make([]byte, 0, 2*len(u))
	for _, c := range u {
		b = append(b, byte(c), byte(c>>8))
	}
	return base64.StdEncoding.EncodeToString(b)
}

// UTF8Stdin is a script for -EncodedCommand that reads the rest of standard
// input as UTF-8 and runs it as one script block. PowerShell's own
// "-Command -" decodes standard input with the console code page, where a
// byte of another character can decode to a curly quote (U+F980's 0xEF is
// U+2019 in code page 775), and runs it line by line, so a terminating error
// does not stop the lines after it. A script of any length goes through
// standard input. The first error stops the script, is written to standard
// error as plain text (an -EncodedCommand host writes CLIXML there otherwise)
// and exits 1.
const UTF8Stdin = `$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
try {
    $reader = New-Object System.IO.StreamReader([Console]::OpenStandardInput(), (New-Object System.Text.UTF8Encoding($false)))
    $script = $reader.ReadToEnd()
    . ([ScriptBlock]::Create($script))
} catch {
    [Console]::Error.WriteLine($_.ToString())
    [Console]::Error.WriteLine($_.InvocationInfo.PositionMessage)
    exit 1
}`
