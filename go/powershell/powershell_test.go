package powershell

import (
	"encoding/base64"
	"testing"
	"unicode/utf16"
)

// TestLiteralDoublesEveryQuoteCharacter: PowerShell treats U+2018 through
// U+201B as single quotes too, so each is doubled like '.
func TestLiteralDoublesEveryQuoteCharacter(t *testing.T) {
	for value, want := range map[string]string{
		`C:\Program Files\OA\openabstractions.exe`: `'C:\Program Files\OA\openabstractions.exe'`,
		`C:\it's`:                       `'C:\it''s'`,
		"C:\\a\u2018b":                  "'C:\\a\u2018\u2018b'",
		"C:\\a\u2019b":                  "'C:\\a\u2019\u2019b'",
		"C:\\a\u201Ab":                  "'C:\\a\u201A\u201Ab'",
		"C:\\a\u201Bb":                  "'C:\\a\u201B\u201Bb'",
		"\u2019; Remove-Item x; \u2018": "'\u2019\u2019; Remove-Item x; \u2018\u2018'",
		"C:\\a\u201Cb\u2017c\u201D":     "'C:\\a\u201Cb\u2017c\u201D'",
		"":                              "''",
	} {
		if got := Literal(value); got != want {
			t.Errorf("Literal(%q) = %q, want %q", value, got, want)
		}
	}
}

func TestEncodedCommandIsUTF16LittleEndianBase64(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(EncodedCommand("a\u2019\U0001F600"))
	if err != nil || len(raw)%2 != 0 {
		t.Fatalf("%v %d", err, len(raw))
	}
	units := make([]uint16, len(raw)/2)
	for i := range units {
		units[i] = uint16(raw[2*i]) | uint16(raw[2*i+1])<<8
	}
	if got := string(utf16.Decode(units)); got != "a\u2019\U0001F600" {
		t.Fatalf("decoded %q", got)
	}
}
