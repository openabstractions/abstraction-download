package main

import (
	"errors"
	"flag"
	"testing"
)

func TestParseGetRefuses(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
	}{
		{"unknown flag with a value", []string{"https://h/a", "--verify", "sha256:ab"}},
		{"unknown flag alone", []string{"https://h/a", "--quiet"}},
		{"digest with nothing after it", []string{"https://h/a", "--digest"}},
		{"o with nothing after it", []string{"https://h/a", "-o"}},
		{"digest given an empty value", []string{"https://h/a", "--digest="}},
		{"o given an empty value", []string{"https://h/a", "-o", ""}},
		{"a second positional", []string{"https://h/a", "-o", "d", "https://h/b"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, _, _, err := parseGet(c.args); err == nil {
				t.Fatal("accepted; a flag dl does not honour must be a refusal")
			}
		})
	}
}

func TestParseGetAccepts(t *testing.T) {
	for _, c := range []struct {
		name             string
		args             []string
		wantOut, wantDig string
	}{
		{"bare url", []string{"https://h/a"}, ".", ""},
		{"digest last", []string{"https://h/a", "--digest", "sha256:ab"}, ".", "sha256:ab"},
		{"single dash digest", []string{"https://h/a", "-digest", "sha256:ab"}, ".", "sha256:ab"},
		{"o last", []string{"https://h/a", "-o", "d"}, "d", ""},
		{"both", []string{"https://h/a", "-o", "d", "--digest", "sha256:ab"}, "d", "sha256:ab"},
	} {
		t.Run(c.name, func(t *testing.T) {
			url, out, dig, err := parseGet(c.args)
			if err != nil {
				t.Fatalf("parseGet: %v", err)
			}
			if url != "https://h/a" || out != c.wantOut || dig != c.wantDig {
				t.Fatalf("got %q %q %q, want %q %q %q", url, out, dig, "https://h/a", c.wantOut, c.wantDig)
			}
		})
	}
}

func TestParseGetNamesWhyItRefused(t *testing.T) {
	if _, _, _, err := parseGet([]string{"ftp.example.com/a"}); !errors.Is(err, errNotAURL) {
		t.Fatalf("err = %v, want errNotAURL", err)
	}
	if _, _, _, err := parseGet([]string{"https://h/a", "-h"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
}
