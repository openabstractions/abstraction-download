package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestAnAddressIsOneRuleOnBothPlatforms(t *testing.T) {
	list := []refusal{{"127.0.0.1", "not this week, the disk is full"}}
	win, warnings := windows(list, "")
	if len(warnings) != 0 || !strings.Contains(win, "-RemoteAddress '127.0.0.1'") || strings.Contains(win, "Dynamic") {
		t.Fatalf("windows:\n%s\n%v", win, warnings)
	}
	nft, warnings := linux(list, "")
	want := `ct state new ip daddr 127.0.0.1 counter log prefix "reach 127.0.0.1: not this week, the disk is full " reject with icmpx admin-prohibited`
	if len(warnings) != 0 || !strings.Contains(nft, want) {
		t.Fatalf("linux:\n%s\n%v", nft, warnings)
	}
}

func TestANameIsAResolvedKeywordOnWindowsAndAnEmptySetOnLinux(t *testing.T) {
	list := []refusal{{"huggingface.co", "no"}}
	win, warnings := windows(list, "")
	ids := regexp.MustCompile(`-Keyword '(\*\.)?huggingface\.co'`).FindAllString(win, -1)
	if len(ids) != 2 || len(warnings) != 1 || !strings.Contains(win, "EnableNetworkProtection") {
		t.Fatalf("windows:\n%s\n%v", win, warnings)
	}
	again, _ := windows(list, "")
	if again != win || !regexp.MustCompile(`-Id '\{[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\}'`).MatchString(win) {
		t.Fatalf("keyword ids are not stable v5 uuids:\n%s", win)
	}
	nft, warnings := linux(list, "")
	if len(warnings) != 1 || !strings.Contains(nft, "set host_huggingface_co {") || !strings.Contains(nft, "ip6 daddr @host_huggingface_co6 counter") {
		t.Fatalf("linux:\n%s\n%v", nft, warnings)
	}
}

func TestAReasonCannotLeaveItsQuotes(t *testing.T) {
	list := []refusal{{"10.0.0.1", `it's "done" \ now`}}
	win, _ := windows(list, "")
	if !strings.Contains(win, `-Description 'it''s "done" \ now'`) {
		t.Fatal(win)
	}
	nft, _ := linux(list, "")
	if !strings.Contains(nft, `log prefix "reach 10.0.0.1: it's \"done\" \\ now "`) {
		t.Fatal(nft)
	}
}

func TestTheKernelLogKeepsAtMost127Bytes(t *testing.T) {
	r := refusal{"10.0.0.1", strings.Repeat("é", 200)}
	p := prefix(r)
	if len(p) > prefixBytes || len(p) < prefixBytes-1 || !utf8.ValidString(p) {
		t.Fatalf("%d bytes, valid=%v", len(p), utf8.ValidString(p))
	}
}

func TestAProgramOrGroupScopesEveryRule(t *testing.T) {
	list := []refusal{{"10.0.0.1", "a"}, {"example.org", "b"}}
	win, _ := windows(list, `C:\Program Files\App\app.exe`)
	if strings.Count(win, `-Program 'C:\Program Files\App\app.exe'`) != 2 {
		t.Fatal(win)
	}
	nft, _ := linux(list, "60001")
	if strings.Count(nft, "\t\tct state new meta skgid 60001 ip") != 3 {
		t.Fatal(nft)
	}
}

func TestAnEmptyListRemovesWhatWasThere(t *testing.T) {
	win, _ := windows(nil, "")
	if !strings.Contains(win, "Remove-NetFirewallRule") || strings.Contains(win, "New-") {
		t.Fatal(win)
	}
	nft, _ := linux(nil, "")
	if !strings.Contains(nft, "delete table inet reachwall") || strings.Contains(nft, "reject") {
		t.Fatal(nft)
	}
}

func TestTheFileIsReadSortedAndFlattened(t *testing.T) {
	path := filepath.Join(t.TempDir(), "refused.json")
	os.WriteFile(path, []byte("{\"b.example\": \"two\\nlines\", \"a.example\": \"one\"}"), 0o600)
	list, err := load(path)
	if err != nil || len(list) != 2 || list[0].host != "a.example" || list[1].reason != "two lines" {
		t.Fatal(list, err)
	}
	if _, err := load(filepath.Join(t.TempDir(), "absent.json")); err != nil {
		t.Fatal(err)
	}
}
