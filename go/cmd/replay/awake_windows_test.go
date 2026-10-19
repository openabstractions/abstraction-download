package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// A standalone replay must exercise the real native inhibitor without an
// installed Resource service. A declared capability and a live job hold both
// pass through Resource's platform owner; ending the job releases that request.
func TestWindowsReplayInhibitorWithoutInstalledResource(t *testing.T) {
	t.Setenv("ABSTRACTION_RESOURCE_ENDPOINT", fmt.Sprintf(`\\.\pipe\replay-absent-%d`, os.Getpid()))
	if !strings.Contains(" "+capabilities()+" ", " inhibitor ") {
		t.Fatal("standalone replay did not acquire and release the native inhibitor")
	}
	store, err := job.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := &replay{store: store, work: t.TempDir(), ids: map[string]string{},
		epochs: map[string]int64{}, holds: map[string]*job.Hold{}}
	for _, step := range []struct{ command, want string }{
		{"submit A size=64 digest=good src=file", "ok"},
		{"hold A", "awake=no"},
		{"claim A alpha 60000", "ok"},
		{"hold A", "awake=yes"},
	} {
		got := p.do(strings.Fields(step.command))
		t.Logf("%s -> %s", step.command, got)
		if !strings.Contains(got, step.want) {
			t.Fatalf("%s: want %s", step.command, step.want)
		}
	}
	held := p.holds[p.ids["A"]]
	defer held.Release()
	if got := p.do([]string{"release", "A", "alpha"}); !strings.HasPrefix(got, "ok ") {
		t.Fatal(got)
	}
	deadline, ok := t.Deadline()
	if !ok {
		t.Fatal("native fixture requires the caller's test deadline")
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-held.Done():
	case <-timer.C:
		t.Fatal("native hold outlived the released job lease")
	}
	if got := p.state("A"); !strings.Contains(got, "awake=no") {
		t.Fatal(got)
	}
}
