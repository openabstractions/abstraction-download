package netcost

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func next(t *testing.T, w *Watcher) Cost {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := w.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	return c
}

// A listener is told the present at once, then only a different cost, and a
// repeated platform notice of the same cost wakes nobody.
func TestFakeNoticesChangesOnly(t *testing.T) {
	f := NewFake(Unmetered)
	w := f.Watch()
	defer w.Close()
	if c := next(t, w); c != Unmetered {
		t.Fatalf("first notice %q", c)
	}
	f.Set(Unmetered)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	if c, err := w.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("an unchanged cost woke the listener: %q %v", c, err)
	}
	cancel()
	f.Set(Metered)
	if c := next(t, w); c != Metered || f.Cost() != Metered {
		t.Fatalf("metered notice %q, cost %q", c, f.Cost())
	}
	f.Set(Unmetered)
	if c := next(t, w); c != Unmetered {
		t.Fatalf("unmetered notice %q", c)
	}
}

// The listener waits on the notice: nothing wakes it between changes, and a
// change made while it waits is delivered.
func TestFakeWakesAWaitingListener(t *testing.T) {
	f := NewFake(Metered)
	w := f.Watch()
	defer w.Close()
	next(t, w)
	got := make(chan Cost, 1)
	go func() {
		c, _ := w.Next(context.Background())
		got <- c
	}()
	select {
	case c := <-got:
		t.Fatalf("woke with no change: %q", c)
	case <-time.After(100 * time.Millisecond):
	}
	f.Set(Unmetered)
	select {
	case c := <-got:
		if c != Unmetered {
			t.Fatalf("woke with %q", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the change never reached the waiting listener")
	}
}

func TestClosingEndsWatchers(t *testing.T) {
	f := NewFake(Unknown)
	w := f.Watch()
	next(t, w)
	f.Close()
	if _, err := w.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
	late := f.Watch()
	if _, err := late.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("watch after close: %v", err)
	}
	own := NewFake(Unknown)
	mine := own.Watch()
	mine.Close()
	if _, err := mine.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed watcher: %v", err)
	}
	own.Set(Metered) // a closed watcher is no longer posted to
}

func inWSL() bool {
	if os.Getenv("WSL_DISTRO_NAME") != "" {
		return true
	}
	b, err := os.ReadFile("/proc/sys/kernel/osrelease")
	return err == nil && strings.Contains(strings.ToLower(string(b)), "microsoft")
}

// TestPlatformSource reads this machine's real cost source without changing
// anything. On Windows it reports the Network List Manager's machine-wide
// cost; in WSL the source is unavailable.
func TestPlatformSource(t *testing.T) {
	s, err := Open()
	switch {
	case runtime.GOOS == "windows":
		if err != nil {
			if expectedPlatformUnavailable(err) {
				t.Skipf("Windows Server has no Network List Manager cost source: %v", err)
			}
			t.Fatalf("Windows NLM source: %v", err)
		}
	case inWSL():
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("WSL must report the source unavailable, got %v", err)
		}
		t.Logf("WSL: %v", err)
		return
	case errors.Is(err, ErrUnavailable):
		t.Skipf("no cost source on this host: %v", err)
	case err != nil:
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	w := s.Watch()
	defer w.Close()
	c := next(t, w)
	if c != s.Cost() || (c != Unknown && c != Unmetered && c != Metered) {
		t.Fatalf("cost %q, source says %q", c, s.Cost())
	}
	t.Logf("%s: this machine's network cost is %s", runtime.GOOS, c)
}
