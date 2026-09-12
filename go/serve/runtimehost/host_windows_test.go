package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/windows"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 4 && os.Args[1] == "serve" && os.Args[2] == "runtime" && os.Args[3] == "--supervised" {
		mode := os.Args[4]
		if mode == "early" {
			os.Exit(17)
		}
		if mode == "missing" {
			for {
				time.Sleep(time.Hour)
			}
		}
		if mode == "malformed" {
			fmt.Print("WRONG 1\n")
			for {
				time.Sleep(time.Hour)
			}
		}
		if mode == "descendant" {
			for {
				time.Sleep(time.Hour)
			}
		}
		if mode == "tree" || mode == "tree-grace" {
			c := exec.Command(os.Args[0], "serve", "runtime", "--supervised", "descendant")
			if err := c.Start(); err != nil {
				panic(err)
			}
			os.WriteFile(filepath.Join(os.Args[5], "descendant"), []byte(strconv.Itoa(c.Process.Pid)), 0600)
		}
		fmt.Print("READY 1\n")
		if mode == "stuck" || mode == "tree" {
			for {
				time.Sleep(time.Hour)
			}
		}
		io.Copy(io.Discard, os.Stdin)
		if mode == "failed-close" {
			os.Exit(19)
		}
		os.Exit(0)
	}
	if len(os.Args) > 2 && os.Args[1] == "owner" {
		h, err := Start(context.Background(), Options{Executable: os.Args[0], Args: []string{"tree", os.Args[2]}})
		if err != nil {
			panic(err)
		}
		os.WriteFile(filepath.Join(os.Args[2], "child"), []byte(strconv.Itoa(h.PID())), 0600)
		h.Wait()
		os.Exit(0)
	}
	os.Exit(m.Run())
}
func options(mode string) Options {
	return Options{Executable: os.Args[0], Args: []string{mode}, StartupTimeout: 3 * time.Second, ShutdownTimeout: 2 * time.Second, ForceTimeout: 3 * time.Second}
}
func TestReadyCloseWait(t *testing.T) {
	h, err := Start(context.Background(), options("ready"))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	var inJob uint32
	p := h.child.(*windowsProcess)
	result, _, callErr := windows.NewLazySystemDLL("kernel32.dll").NewProc("IsProcessInJob").Call(uintptr(p.handle), uintptr(p.job), uintptr(unsafe.Pointer(&inJob)))
	if result == 0 || inJob == 0 {
		err := callErr
		t.Fatalf("job membership: %v %v", inJob, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := h.Wait(); err != nil {
		t.Fatal(err)
	}
}
func TestFailureAndDeadlines(t *testing.T) {
	for _, mode := range []string{"early", "malformed", "missing"} {
		t.Run(mode, func(t *testing.T) {
			o := options(mode)
			o.StartupTimeout = 200 * time.Millisecond
			o.ShutdownTimeout = 100 * time.Millisecond
			start := time.Now()
			h, err := Start(context.Background(), o)
			if h != nil || err == nil {
				if h != nil {
					h.Close()
				}
				t.Fatalf("accepted %s", mode)
			}
			if time.Since(start) > 5*time.Second {
				t.Fatal("startup cleanup exceeded budget")
			}
		})
	}
}
func TestForcedShutdown(t *testing.T) {
	o := options("stuck")
	o.ShutdownTimeout = 100 * time.Millisecond
	h, err := Start(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err = h.Close(); !errors.Is(err, ErrForced) {
		t.Fatalf("force result: %v", err)
	}
	if err = h.Wait(); err == nil {
		t.Fatal("forced child success was reported")
	}
}
func TestChildFailureDuringClose(t *testing.T) {
	h, err := Start(context.Background(), options("failed-close"))
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Close(); err == nil {
		t.Fatal("Close hid child failure")
	}
	if err = h.Wait(); err == nil {
		t.Fatal("Wait hid child failure")
	}
}
func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if h, err := Start(ctx, options("ready")); h != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(h, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	h, err := Start(ctx, options("ready"))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	cancel()
	done := make(chan error, 1)
	go func() { done <- h.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not reap")
	}
	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	o := options("missing")
	o.ShutdownTimeout = 100 * time.Millisecond
	if h, err := Start(ctx, o); h != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(h, err)
	}
}
func TestExecutableMustBeAbsolute(t *testing.T) {
	if h, err := Start(context.Background(), Options{Executable: "openabstractions.exe"}); h != nil || err == nil {
		t.Fatal(h, err)
	}
}
func readPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(string(b))
			if err == nil {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no PID file %s", path)
	return 0
}
func TestParentDeathContainsDescendant(t *testing.T) {
	dir := t.TempDir()
	owner := exec.Command(os.Args[0], "owner", dir)
	owner.Stderr = os.Stderr
	if err := owner.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { owner.Process.Kill(); owner.Wait() }()
	childPID := readPID(t, filepath.Join(dir, "child"))
	descPID := readPID(t, filepath.Join(dir, "descendant"))
	handles := []windows.Handle{}
	for _, pid := range []int{childPID, descPID} {
		h, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_TERMINATE, false, uint32(pid))
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
		defer windows.CloseHandle(h)
		defer windows.TerminateProcess(h, 99)
	}
	if err := owner.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	for _, h := range handles {
		state, err := windows.WaitForSingleObject(h, 5000)
		if err != nil || state != windows.WAIT_OBJECT_0 {
			t.Fatalf("descendant survived owner: %v %v", state, err)
		}
	}
}

func TestGracefulPrimaryExitContainsDescendant(t *testing.T) {
	dir := t.TempDir()
	o := options("tree-grace")
	o.Args = append(o.Args, dir)
	h, err := Start(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	pid := readPID(t, filepath.Join(dir, "descendant"))
	process, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(process)
	defer windows.TerminateProcess(process, 99)
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	if state, err := windows.WaitForSingleObject(process, 0); err != nil || state != windows.WAIT_OBJECT_0 {
		t.Fatalf("descendant survived clean exit: %v %v", state, err)
	}
}
func TestCannotOverrideSupervision(t *testing.T) {
	for _, flag := range []string{"--supervised=false", "-supervised", "--supervised"} {
		o := options("ready")
		o.Args = []string{flag}
		if h, err := Start(context.Background(), o); h != nil || err == nil {
			if h != nil {
				h.Close()
			}
			t.Fatal("accepted override", flag)
		}
	}
}

func TestDefaultDiagnosticsWithoutParentStderr(t *testing.T) {
	original := os.Stderr
	os.Stderr = nil
	defer func() { os.Stderr = original }()
	h, err := Start(context.Background(), options("ready"))
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestExplicitInvalidDiagnosticsRefused(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	o := options("ready")
	o.Stderr = f
	if h, err := Start(context.Background(), o); h != nil || err == nil {
		if h != nil {
			h.Close()
		}
		t.Fatal("invalid supplied stderr accepted")
	}
}
