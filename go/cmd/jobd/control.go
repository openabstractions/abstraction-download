package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	download "github.com/ReinisLusis/abstraction/download/go"
	job "github.com/ReinisLusis/abstraction/job/go"
)

// This file is jobd controlling jobd: start it in the background, stop it, and
// say whether one is running.
//
// It lives in the tool rather than in a shell script on purpose. A script would
// have to open supervisor.json and pick the pid out of the JSON — reaching
// through the abstraction into the file binding, which is the thing this project
// spends its time removing. jobd already owns that file. A script would also
// only run on the machine it was written for, and the other end of this chain is
// a Linux NAS.

// supervisorPID returns the process id of the supervisor watching this store,
// and whether it is alive.
//
// The heartbeat carries owner as "program@host:pid". Trusting the pid alone
// would be wrong across machines — a NAS supervisor's pid means nothing here —
// so the host has to match before the number is worth anything.
func supervisorPID(store job.Store) (int, bool) {
	sup, live := download.SupervisorOf(store)
	if !live {
		return 0, false
	}
	host, _ := os.Hostname()
	if !strings.EqualFold(sup.Host, host) {
		return 0, false
	}
	i := strings.LastIndex(sup.Owner, ":")
	if i < 0 {
		return 0, false
	}
	pid, err := strconv.Atoi(sup.Owner[i+1:])
	if err != nil {
		return 0, false
	}
	return pid, true
}

// cmdStop ends the supervisor watching this store. Nothing it was doing is lost:
// leases lapse, records stay, and the next supervisor adopts the work.
func cmdStop() {
	_, store, _ := openRunner()
	sup, live := download.SupervisorOf(store)
	if !live {
		fmt.Println("no supervisor is running on this store")
		download.StopHeartbeat(store)
		return
	}
	pid, ok := supervisorPID(store)
	if !ok {
		fmt.Printf("a supervisor is announced as %s, but not on this machine — stop it there\n", sup.Owner)
		return
	}
	p, err := os.FindProcess(pid)
	if err == nil {
		_ = p.Kill()
	}
	// Remove the announcement even if the kill raced, so applications stop
	// handing work to something that has gone. A stale heartbeat is treated as
	// dead after a few intervals anyway; this just makes it immediate.
	download.StopHeartbeat(store)
	fmt.Printf("stopped %s\n", sup.Owner)
}

// cmdStart replaces whatever is running with a fresh detached supervisor.
func cmdStart(args []string) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	interval := fs.Duration("interval", 30*time.Second, "how often to sweep")
	var without systems
	fs.Var(&without, "without", `run one tier lower by ignoring a system; repeatable, e.g. --without nas --without bits`)
	fs.Parse(args)

	_, store, _ := openRunner()
	if _, live := download.SupervisorOf(store); live {
		cmdStop()
		// Give the old process a moment to release its handles before the new
		// one announces itself, so `jobd status` never shows two.
		time.Sleep(300 * time.Millisecond)
	}

	self, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	childArgs := []string{"run", "--interval", interval.String()}
	for _, w := range without {
		childArgs = append(childArgs, "--without", w)
	}

	logPath := filepath.Join(storeRoot(), "jobd.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fatal(err)
	}
	defer logFile.Close()

	cmd := exec.Command(self, childArgs...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = detached()
	if err := cmd.Start(); err != nil {
		fatal(err)
	}
	// Do not wait for it. Release it so this process can exit without leaving a
	// zombie on unix.
	_ = cmd.Process.Release()

	// Wait for it to announce itself rather than printing success on the
	// strength of having called exec. A supervisor that failed to start looks
	// exactly like one that started, until somebody looks at the store.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if sup, live := download.SupervisorOf(store); live {
			fmt.Printf("started %s\n", sup.Owner)
			fmt.Printf("  store        %s\n", storeRoot())
			fmt.Printf("  delegates to %s\n", sup.Tier)
			fmt.Printf("  log          %s\n", logPath)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "jobd: started, but it never announced itself. See %s\n", logPath)
	os.Exit(1)
}
