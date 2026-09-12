package download

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// StartSupervisor is the named call that brings a supervisor into existence.
// The tool makes it for `jobd start`; an installer makes it once at the end of
// an install; a platform registration makes it not at all, because the platform
// starts the program itself.
//
// Nothing on the client path may call it. A client that found no service
// answers "no service" — Nudge and Who return ErrNoSupervisor and the work runs
// in the calling process — because a client that starts a helper it could not
// find has quietly become its own provider, which is the thing SLF4J does not
// do.
// TestAClientThatFoundNoServiceDidNotStartOne is where that boundary is kept.
//
// It is idempotent, and the platform decides that rather than this code: the
// endpoint admits one listener, so a second starter is refused by name instead
// of losing a race on timing (listen.ErrTaken, and the first-instance flag on
// Windows). Two installers, a tool and a logon entry may all call it at once
// and exactly one supervisor exists afterwards.
type Starting struct {
	// Endpoint is the name the supervisor will hold, and the name this call
	// asks first. DefaultEndpoint is what a supervisor nobody told otherwise
	// listens at.
	Endpoint string
	// Exe and Args are the resident program and the arguments that make it
	// serve. This package does not know what the program is called: jobd,
	// openabstractions and a service registration all name it differently.
	Exe  string
	Args []string
	// Log is where the child's output goes. A detached child has no terminal to
	// complain to, so a start that fails must leave its reason somewhere a
	// person can be pointed at.
	Log string
	// Within bounds the wait for the child to take the name. Zero means the
	// default below.
	Within time.Duration
}

// Started is what the call did, so a caller can say "already running" rather
// than claiming to have started something.
type Started struct {
	Endpoint string
	Already  bool
	Owner    string
	Tier     string
	Log      string
	PID      int
}

// ErrNeverAnswered is a child that started and did not take the endpoint. It is
// not the same as a failure to spawn: something ran, and the log says why it
// did not get as far as listening.
var ErrNeverAnswered = errors.New("download: the supervisor started and never answered at its endpoint")

const (
	// A start waits this long for the child to bind its name. It is a ceiling
	// on an error, not a schedule: the two things that end this wait early are
	// the endpoint answering and the child exiting, and only a child that
	// neither answers nor exits reaches it.
	startWithin = 10 * time.Second
	// How often the endpoint is asked while waiting. There is no notification
	// on any of the three platforms for "a name appeared" — a named pipe lives
	// in npfs, which raises no change notification, and a unix socket appears
	// by rename in a directory this process may not watch — so the appearing of
	// the name is polled and the child's exit is not.
	startAsk = 200 * time.Millisecond
)

func StartSupervisor(s Starting) (Started, error) {
	if s.Endpoint == "" {
		return Started{}, ErrNoName
	}
	if a, err := exchange(s.Endpoint, "who"); err == nil {
		return Started{Endpoint: s.Endpoint, Already: true, Owner: a.Owner, Tier: a.Tier, Log: s.Log}, nil
	} else if errors.Is(err, ErrCallerRefused) {
		return Started{Endpoint: s.Endpoint, Already: true, Owner: a.Owner, Log: s.Log},
			fmt.Errorf("%w: it does not accept this caller", ErrCallerRefused)
	}

	cmd := exec.Command(s.Exe, s.Args...)
	cmd.SysProcAttr = detached()
	if s.Log != "" {
		// Explicit activation owns its diagnostic destination. A fresh user may
		// not have a store directory until the worker first starts.
		if err := os.MkdirAll(filepath.Dir(s.Log), 0o700); err != nil {
			return Started{Endpoint: s.Endpoint, Log: s.Log}, fmt.Errorf("create supervisor log directory: %w", err)
		}
		f, err := os.OpenFile(s.Log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return Started{Endpoint: s.Endpoint, Log: s.Log}, err
		}
		defer f.Close()
		cmd.Stdout, cmd.Stderr = f, f
	}
	if err := cmd.Start(); err != nil {
		return Started{Endpoint: s.Endpoint, Log: s.Log}, err
	}
	done := Started{Endpoint: s.Endpoint, Log: s.Log, PID: cmd.Process.Pid}

	// Waiting on the child is a wait on something: a supervisor that dies
	// before it listens says so at once and names its log, instead of being
	// indistinguishable from a slow one until a timeout expires. A fixed sleep
	// after a kill would stand in for a notification nobody had wired up.
	gone := make(chan error, 1)
	go func() { gone <- cmd.Wait() }()

	ask := time.NewTicker(startAsk)
	defer ask.Stop()
	within := s.Within
	if within == 0 {
		within = startWithin
	}
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	for {
		if a, err := exchange(s.Endpoint, "who"); err == nil {
			done.Owner, done.Tier = a.Owner, a.Tier
			return done, nil
		}
		select {
		case err := <-gone:
			return done, fmt.Errorf("%w before it answered at %s: %v", ErrNeverAnswered, s.Endpoint, err)
		case <-deadline.C:
			return done, fmt.Errorf("%w within %s: %s", ErrNeverAnswered, within, s.Endpoint)
		case <-ask.C:
		}
	}
}
