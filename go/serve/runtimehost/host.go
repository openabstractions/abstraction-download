// Package runtimehost owns an installed central runtime process and its lifecycle.
package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrForced = errors.New("runtimehost: graceful shutdown expired; forced termination requested")

type Options struct {
	Executable                                    string   // Empty selects the exact installed sibling; overrides must be absolute.
	Args                                          []string // Runtime flags appended to serve runtime --supervised.
	StartupTimeout, ShutdownTimeout, ForceTimeout time.Duration
	Stderr                                        *os.File
}
type process interface {
	wait() error
	terminate() error
	pid() int
}
type Host struct {
	child             process
	control           *os.File
	output            *os.File
	done              chan struct{}
	shutdown, force   time.Duration
	closeOnce         sync.Once
	closeErr, exitErr error
	forced            atomic.Bool
}

func (h *Host) PID() int { return h.child.pid() }

// Wait returns the observed child result, including failures concurrent with Close.
func (h *Host) Wait() error {
	<-h.done
	if h.exitErr == nil && h.forced.Load() {
		return ErrForced
	}
	return h.exitErr
}
func (h *Host) Close() error {
	h.closeOnce.Do(func() {
		h.control.Close()
		timer := time.NewTimer(h.shutdown)
		defer timer.Stop()
		select {
		case <-h.done:
			h.closeErr = h.exitErr
			return
		case <-timer.C:
		}
		forceDeadline := time.Now().Add(h.force)
		h.forced.Store(true)
		if err := h.child.terminate(); err != nil {
			h.closeErr = err
			return
		}
		timer.Reset(time.Until(forceDeadline))
		select {
		case <-h.done:
			h.closeErr = errors.Join(ErrForced, h.exitErr)
		case <-timer.C:
			h.closeErr = fmt.Errorf("%w: exit not observed within force deadline", ErrForced)
		}
	})
	return h.closeErr
}
func Start(ctx context.Context, o Options) (*Host, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, p := range []*time.Duration{&o.StartupTimeout, &o.ShutdownTimeout, &o.ForceTimeout} {
		if *p < 0 {
			return nil, errors.New("runtimehost: negative timeout")
		}
	}
	if o.StartupTimeout == 0 {
		o.StartupTimeout = 10 * time.Second
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = 5 * time.Second
	}
	if o.ForceTimeout == 0 {
		o.ForceTimeout = 5 * time.Second
	}
	if o.Executable == "" {
		self, err := os.Executable()
		if err != nil {
			return nil, err
		}
		o.Executable = filepath.Join(filepath.Dir(self), executableName)
	}
	if !filepath.IsAbs(o.Executable) {
		return nil, errors.New("runtimehost: executable must be absolute")
	}
	for _, arg := range o.Args {
		name := strings.TrimLeft(arg, "-")
		if name == "supervised" || strings.HasPrefix(name, "supervised=") {
			return nil, errors.New("runtimehost: Args cannot override supervision")
		}
	}

	child, control, output, err := launch(ctx, o)
	if err != nil {
		return nil, err
	}
	h := &Host{child: child, control: control, output: output, done: make(chan struct{}), shutdown: o.ShutdownTimeout, force: o.ForceTimeout}
	go func() { h.exitErr = child.wait(); control.Close(); output.Close(); close(h.done) }()
	ready := make(chan error, 1)
	go func() {
		const marker = "READY 1\n"
		b := make([]byte, len(marker))
		_, err := io.ReadFull(output, b)
		if err == nil && string(b) != marker {
			err = fmt.Errorf("runtimehost: malformed readiness %q", b)
		}
		ready <- err
		if err == nil {
			io.Copy(io.Discard, output)
		}
	}()
	timer := time.NewTimer(o.StartupTimeout)
	defer timer.Stop()
	select {
	case err = <-ready:
		if err != nil {
			h.Close()
			return nil, fmt.Errorf("runtimehost readiness: %w", err)
		}
	case <-h.done:
		return nil, fmt.Errorf("runtimehost: exited before readiness: %v", h.exitErr)
	case <-ctx.Done():
		h.Close()
		return nil, ctx.Err()
	case <-timer.C:
		h.Close()
		return nil, errors.New("runtimehost: readiness deadline expired")
	}
	if err := ctx.Err(); err != nil {
		h.Close()
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			h.Close()
		case <-h.done:
		}
	}()
	return h, nil
}
