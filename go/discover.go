package download

import (
	"os"
	"strings"

	config "github.com/openabstractions/abstraction-config/go"
	job "github.com/openabstractions/abstraction-job/go"
)

// DiscoverIn builds a runner over a store the caller already has open. It
// registers every delegation tier this machine's configuration names and can
// reach, and the default refusals. Callers that will not accept their program's
// behaviour changing because something got installed name the executor with
// NewClient(DiscoverIn(store), WithExecution(...)). See Execution.
func DiscoverIn(store job.Store) *Runner {
	r := NewRunner(store, Owner())
	r.Reach = DefaultRefusals().Check
	r.Rebind()
	return r
}

// Rebind rebuilds the delegation chain from what the machine says now, and
// returns what would serve.
//
// Discovering once is right for an application that runs for a minute and wrong
// for a supervisor that runs for days. A person who switches a tier off, or
// points this machine at a NAS for the first time, was obeyed only by processes
// started afterwards — so the switch was a setting rather than a control, and
// every one of them ended up being an environment variable read at startup.
// Refusals already solved the same problem the same way: read at the moment the
// decision is taken, and nothing restarts.
//
// Rebuilding probes a share, so it happens only when the machine's answer has
// actually changed. Asking is two small file reads.
//
// Called from whichever goroutine takes the sweep. The chain is a plain field
// read all over this package and this method writes it; a process that also
// reads it elsewhere publishes what it needs instead of reading across.
func (r *Runner) Rebind() string {
	cfg := machineConfig()
	if stamp := cfg.Stamp() + "|" + strings.Join(r.NotServing, ","); stamp != r.bound || r.Delegators == nil {
		r.bound = stamp
		ds := NewDelegators(available(cfg)...)
		for _, s := range r.NotServing {
			ds = ds.Without(s)
		}
		// The chain this replaces built its delegates from a probe and was the
		// only thing holding them. A plugin's delegate owns a child process, so
		// rebinding without this leaks one per rebind — and a supervisor
		// rebinds for days.
		old := r.Delegators
		r.Delegators = ds
		old.Close()
	}
	return r.Tier()
}

// Close releases the delegation chain. A delegate may own a process.
func (r *Runner) Close() error { return r.Delegators.Close() }

// Tier names what would answer, for a status line. An application can tell its
// user "this is going to your NAS" without knowing what a NAS is.
func (r *Runner) Tier() string {
	if r.Delegators == nil || len(r.Delegators.all) == 0 {
		return "here"
	}
	return r.Delegators.all[0].System()
}

// machineConfig is this process's reading of the machine's delegation tiers:
// the configuration files with this process's own ABSTRACTION_* values as the
// run overrides. The legacy download client is the provider that owns those
// values; the config layer exports no process-environment loader since 0.1.8.
func machineConfig() config.Config {
	values := map[string]string{}
	for _, name := range config.EnvVars {
		values[name] = os.Getenv(name)
	}
	return config.LoadWithOverrides(values)
}
