package main

import (
	"flag"
	"fmt"
	"os"

	config "github.com/openabstractions/abstraction-config/go"
	download "github.com/openabstractions/abstraction-download/go"
)

// cmdSetup writes the machine's configuration once, so that no application ever
// has to be configured again.
//
// This is the step that makes discovery work for programs that know nothing —
// a fork of Lemonade, ComfyUI, anything. They call download.Discover and get the
// right tier because somebody, once, said what this machine has. Compare with
// the alternative, which is every application growing its own NAS setting.
func cmdSetup(args []string) {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	nasStore := fs.String("nas-store", "", "a job store on a share that a jobd elsewhere watches")
	store := fs.String("store", "", "the local job store (default ~/.abstraction)")
	logSink := fs.String("log-sink", "", "a file every tool appends structured log records to")
	logService := fs.String("log-service", "", "a local socket that attests identity")
	machine := fs.Bool("machine", false, "write the machine-wide file instead of this user's")
	show := fs.Bool("show", false, "print what is configured and where it came from")
	fs.Parse(args)

	if *show {
		cfg := config.Load()
		fmt.Print(cfg.Describe())
		fmt.Printf("\ntiers linked into this build: %v\n", download.RegisteredTiers())
		return
	}

	path := config.UserPath()
	if *machine {
		path = config.MachinePath()
		if path == "" {
			fatal(fmt.Errorf("no machine-wide configuration location on this OS"))
		}
	}

	// Start from what is already there, so setting one thing does not silently
	// unset another.
	cfg := config.Load()
	cfg.From = ""
	if *nasStore != "" {
		cfg.NASStore = *nasStore
	}
	if *store != "" {
		cfg.Store = *store
	}
	if *logSink != "" {
		cfg.LogSink = *logSink
	}
	if *logService != "" {
		cfg.LogService = *logService
	}

	if err := config.Save(path, cfg); err != nil {
		fatal(err)
	}
	fmt.Printf("wrote %s\n\n", path)
	fmt.Print(config.Load().Describe())
	fmt.Println()
	fmt.Println("Every tool that speaks these abstractions now finds this by itself.")
	fmt.Println("Nothing else needs configuring, and no application needs to know.")
	os.Exit(0)
}
