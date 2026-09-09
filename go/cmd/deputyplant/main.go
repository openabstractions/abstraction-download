package main

import (
	"flag"
	"fmt"
	"os"

	dl "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
)

func main() {
	fs := flag.NewFlagSet("deputyplant", flag.ExitOnError)
	sink := fs.String("sink", "files/loot.bin", "where the record tells the adopter to write")
	credential := fs.String("credential", "hf", "the credential name the record asks for; empty asks for none")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: deputyplant [--sink path] [--credential name] <store> <locator>")
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])
	if fs.NArg() != 2 {
		fs.Usage()
		os.Exit(2)
	}
	store, err := job.NewFileStore(fs.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	src := dl.Source{Scheme: "http", Locator: fs.Arg(1)}
	if *credential != "" {
		src.Attrs = map[string]string{dl.CredentialAttr: *credential}
	}
	id, err := dl.Submit(store, dl.Spec{Sources: []dl.Source{src}, Sink: dl.Sink{Final: *sink}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(id)
}
