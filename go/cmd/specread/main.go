// specread parses a download spec and prints what this implementation
// understood, in a form another implementation can be compared against.
//
// # Why this exists
//
// The job RECORD has cross-language conformance tests: three implementations
// pass one record around and must agree byte for byte. The SPEC inside it has
// none — and could not, because the job layer refuses to look inside a spec,
// which is exactly what lets download evolve without a schema change in three
// languages.
//
// That opacity is right and it leaves a hole. A digest written bare by one
// implementation reached another that built "sha256:" + hex and compared
// strings; the error said "got sha256:1fc70f… want 1fc70f…", the same digest
// twice, and a correct 1.5 GB download was deleted and fetched again. Nothing
// was going to catch that, because nothing checked that three implementations
// read a spec the same way.
//
// So the record's conformance is by identical BYTES, and the spec's is by
// identical MEANING. This prints the meaning.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	download "github.com/ReinisLusis/abstraction-download"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: specread <spec.json>")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "specread:", err)
		os.Exit(1)
	}
	var spec download.Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		fmt.Fprintln(os.Stderr, "specread:", err)
		os.Exit(1)
	}

	// Normalised, not verbatim. Two implementations may legitimately hold a
	// digest as "sha256:<hex>" or bare, and the contract is that they mean the
	// same artifact — so what is compared is the meaning each arrived at.
	fmt.Printf("digest=%s\n", download.NormalDigest(spec.Artifact.Digest))
	fmt.Printf("size=%d\n", spec.Artifact.Size)
	fmt.Printf("final=%s\n", download.Portable(spec.Sink.Final))
	fmt.Printf("partial=%s\n", download.Portable(spec.Sink.Partial))

	// Sources in the order they would be tried, because that order is the
	// behaviour: a local copy at priority -100 is what turns a download into a
	// copy, and an implementation that sorted differently would fetch over the
	// network while the bytes sat on disk.
	srcs := append([]download.Source(nil), spec.Sources...)
	sort.SliceStable(srcs, func(i, j int) bool { return srcs[i].Priority < srcs[j].Priority })
	for i, s := range srcs {
		fmt.Printf("source%d=%s|%s\n", i, s.Scheme, s.Locator)
	}
}
