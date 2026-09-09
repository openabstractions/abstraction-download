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
	"strings"

	download "github.com/openabstractions/abstraction-download/go"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: specread <spec.json>\n"+
			"       specread --echo <spec.json>\n"+
			"       specread --partial <final> <id>\n"+
			"       specread --portable <path>\n"+
			"       specread --reserved <owner-id> <path>\n"+
			"       specread --foreign <path>")
		os.Exit(2)
	}
	// The spelling a path gets when it is written into a record. Every other
	// check here reads a path back; this one is the only view of what the layer
	// WROTE, and the disagreement it pins was visible in a window before it was
	// visible to any of them: two finished jobs whose destinations were spelled
	// differently, one of them changing convention halfway along.
	if os.Args[1] == "--portable" {
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: specread --portable <path>")
			os.Exit(2)
		}
		fmt.Printf("portable=%s\n", download.Portable(os.Args[2]))
		return
	}
	// The partial name a caller who chose none would get. It is not read out of
	// a spec — it is INVENTED, by whichever implementation happens to submit —
	// and it then lands in the record for the others to resume from. Two
	// implementations inventing it independently is the same shape of bug as the
	// digest that cost a 1.5 GB download, so it is compared here too.
	if os.Args[1] == "--partial" {
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: specread --partial <final> <id>")
			os.Exit(2)
		}
		fmt.Printf("partial=%s\n", download.PartialFor(download.Portable(os.Args[2]), os.Args[3]))
		return
	}
	// Whether a sink names the store's own layout. Contained paths, all of
	// them, and every one of them able to overwrite a job record or another
	// job's partial — so the three implementations have to refuse exactly the
	// same set, and the set is not spellable in a fixture because it depends on
	// which job is asking.
	if os.Args[1] == "--reserved" {
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: specread --reserved <owner-id> <path>")
			os.Exit(2)
		}
		fmt.Printf("reserved=%s\n", refusal(download.ReservedSink(os.Args[2], os.Args[3])))
		return
	}
	// Whether this machine may write an absolute sink at all. The only answer
	// here that depends on the host, and it must still be the same answer from
	// all three implementations ON that host.
	if os.Args[1] == "--foreign" {
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: specread --foreign <path>")
			os.Exit(2)
		}
		fmt.Printf("foreign=%s\n", refusal(download.ForeignPath(os.Args[2])))
		return
	}
	// The spec as this implementation would carry it in a record. Go holds it as
	// json.RawMessage and Python and C++ hold it parsed, so a spec's bytes may
	// change on the way through one implementation and not another — a number
	// respelled, an escape policy applied — and every reader downstream sees the
	// changed ones. Compared compact because whitespace is the record writer's
	// choice; escapes and number spellings survive compaction and are not.
	if os.Args[1] == "--echo" {
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: specread --echo <spec.json>")
			os.Exit(2)
		}
		raw, err := os.ReadFile(os.Args[2])
		if err != nil {
			fmt.Fprintln(os.Stderr, "specread:", err)
			os.Exit(1)
		}
		out, err := json.Marshal(json.RawMessage(raw))
		if err != nil {
			fmt.Fprintln(os.Stderr, "specread:", err)
			os.Exit(1)
		}
		os.Stdout.Write(append([]byte("echo="), out...))
		os.Stdout.Write([]byte("\n"))
		return
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

	// Whether each sink path stays under the store root, and in the same words
	// everywhere. A relative path that climbs out of the root is refused by the
	// machine that would do the writing, and the three implementations have to
	// refuse the same records for the same stated reason — otherwise a record
	// one of them will not touch is quietly acted on by another. Empty means
	// nothing to refuse, the same way an unreadable digest reads as empty.
	fmt.Printf("final_refusal=%s\n", refusal(download.EscapesRoot(spec.Sink.Final)))
	fmt.Printf("partial_refusal=%s\n", refusal(download.EscapesRoot(spec.Sink.Partial)))

	// Sources in the order they would be tried, because that order is the
	// behaviour: a local copy at priority -100 is what turns a download into a
	// copy, and an implementation that sorted differently would fetch over the
	// network while the bytes sat on disk.
	srcs := append([]download.Source(nil), spec.Sources...)
	sort.SliceStable(srcs, func(i, j int) bool { return srcs[i].Priority < srcs[j].Priority })
	// Which keys are a description of the source and which are sent to the
	// server. One bag used to be both, so an attribute nobody remembered to
	// exclude went out as a header — and no unit test in one language could see
	// that another language classified the same key differently. This is the
	// line that makes the split a contract rather than one implementation's
	// habit.
	for i, s := range srcs {
		fmt.Printf("source%d=%s|%s\n", i, s.Scheme, s.Locator)
		fmt.Printf("source%d.attrs=%s\n", i, pairs(s.Attrs))
		fmt.Printf("source%d.headers=%s\n", i, pairs(s.Headers))
	}
}

func pairs(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+m[k])
	}
	return strings.Join(out, ",")
}

func refusal(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
