package download

import (
	"errors"
	"fmt"
	"runtime"
	"strings"

	job "github.com/openabstractions/abstraction-job/go"
)

// ErrReservedPath is a sink that stays inside the store root and aims at the
// store's own contents.
//
// Containment stopped a sink from climbing OUT of the root. It never stopped
// one from naming what is IN it: a final of `jobs/<id>.json` overwrites a job
// record, and a final of `work/<other>` overwrites another job's partial. Both
// are contained, and both were accepted by every check. The confused deputy is
// the same one — a record written by one machine, written to by another with
// the adopter's authority — and the target is now the store itself, so a single
// record could delete every job in a shared store or hand one an arbitrary
// spec.
//
// Refused rather than redirected, for the reason ErrEscapesRoot is: a caller
// that named the wrong place has to be told, and 40 GB delivered somewhere
// nobody asked for is the failure this layer exists to prevent.
var ErrReservedPath = forever("download: sink path is reserved by the store")

// ErrForeignPath is an absolute sink written in the other platform's
// convention.
//
// The contract said an absolute path is left alone, so a Windows path handed to
// Linux "fails with no such file rather than quietly creating a directory".
// That is true of a path being READ and false of a sink: opening
// `D:\models\x.gguf` with O_CREAT on Linux succeeds and makes a file of that
// literal name in the working directory, with a `.part` beside it. The runner
// creates the parent first, so the mirror case is no better — `/mnt/models` on
// Windows becomes `C:\mnt\models` and the bytes land on whatever drive the
// process happened to be on.
//
// So an absolute sink is honoured only by a machine whose convention it is
// written in, and refused by any other. A record meant to be adopted by a
// machine that is not the one that wrote it uses a RELATIVE sink, which is what
// resolving against the store root is for.
//
// Not permanent, deliberately: a path unusable HERE is perfectly usable on the
// machine whose convention it is, which is a reason to leave the job for
// somebody else rather than to end it.
var ErrForeignPath = errors.New("download: sink path names another platform's filesystem")

// besideTheStore are the names this layer keeps in the store root. The store
// owns jobs/, work/ and services.json; these are ours, and a sink must not name
// them either.
//
// Spelled from the constants rather than beside them, so a heartbeat that gets
// renamed cannot leave this list pointing at a file nobody writes any more.
var besideTheStore = map[string]bool{
	heartbeatName:          true,
	heartbeatName + ".tmp": true,
	nudgeName:              true,
}

// ReservedSink refuses a sink that names the store's own layout, or this
// layer's own files beside it.
//
// owner is the id of the job the sink belongs to. Its own scratch is not
// reserved against it — that is where its partial goes — and is reserved
// against every other job. An empty owner reserves the whole of the work area,
// which is the right answer for a caller that has no id yet.
//
// Absolute paths are not this check's business: they are never joined onto the
// root, so they name the store's contents only by a coincidence no rule here
// could see. What a machine should do with an absolute sink is ErrForeignPath's
// question.
func ReservedSink(owner, p string) error {
	if !relativeEverywhere(p) {
		return nil
	}
	if job.Reserved(owner, p) || besideTheStore[job.RootName(p)] {
		return fmt.Errorf("%w: %s", ErrReservedPath, p)
	}
	return nil
}

// ForeignPath refuses an absolute sink written in a convention this machine
// does not use. See ErrForeignPath.
//
// A record carrying one is not invalid — it is valid on the machine that wrote
// it — so this is asked by the machine about to do the writing and never at
// submission. That is the same division ErrEscapesRoot draws between EscapesRoot
// and Sink.Resolve, for the same reason: a record has no platform.
func ForeignPath(p string) error {
	if p == "" || relativeEverywhere(p) || windowsShaped(p) == (runtime.GOOS == "windows") {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrForeignPath, p)
}

// windowsShaped reports whether an absolute path is spelled the way Windows
// spells one: a drive letter, or the two leading separators of a UNC path.
// Anything else that is absolute is rooted at a single `/`, which is POSIX's
// spelling.
//
// `//server/share` counts because Portable writes a UNC root that way, and a
// record whose UNC sink stopped being recognised as Windows would be refused on
// the only host that can write it. POSIX reaches the same spelling only for a
// path whose leading `//` POSIX itself leaves implementation-defined; refusing
// that one on Linux is the safe direction, and comparablePath has always read
// `\\server\share` as `//server/share` anyway.
func windowsShaped(p string) bool {
	if strings.HasPrefix(p, `\`) || strings.HasPrefix(p, "//") {
		return true
	}
	if len(p) < 2 || p[1] != ':' {
		return false
	}
	c := p[0]
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}
