// Package all links every download tier into a program.
//
//	import _ "github.com/openabstractions/abstraction-download/go/all"
//
// That single line is what an application adds, and then download.Discover finds
// whatever this machine actually has. It is the classpath, spelled the way a
// compiled language has to spell it: "present" must mean "linked in", because
// portable plugin loading does not exist.
//
// Importing individual bindings instead is fine and sometimes better — a program
// that must never touch a network share can link nas out entirely, and then no
// configuration file can turn it on.
package all

import (
	_ "github.com/openabstractions/abstraction-download/go/bits"
	_ "github.com/openabstractions/abstraction-download/go/nas"
)
