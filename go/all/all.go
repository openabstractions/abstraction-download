// Package all links every download tier into a program.
//
//	import _ "github.com/openabstractions/abstraction-download/go/all"
//
// That single line is what an application adds, and then download.Discover finds
// whatever this machine actually has. It is the classpath, spelled the way a
// compiled language spells it for code that runs in this address space.
//
// It is not the only way a tier arrives. A Delegator hands the whole job away
// and carries no byte through us, so it can be a program launched over a pipe
// rather than a package linked in — see download.Tier.
//
// Importing individual bindings instead is fine and sometimes better — a program
// that must never touch a network share can link nas out entirely, and then no
// configuration file can turn it on.
package all

import (
	_ "github.com/openabstractions/abstraction-download/go/bits"
	_ "github.com/openabstractions/abstraction-download/go/nas"
)
