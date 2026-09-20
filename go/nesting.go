package download

import "fmt"

// SpecDepthLimit is the deepest nesting a spec document read on its own may
// carry [DL-S25]: each object and array opened counts one level, the document's
// own value being level 1.
const SpecDepthLimit = 1000

// ErrNestedTooDeep refuses a spec document nested deeper than SpecDepthLimit.
//
// encoding/json stops at its own internal limit of 10000, CPython at whatever
// its recursion limit and version allow, and the C++ parser was capped at 128,
// so one document read three ways and the verdict depended on the toolchain.
// The count is this layer's and is taken before any parser builds a tree.
var ErrNestedTooDeep = forever(fmt.Sprintf("download: spec nested deeper than %d levels", SpecDepthLimit))

// RefuseDeepNesting counts the containers a JSON document opens with constant
// stack and memory, skipping string contents, and refuses the first one that
// would open a level past SpecDepthLimit. Syntax is not judged here: a document
// this passes is still refused by the parser for anything else wrong with it.
func RefuseDeepNesting(b []byte) error {
	depth, inString, escaped := 0, false, false
	for _, c := range b {
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > SpecDepthLimit {
				return ErrNestedTooDeep
			}
		case '}', ']':
			if depth > 0 {
				depth--
			}
		}
	}
	return nil
}
