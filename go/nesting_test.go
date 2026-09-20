package download

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func nested(levels int, open, close, core string) []byte {
	return []byte(strings.Repeat(open, levels) + core + strings.Repeat(close, levels))
}

// [DL-S25]: 1000 levels are read, 1001 are refused with the typed refusal, and
// the count is this layer's rather than encoding/json's 10000.
func TestASpecDocumentIsRefusedPastAThousandLevels(t *testing.T) {
	for _, c := range []struct {
		name   string
		doc    []byte
		refuse bool
	}{
		{"1000 arrays around a value", nested(1000, "[", "]", "1"), false},
		{"1001 arrays around a value", nested(1001, "[", "]", "1"), true},
		{"1000 arrays, innermost empty", nested(1000, "[", "]", ""), false},
		{"1001 arrays, innermost empty", nested(1001, "[", "]", ""), true},
		{"1000 objects", nested(1000, `{"n":`, "}", "1"), false},
		{"1001 objects", nested(1001, `{"n":`, "}", "1"), true},
		{"3000 levels encoding/json would read", nested(3000, "[", "]", "1"), true},
		{"brackets inside a string are not levels", []byte(`{"s":"` + strings.Repeat("[", 5000) + `\"["}`), false},
	} {
		err := RefuseDeepNesting(c.doc)
		if c.refuse != (err != nil) {
			t.Errorf("%s: RefuseDeepNesting = %v", c.name, err)
		}
		if err != nil && !errors.Is(err, ErrNestedTooDeep) {
			t.Errorf("%s: refusal %v is not ErrNestedTooDeep", c.name, err)
		}
		if err != nil && !Permanent(err) {
			t.Errorf("%s: a document this deep will never read, so the refusal is permanent", c.name)
		}
		if !c.refuse {
			var v any
			if err := json.Unmarshal(c.doc, &v); err != nil {
				t.Errorf("%s: a document within the limit did not parse: %v", c.name, err)
			}
		}
	}
}
