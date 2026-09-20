package serve

import (
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// LabelSeparator joins the source host and the last path segment of a derived
// download label: a middle dot between spaces.
const LabelSeparator = " · "

var _ acceptanceprovider.LabelDeriver = HTTPExecution{}

// DeriveLabel names a download for a person when its caller supplied no label
// [JOB-A12]: the first source's host and last non-empty path segment, such as
// "huggingface.co · model.safetensors". The scheme, userinfo, port, query and
// fragment are dropped, so a credential carried there never reaches the label.
// A source without a path segment yields the host alone. Another kind, an
// unreadable request or a source without a host yields no label. The label
// names the operation and is never the name of the file a result is bound to.
func (HTTPExecution) DeriveLabel(kind string, spec []byte) string {
	if kind != download.Kind {
		return ""
	}
	input, err := request.Decode(spec)
	if err != nil || len(input.Sources) == 0 {
		return ""
	}
	return sourceLabel(input.Sources[0].Locator)
}

func sourceLabel(locator string) string {
	u, err := url.Parse(locator)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	label := printable(u.Hostname())
	if label == "" {
		return ""
	}
	if segment := lastSegment(u); segment != "" {
		label += LabelSeparator + segment
	}
	return truncate(label, 256)
}

// lastSegment is the last non-empty path segment, decoded when the decoded
// form is printable and left escaped otherwise.
func lastSegment(u *url.URL) string {
	escaped := strings.Split(u.EscapedPath(), "/")
	for i := len(escaped) - 1; i >= 0; i-- {
		if escaped[i] == "" {
			continue
		}
		if decoded, err := url.PathUnescape(escaped[i]); err == nil && printable(decoded) == decoded {
			return decoded
		}
		return printable(escaped[i])
	}
	return ""
}

// printable returns s when it is valid UTF-8 without control characters or
// line and paragraph separators, and the empty string otherwise.
func printable(s string) string {
	if !utf8.ValidString(s) {
		return ""
	}
	for _, r := range s {
		if unicode.IsControl(r) || r == ' ' || r == ' ' {
			return ""
		}
	}
	return s
}

// truncate shortens s to at most limit bytes on a code point boundary.
func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return strings.TrimSpace(s[:cut])
}
