package download

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// This file is about one question: are the bytes a server is offering now the
// same artifact as the bytes already on disk?
//
// A resumed transfer asks a server to continue a file. Nothing in a bare `Range`
// request says WHICH file, so a server that has since replaced the artifact
// answers the range honestly and the answer is a valid range of something else.
// Appended to the prefix on disk that produces a file of exactly the right
// length holding two versions spliced at an arbitrary offset — no transport
// error, no short read, and nothing for a digest-less download to catch.
//
// The remedy is the one HTTP already has: send with the range a token
// identifying the version the prefix came from, and let the server decide. See
// Validators.IfRange and, for what to do when the server says no, HTTP.Fetch.

// Validators identify the VERSION of an artifact a server served, so that a
// later request can say which version its bytes on disk came from.
//
// Only strong validators are ever put in here; see StrongValidators.
type Validators struct {
	// ETag is the entity tag exactly as the server wrote it, quotes included,
	// and never a weak one.
	ETag string `json:"etag,omitempty"`
	// LastModified is the Last-Modified header, used only when there is no
	// usable ETag.
	LastModified string `json:"last_modified,omitempty"`
}

// Empty reports that nothing here identifies a version.
func (v Validators) Empty() bool { return v.ETag == "" && v.LastModified == "" }

// IfRange is the value to send as `If-Range` alongside a `Range` request, or ""
// when there is nothing worth sending.
//
// If-Range rather than If-Match, which is the same choice Chromium made and for
// the same reason: a failed If-Match is a 412 with an empty body, so the client
// learns the file changed and must then spend a second round trip asking for it.
// A failed If-Range is the new file, in the same response.
func (v Validators) IfRange() string {
	if v.ETag != "" {
		return v.ETag
	}
	return v.LastModified
}

// StrongValidators reads the validators out of a response, keeping only the
// ones worth acting on.
//
// A weak ETag — `W/"..."` — is dropped rather than recorded. Weak means the
// server is asserting semantic equivalence, not byte equality: two responses
// may share a weak tag and differ in their bytes, which is precisely the
// distinction a resume depends on. Recording one would produce a token that
// makes a server answer 206 for a file whose bytes moved, which is worse than
// having no validator at all, because no validator at least leaves the 200
// path (and its restart) available.
//
// Last-Modified is used only when there is no usable ETag, and only when it
// parses as an HTTP date, so a malformed header is not echoed back to a server
// that would then have to guess what it meant.
func StrongValidators(h http.Header) Validators {
	var v Validators
	etag := strings.TrimSpace(h.Get("ETag"))
	if strongETag(etag) {
		v.ETag = etag
	}
	if v.ETag == "" {
		lm := strings.TrimSpace(h.Get("Last-Modified"))
		if _, err := http.ParseTime(lm); err == nil {
			v.LastModified = lm
		}
	}
	return v
}

// strongETag reports whether s is an entity tag this layer will act on: quoted,
// non-empty, and not marked weak.
func strongETag(s string) bool {
	if s == "" {
		return false
	}
	// Case-insensitive because RFC 7232 writes the marker as `W/` and servers
	// have been seen to write `w/`.
	if len(s) >= 2 && strings.EqualFold(s[:2], "w/") {
		return false
	}
	return len(s) >= 2 && strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)
}

// contentRangeStart reads the first byte position out of a `Content-Range`
// header — `bytes 1000-40959/40960` gives 1000.
//
// A 206 whose range does not begin where the client asked is not a partial
// answer to this request; it is a different answer altogether, and the bytes
// after it belong at an offset nobody asked about. Reading the header is the
// only way to notice, because the body itself looks exactly like a correct one.
func contentRangeStart(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("download: a 206 arrived with no Content-Range")
	}
	unit, rest, ok := strings.Cut(s, " ")
	if !ok || !strings.EqualFold(unit, "bytes") {
		return 0, fmt.Errorf("download: Content-Range %q is not in bytes", s)
	}
	span, _, ok := strings.Cut(strings.TrimSpace(rest), "/")
	if !ok {
		return 0, fmt.Errorf("download: Content-Range %q has no total", s)
	}
	first, _, ok := strings.Cut(span, "-")
	if !ok {
		return 0, fmt.Errorf("download: Content-Range %q has no range", s)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(first), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("download: Content-Range %q does not start at a byte position", s)
	}
	return n, nil
}
