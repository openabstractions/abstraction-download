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
		if httpDate(lm) {
			v.LastModified = lm
		}
	}
	return v
}

// httpDate reports whether s is one of the three spellings of an HTTP-date RFC
// 9110 requires a recipient to accept: IMF-fixdate, which is the only one a
// sender may use, and the two obsolete forms a recipient still meets.
//
// This recognises a shape and does not parse a time, because the value is never
// interpreted: it is echoed back verbatim as If-Range, which the origin server
// evaluates by exact match. Written out rather than handed to http.ParseTime
// because the three languages' date parsers accept three different sets — this
// one took a three-letter timezone where the grammar says GMT, C++ took only
// IMF-fixdate, Python took RFC 2822 — so "an HTTP date" implemented as
// "whatever the standard library takes" is three rules wearing one name.
func httpDate(s string) bool {
	if shaped(s, "aaa, 99 aaa 9999 99:99:99 GMT") {
		return monthAt(s, 8)
	}
	if shaped(s, "aaa aaa #9 99:99:99 9999") {
		return monthAt(s, 4)
	}
	day, tail, ok := strings.Cut(s, ", ")
	if !ok || day == "" {
		return false
	}
	return shaped(day, strings.Repeat("a", len(day))) &&
		shaped(tail, "99-aaa-99 99:99:99 GMT") && monthAt(tail, 3)
}

// shaped matches a fixed layout: `9` a digit, `a` a letter, `#` either a digit
// or the space asctime pads a one-digit day with, anything else itself.
func shaped(s, pattern string) bool {
	if len(s) != len(pattern) {
		return false
	}
	for i := 0; i < len(s); i++ {
		digit := s[i] >= '0' && s[i] <= '9'
		letter := (s[i]|0x20) >= 'a' && (s[i]|0x20) <= 'z'
		switch pattern[i] {
		case '9':
			if !digit {
				return false
			}
		case 'a':
			if !letter {
				return false
			}
		case '#':
			if !digit && s[i] != ' ' {
				return false
			}
		default:
			if s[i] != pattern[i] {
				return false
			}
		}
	}
	return true
}

func monthAt(s string, at int) bool {
	i := strings.Index("JanFebMarAprMayJunJulAugSepOctNovDec", s[at:at+3])
	return i >= 0 && i%3 == 0
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

// answersFrom reports whether a 206 is an answer to the request that was sent:
// a single range, beginning at the offset the next byte will be written at.
//
// Both ways of failing put real artifact bytes at a place nobody asked about,
// and neither is visible to a length check or a transport error. A
// `multipart/byteranges` body is worse than misplaced — its MIME boundary and
// per-part headers are content this layer would author into the artifact
// itself. RFC 9110 allows a server to answer a single range that way and a
// coalescing proxy does; we never send a multi-range request, so it is never an
// answer to ours.
func answersFrom(resp *http.Response, from int64) error {
	if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(strings.ToLower(strings.TrimSpace(ct)), "multipart/") {
		return fmt.Errorf("download: one range was answered with %s", ct)
	}
	start, err := contentRangeStart(resp.Header.Get("Content-Range"))
	if err != nil {
		return err
	}
	if start != from {
		return fmt.Errorf("download: asked for bytes from %d, got a range starting at %d", from, start)
	}
	return nil
}

// unwantedCoding names a content coding the request did not ask for, or "".
//
// Every request this layer sends carries `Accept-Encoding: identity`, so any
// other coding coming back is the server overriding what was asked. It matters
// because a digest is over the artifact and a coding changes what "the bytes"
// are: one transport decoded and completed, one hashed the envelope, one failed
// a third way, and all three were reading the same legal response.
func unwantedCoding(h http.Header) string {
	v := strings.TrimSpace(h.Get("Content-Encoding"))
	if v == "" || strings.EqualFold(v, "identity") {
		return ""
	}
	return v
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
	byteRange, _, ok := strings.Cut(strings.TrimSpace(rest), "/")
	if !ok {
		return 0, fmt.Errorf("download: Content-Range %q has no total", s)
	}
	first, _, ok := strings.Cut(byteRange, "-")
	if !ok {
		return 0, fmt.Errorf("download: Content-Range %q has no range", s)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(first), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("download: Content-Range %q does not start at a byte position", s)
	}
	return n, nil
}

// contentRangeTotal reads the artifact's full length out of a `Content-Range` —
// `bytes 1000-1999/40960` gives 40960 — and 0 when the server wrote `*` for it,
// which it is entitled to do, or when the header is not one this code
// understands.
//
// It exists because a BOUNDED range request broke the only other way of
// learning the size. Content-Length on a 206 is the length of what was sent, so
// adding the start offset back gives the whole artifact for an open range and
// the end of the gap for a bounded one — and the second number would go
// straight into Progress.Total and be shown to a person as the size of their
// download. The total after the slash is the one that is right either way.
//
// Never an error: a missing or unparseable total is a server declining to say,
// which is exactly what 0 already means everywhere else in this layer.
func contentRangeTotal(s string) int64 {
	_, rest, ok := strings.Cut(strings.TrimSpace(s), " ")
	if !ok {
		return 0
	}
	_, total, ok := strings.Cut(strings.TrimSpace(rest), "/")
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(total), 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}
