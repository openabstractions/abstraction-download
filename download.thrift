// What a failed download means, stated once, in a language that is nobody's
// language.
//
// This is the download layer's ONE definition, in the profile described by
// ../idl/LANGUAGE.md. It is the input to ../idl/gen, which emits the encoder
// and the decoder for every language. Nothing here is prose about what a
// refusal MEANS: the semantics live in CONTRACT.md, which an implementer reads
// alongside this.
//
// WHY ONLY THE FAILURE. A download's spec is opaque payload the layer above
// extends, and declaring it was measured against what it would cost:
// the wire's extension point is bare top-level keys, `grant` drops them,
// and a generated encoder would delete an adopter's `group_id` on the first
// read-modify-write. The failure payload is the opposite case in every respect
// — nobody else writes it, it is versioned by the key it is carried under, and
// it is overwritten whole rather than edited — which is why it is the part of
// this layer that a definition can hold and the spec is not.
//
// WHAT THIS FILE CANNOT SAY, and therefore what stays hand-written beside the
// generated code: which of this layer's errors are refusals. That is a property
// of each error where it is DEFINED — see download/CONTRACT.md § Two endings —
// and a list of them here would be the fourth copy of the list that already
// disagreed across two languages.

// The bytes. Every language writes exactly this or it is not an implementation.
//
// A failure is carried inside job.Record.extensions, which is opaque to the job
// layer, so the indentation here is not what a record shows: the record writer
// strips a carried value's whitespace and re-indents it at the depth it lands
// at. The settings are still binding, because the payload is also written and
// read on its own — over a delegation channel, and in the corpus under
// testdata/ — and `newline` is the terminator every line-delimited framing in
// this tree already expects.
encoding json {
  escape         = "minimal"
  indent         = "0"
  map_keys       = "utf8-bytes"
  numbers        = "integer-decimal"
  opaque         = "verbatim"
  terminator     = "newline"
  duplicate_keys = "refuse"
  depth_limit    = "64"
}

// Ten words, not the record's fourteen. There is no timestamp here, so
// bad_timestamp can never be said, and there is no derived declaration, so the
// three derivation words cannot. The rows are in the order two of them are
// chosen between, which is [DEF-R1]'s rule and not a preference.
refusal {
   1: malformed       (stage = "grammar")
   2: bad_string      (stage = "grammar")
   3: number_spelling (stage = "grammar")
   4: wrong_type      (stage = "grammar")
   5: depth_exceeded  (stage = "grammar")
   6: duplicate_key   (stage = "grammar")
   7: duplicate_field (stage = "structure")
   8: unknown_field   (stage = "structure")
   9: missing_field   (stage = "structure")
  10: trailing_bytes  (stage = "document")
}

// The name a record carries this payload under, in `extensions` and therefore
// in `content`.
//
// A list because the profile has no scalar constant ([DEF-A6]); one entry
// because there is one version. An incompatible change to the shape below is a
// SECOND entry and a second key, never an edit to this one — which is the whole
// reason the shape can afford to refuse an unknown field.
const list<string> failure_names = ["abstraction.download/failure@1"]

// Why the last attempt ended, and whether trying again could ever help.
//
// Both, together, because a caller reads them back as one thing. `error` is
// prose and a sentence is not a class: an error rebuilt from it alone is a bare
// string, so every attempt looks retryable however it ended, including the
// refusals this layer declares forever — and the two endings are the whole
// retry model, unobservable in the one place a caller can branch on them.
//
// `permanent` is omitted when false, so a retryable failure is the shorter
// document and the byte a reader is most likely to meet is the absent one.
//
// unknown_fields = "refuse" rather than "grant". `grant` skips and drops, which
// is right where a stranger extends a document we carry; nobody extends this
// one, the key above is its version, and refusing makes an unreadable payload
// behave exactly like an absent payload — a failed job that is still adoptable,
// which is what every reader did before this key existed. See
// CONTRACT.md [DL-F5].
struct Failure {
  1: required string error
  2: optional bool permanent (omit = "zero")
} (document = "true", unknown_fields = "refuse")
