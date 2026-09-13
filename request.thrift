namespace * abstraction.download.request

encoding json {
  escape = "minimal"
  indent = "2"
  map_keys = "utf8-bytes"
  numbers = "integer-decimal"
  opaque = "verbatim"
  terminator = "newline"
  duplicate_keys = "refuse"
  depth_limit = "64"
}
refusal {
  1: malformed (stage = "grammar")
  2: bad_string (stage = "grammar")
  3: number_spelling (stage = "grammar")
  4: wrong_type (stage = "grammar")
  5: depth_exceeded (stage = "grammar")
  6: duplicate_key (stage = "grammar")
  7: duplicate_field (stage = "structure")
  8: unknown_field (stage = "structure")
  9: missing_field (stage = "structure")
 10: trailing_bytes (stage = "document")
}

// Named roster of downstream recovery guarantees accepted through the job
// submission contract and persisted in the existing string requirement set.
// This promise is independent of survives_process_exit: it recovers acceptance
// after a lost downstream Start reply, under the original request and owner.
const list<string> downstream_recovery_guarantees = ["abstraction.download/recoverable-submission@1"]

struct Artifact {
  1: optional string digest (omit = "zero")
  2: optional i64 size (omit = "zero")
} (unknown_fields = "refuse", doc="Expected content identity and size. Empty digest and zero size mean unknown. A supplied digest is verified before delivery.")

struct Source {
  1: required string scheme
  2: required string locator
} (unknown_fields = "refuse", doc="A location interpreted by its named source scheme. Execution providers declare which schemes they support. Credentials require a separately authorized capability.")

struct Request {
  1: required Artifact artifact
  2: required list<Source> sources
} (document = "true", unknown_fields = "refuse", doc="Service request payload for recoverable job admission with kind download. The provider owns result allocation and all partial files. Request identity, required guarantees and cancellation use the job service. This payload contains no provider paths. The initial execution profile supports anonymous HTTP(S); unsupported work is sealed as definitely not accepted.")
