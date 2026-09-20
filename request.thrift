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
 10: bad_enum (stage = "structure")
 11: trailing_bytes (stage = "document")
}

// Named roster of downstream recovery guarantees accepted through the job
// submission contract and persisted in the existing string requirement set.
// This promise is independent of survives_process_exit: it recovers acceptance
// after a lost downstream Start reply, under the original request and owner.
const list<string> downstream_recovery_guarantees = ["abstraction.download/recoverable-submission@1"]
// Execution guarantee a request naming a credential in any source requires. A
// provider with no credentials applier refuses it at resolution as unmet.
const list<string> credential_guarantees = ["abstraction.download/credentials@1"]
// Execution guarantee a request with network unmetered requires. A provider
// with no network cost source on its platform refuses it at resolution as
// unmet. See CONTRACT.md DL-N2.
const list<string> network_cost_guarantees = ["abstraction.download/network-cost@1"]
// The job record extension key a waiting attempt carries: a JSON string of
// one word, <constraint>:<state>, such as network:metered. See DL-N5.
const list<string> waiting_extensions = ["abstraction.download/waiting@1"]

enum Network {
  1: any
  2: unmetered
} (unknown = "refuse", reader = "act")

struct Constraints {
  1: optional Network network (omit = "zero")
} (unknown_fields = "refuse", doc="When the executing service may move the bytes. network unmetered opens sources only while the platform reports the path unmetered and requires abstraction.download/network-cost@1; the attempt waits, holding no lease, while the path is metered and resumes with Range. Empty network and any are the same. Constraints are evaluated where the bytes move.")

struct Artifact {
  1: optional string digest (omit = "zero")
  2: optional i64 size (omit = "zero")
} (unknown_fields = "refuse", doc="Expected content identity and size. Empty digest and zero size mean unknown. A supplied digest is verified before delivery.")

struct Source {
  1: required string scheme
  2: required string locator
  3: optional string credential (omit = "zero")
} (unknown_fields = "refuse", doc="A location interpreted by its named source scheme. Execution providers declare which schemes they support. credential, when present, names a credential registered with abstraction.credentials/holder@1 in the submitting caller's account; it is a name, never a secret. The executing service applies it through abstraction.credentials/applier@1 with consumer abstraction.download/http-execution@1 to each request it sends to this source, including redirects and resumed ranges, and never records the applied headers. A request naming a credential requires abstraction.download/credentials@1.")

struct Request {
  1: required Artifact artifact
  2: required list<Source> sources
  3: optional Constraints constraints (omit = "absent")
} (document = "true", unknown_fields = "refuse", doc="Service request payload for recoverable job admission with kind download. The provider owns result allocation and all partial files. Request identity, required guarantees and cancellation use the job service. This payload contains no provider paths. The initial execution profile supports HTTP(S), anonymous or with a named credential; unsupported work is sealed as definitely not accepted.")
