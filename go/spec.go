package download

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	job "github.com/openabstractions/abstraction-job/go"
)

// Kind is the job.Record.Kind this package understands. A process that finds a
// job of an unknown kind leaves it alone rather than guessing at its spec.
const Kind = "download"

// Spec is what a download job is: an artifact, the places it can be had, and
// where it goes. It lives inside job.Record.Spec, opaque to the job layer.
//
// That indirection is the point. An earlier version had these three fields in
// the job record itself, which meant every improvement to downloading — mirrors,
// chunk manifests, webseeds — forced a schema change on a record that Go, Python
// and eventually C++ all have to agree about. Now this struct can grow and the
// job layer never notices.
type Spec struct {
	Artifact Artifact `json:"artifact"`
	Sources  []Source `json:"sources"`
	Sink     Sink     `json:"sink"`
}

// Artifact is what the job is for: an identity, and how big it is. Both may be
// unknown at submission — some sources only reveal length in a response header,
// and some callers genuinely do not know the digest in advance.
//
// Digest is an INPUT, not a result. Every transfer tool in the survey either
// verifies nothing (curl, SMB), verifies only size and timestamp (BITS), or
// verifies only what it is told to. If the caller knows what the bytes should
// be, this layer must be able to refuse anything else.
type Artifact struct {
	Digest string `json:"digest,omitempty"` // "sha256:<hex>"
	Size   int64  `json:"size,omitempty"`   // 0 means unknown
}

// Source is somewhere the bytes can be obtained from. It is deliberately NOT a
// URL.
//
// Two systems designed independently for exactly this payload class —
// BitTorrent v2 and HuggingFace Xet — both describe a transfer as a content
// identity plus a list of places it can be had, never as a URL. A URL is one
// kind of source. An SMB share, a peer, a local file and another machine's store
// are others, and none of them are expressible as an http URL without lying.
// Lock the descriptor to a URL and delegation, multi-source, mirrors and dedup
// across model revisions all become impossible to add later.
type Source struct {
	Scheme  string `json:"scheme"`
	Locator string `json:"locator"`

	// Attrs describe the source to THIS LAYER and never reach the wire.
	//
	// They used to do both. Anything here that no switch statement recognised
	// was copied into the outgoing request as a header, so `boundaries` — a
	// range table read off a reconstruction manifest — was one forgotten case
	// away from being sent to a CDN, and every attribute anyone added after it
	// would have leaked by default. Four special cases and no rule.
	//
	// Now the rule is the shape: an attribute cannot become a header, because
	// nothing reads this map when a request is built. Forgetting is no longer
	// expressible.
	Attrs map[string]string `json:"attrs,omitempty"`

	// Headers are what the caller intends to send, named one at a time.
	//
	// Deliberately not a place for a secret. A credential is named in Attrs and
	// resolved on this machine at the moment of the request, so it is never in
	// the record — see Credentials. Validate refuses the header names a secret
	// is conventionally spelled with, which closes the common mistake and
	// cannot close them all; the rule that carries the weight is still that a
	// record stores a reference, never a value.
	//
	// That constraint is what makes this field ADVISORY in the sense job.Record
	// means it: an older reader that has never heard of this key drops it, and
	// what it drops is decoration rather than the thing that authorises the
	// request. A header whose absence would break the transfer is a credential,
	// and a credential's absence is already a refusal rather than a silent
	// anonymous fetch.
	Headers map[string]string `json:"headers,omitempty"`

	Priority int `json:"priority,omitempty"`
}

// resolvedHeaders are the header names this layer decides for itself, which a
// record therefore may not decide for it.
//
// Authorization and its proxy twin are where Credentials puts a resolved
// secret; Cookie is the other place one is conventionally spelled. Range is
// refused for a different reason: the bounds of a transfer are what the resume
// logic and the range planner agree about, and a record that sets them tells the
// server one thing and this layer another.
var resolvedHeaders = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"cookie":              true,
	"range":               true,
}

// Sink is where the bytes land, declared at submission and never returned.
//
// 40 GB does not fit in a []byte, and the process doing the writing may not be
// ours at all — BITS writes under its own service account and hands the file
// over on completion. So the destination is part of the description of the work.
type Sink struct {
	// Partial is written to while the job runs. It belongs to the lease holder.
	Partial string `json:"partial"`
	// Final is where the bytes are moved on delivery.
	Final string `json:"final"`
}

// Checkpoint is what a successor needs in order to continue, stored in
// job.Record.Checkpoint.
//
// Verified is every byte of Partial known to be the artifact's real bytes, as a
// set of half-open ranges. A new owner may resume ONLY from what is in there —
// anything outside it was written by an owner that has since vanished, and
// nothing vouches for it. There is no field meaning "trust the part I did not
// check", so curl's `-C -` mistake is not expressible.
//
// VerifiedPrefix is the same claim about the leading bytes, and it is the field
// that was here first. It stays because it is what a reader that has never
// heard of ranges resumes from: such a reader re-fetches the scattered parts
// and is correct, only slower. The two are never checked against each other —
// see Checkpoint.Proven — because each is a claim that bytes ARE proven and
// neither is a claim that other bytes are not.
//
// Validators say which VERSION of the artifact those bytes came from, so a
// successor can ask the source to continue that version rather than whatever it
// is serving today. A reader that does not know the field resumes without one,
// which is safe rather than merely tolerable: a source that has changed then
// answers a ranged request with the whole new file, and that answer is handled
// by starting again from byte zero. See validators.go and HTTP.Fetch.
type Checkpoint struct {
	VerifiedPrefix int64      `json:"verified_prefix"`
	Validators     Validators `json:"validators,omitempty"`

	// Not a JSON field of this struct, deliberately. The record's encoding of a
	// range set is the job layer's — three languages compare those bytes — so
	// it is written by job.CheckpointWithRanges and read by
	// job.RangesFromCheckpoint, never by struct tags here. Marshalling this
	// struct with a `verified` tag would give a second, wrong spelling of a
	// state that is only allowed one. See ranges.go, CheckpointOf and
	// setCheckpoint.
	Verified Ranges `json:"-"`
}

// MarshalJSON refuses to write a checkpoint that carries ranges.
//
// Verified is not a field of this struct's JSON, so marshalling one drops it
// without a word — and rec.SetCheckpoint(Checkpoint{Verified: ...}) is the
// obvious call for anybody who has just read the struct. It has already been
// written, in a test, and the ranges silently vanished. There is no way to make
// that call correct without giving the record a second spelling of a state
// allowed only one, so it is made loud instead: the right call is
// setCheckpoint, which knows where the job layer keeps a range set.
func (c Checkpoint) MarshalJSON() ([]byte, error) {
	if len(c.Verified) > 0 {
		return nil, errors.New("download: a checkpoint carrying ranges must be written through setCheckpoint")
	}
	type plain Checkpoint
	return json.Marshal(plain(c))
}

// Validate checks a spec that belongs to no job yet. Every path in the store's
// work area is reserved against it, because nothing owns one until an id
// exists. See ValidateFor.
func (s Spec) Validate() error { return s.ValidateFor("") }

// ValidateFor checks a spec on behalf of the job that will carry it. The id is
// needed because one relative path in the store — work/<id> — is reserved
// against every job except the one it belongs to.
func (s Spec) ValidateFor(owner string) error {
	if len(s.Sources) == 0 {
		return fmt.Errorf("download: at least one source is required")
	}
	for i, src := range s.Sources {
		if strings.TrimSpace(src.Scheme) == "" {
			return fmt.Errorf("download: source %d: scheme is required", i)
		}
		if strings.TrimSpace(src.Locator) == "" {
			return fmt.Errorf("download: source %d: locator is required", i)
		}
		if err := validHeaders(src); err != nil {
			return fmt.Errorf("download: source %d: %w", i, err)
		}
	}
	if strings.TrimSpace(s.Sink.Final) == "" {
		return fmt.Errorf("download: sink final path is required")
	}
	// Final first, then partial, so a record with both wrong names the same
	// field in every implementation.
	for _, p := range []string{s.Sink.Final, s.Sink.Partial} {
		if err := EscapesRoot(p); err != nil {
			return err
		}
		if err := ReservedSink(owner, p); err != nil {
			return err
		}
	}
	if s.Artifact.Digest != "" && !strings.HasPrefix(s.Artifact.Digest, "sha256:") {
		return fmt.Errorf("download: digest %q is not sha256:<hex>", s.Artifact.Digest)
	}
	return nil
}

func validHeaders(src Source) error {
	for name := range src.Headers {
		lower := strings.ToLower(strings.TrimSpace(name))
		if lower == "" {
			return fmt.Errorf("a header needs a name")
		}
		if resolvedHeaders[lower] || lower == strings.ToLower(src.Attrs[CredentialHeaderAttr]) {
			return fmt.Errorf("header %q is resolved on the machine that fetches, so a record may not carry it", name)
		}
	}
	return nil
}

// Submit creates a download job. It names the partial file when the caller does
// not, so a successor can find what a predecessor left without the two of them
// agreeing on a convention privately. See PartialFor.
func Submit(store job.Store, spec Spec, requires ...string) (string, error) {
	return submitAs(store, job.NewID(), spec, requires...)
}

// submitAs is Submit under an id the caller chose, which is how a record keyed
// to its destination gets created: the create either wins or tells the caller
// somebody else already holds that destination. See client.claimDestination.
func submitAs(store job.Store, id string, spec Spec, requires ...string) (string, error) {
	if err := spec.ValidateFor(id); err != nil {
		return "", err
	}
	// Slashes, always, for the paths that are relative. filepath.Join on Windows
	// produces `models\x.gguf`, and on Linux that is not a directory and a file
	// — it is ONE file whose name contains a backslash. The job would "succeed"
	// and put the weights somewhere nobody would ever look. A record is read by
	// machines that do not share this one's separator, so the separator is part
	// of the contract, not a local detail.
	spec.Sink.Partial = Portable(spec.Sink.Partial)
	spec.Sink.Final = Portable(spec.Sink.Final)
	if spec.Sink.Partial == "" {
		spec.Sink.Partial = PartialFor(spec.Sink.Final, id)
	}
	rec := job.Record{ID: id, Kind: Kind, Requires: requires}
	rec.Progress.Total = spec.Artifact.Size
	if err := rec.SetSpec(spec); err != nil {
		return "", err
	}
	return store.Submit(rec)
}

// SpecOf reads the download spec out of a record, refusing any job that is not
// a download.
func SpecOf(rec *job.Record) (Spec, error) {
	var s Spec
	if rec.Kind != Kind {
		return s, fmt.Errorf("download: job %s is kind %q, not %q", rec.ID, rec.Kind, Kind)
	}
	if err := rec.DecodeSpec(&s); err != nil {
		return s, err
	}
	return s, nil
}

// CheckpointOf reads the resume point. A job that has never checkpointed
// returns a zero Checkpoint, which correctly means "start at the beginning".
func CheckpointOf(rec *job.Record) (Checkpoint, error) {
	var c Checkpoint
	if err := rec.DecodeCheckpoint(&c); err != nil {
		return c, err
	}
	rs, err := rec.CheckpointRanges()
	if err != nil {
		return c, err
	}
	c.Verified = rs
	c.VerifiedPrefix = rs.VerifiedPrefix()
	return c, nil
}

// Resolve turns the sink's paths into paths on THIS machine.
//
// A job record is a file that another machine reads. The moment the store lives
// on a share — a NAS mount, an SMB path — an absolute sink is a lie: the PC
// wrote `\\nas\models\store\work\abc`, and the container that adopts the job sees
// that same directory as `/store/work/abc`. Neither string is wrong; they are
// two views of one directory, and a record that hard-codes either one only works
// on the machine that wrote it.
//
// So a relative path means "under the store root", and each machine resolves it
// against its own view. This is the same move a container image makes: the
// contents are portable because nothing inside names the host's mount point.
// Absolute paths are left alone — a caller who names `D:\models\x.gguf` means
// that drive, and it is not this function's business to second-guess them.
//
// "Under the store root" is enforced, not assumed, and so are the two things
// containment alone does not cover: a contained path that names the store's own
// files (ErrReservedPath) and an absolute path written in the other platform's
// convention (ErrForeignPath). owner is the job the sink belongs to, because
// work/<owner> is the one reserved path that is this job's to use.
func (s Sink) Resolve(root, owner string) (partial, final string, err error) {
	if partial, err = resolveSink(root, owner, s.Partial); err != nil {
		return "", "", err
	}
	if final, err = resolveSink(root, owner, s.Final); err != nil {
		return "", "", err
	}
	return partial, final, nil
}

// resolveSink is resolveUnder plus the two refusals that need to know which
// machine is asking and which job is asking. Kept apart from resolveUnder
// because EscapesRoot answers about a RECORD, which has neither.
func resolveSink(root, owner, p string) (string, error) {
	resolved, err := resolveUnder(root, p)
	if err != nil {
		return "", err
	}
	if err := ForeignPath(p); err != nil {
		return "", err
	}
	if err := ReservedSink(owner, p); err != nil {
		return "", err
	}
	return resolved, nil
}

// ErrEscapesRoot is a relative sink path that resolves somewhere other than
// under the store root.
//
// This is the one place in the layer where the writer's authority and the
// caller's choice come apart. A PC submits the record; a NAS adopts it and does
// the writing, with the NAS's account, to a destination the PC named. Until this
// check existed `filepath.Join` cleaned the `..` away and said nothing, so
// `../../../Users/victim/.ssh/authorized_keys` in a record resolved to exactly
// that file — as did an autostart directory. A confused deputy, reachable by
// anyone who can put a record in a shared store.
//
// Refused, never clamped. Clamping would write 40 GB somewhere the caller did
// not ask for and report success, which is the failure this layer exists to
// refuse, and the caller would never learn its record was wrong.
var ErrEscapesRoot = forever("download: sink path escapes the store root")

func resolveUnder(root, p string) (string, error) {
	if p == "" || !relativeEverywhere(p) {
		return p, nil
	}
	// Forgive a backslash in a RELATIVE path, because records written before
	// Submit normalised them are on disk already, and because a backslash cannot
	// legally appear in a Windows filename anyway — so reading it as a separator
	// is the only interpretation that is ever right.
	resolved := filepath.Join(root, filepath.FromSlash(strings.ReplaceAll(p, `\`, "/")))
	if !under(root, resolved) {
		return "", fmt.Errorf("%w: %s", ErrEscapesRoot, p)
	}
	return resolved, nil
}

// probeRoot is a stand-in store root, used to answer the containment question
// about a path that has no root to hand — a record being read rather than run.
// One segment deep, because containment is measured from the store root and a
// deeper stand-in would absorb a `..` that a real root would not.
const probeRoot = "/probe"

// EscapesRoot reports whether a relative sink path would resolve outside the
// store root — whichever root it is resolved against.
//
// Root-independent, and that is a fact about the question rather than a
// shortcut: containment is measured from the store root, so one `..` climbs out
// of it no matter how deep the root sits on any particular machine. That is what
// lets a reader answer this about a RECORD, which deliberately names no root.
//
// Absolute paths answer nil. They are never joined onto the root, so they cannot
// escape it in this sense; what a machine adopting a record should do with one
// is a separate question and is not decided here.
func EscapesRoot(p string) error {
	_, err := resolveUnder(probeRoot, p)
	return err
}

// under reports whether resolved names a location inside root.
//
// Asked of the RESULT of the join, never of the input. Scanning the input for
// ".." is the check everybody writes and it is defeated by a path that spells
// the climb some other way, and it fires on paths like `a/../b` that are
// perfectly contained. Resolve first, then ask where the answer landed.
//
// Every shortcut in the comparison itself is wrong somewhere, so none is taken:
//
//   - `C:\store2` starts with `C:\store` and is a different directory, so the
//     prefix has to end on a separator boundary.
//   - Windows ignores case in a path and POSIX does not, so folding is
//     conditional on the OS rather than done to be safe.
//   - `\\nas\share\store`, `C:\`, `/` and a trailing separator must not change
//     the answer, so both sides are reduced to one spelling first.
//
// What this does NOT close: a directory inside the root that is itself a
// symlink or a junction pointing out of it. `models/x.gguf` is then contained
// by every lexical measure and the bytes still land elsewhere. Closing it needs
// the path resolved at the moment of the write — the file does not exist yet
// when this runs, and resolving it here would only move the race earlier — and
// none of Go, Python and C++ has a portable "open without following a link".
// This is lexical containment and claims nothing more.
func under(root, resolved string) bool {
	r := comparablePath(root)
	c := comparablePath(resolved)
	if r == "" || r == "." {
		// The store's binding is not a filesystem, so there is no root to be
		// inside — localRoot answers "" for exactly that case. All that can be
		// said is that the path did not climb out of wherever it eventually
		// gets resolved, which the cleaning has already made visible.
		return c != ".." && !strings.HasPrefix(c, "../")
	}
	if c == r {
		return true
	}
	if !strings.HasSuffix(r, "/") {
		r += "/"
	}
	return strings.HasPrefix(c, r)
}

// comparablePath reduces a path to the one spelling in which two of them can be
// compared: cleaned, forward slashes, no trailing separator, and case-folded
// only where the filesystem itself ignores case.
func comparablePath(p string) string {
	if p == "" {
		return ""
	}
	// The UNC root is held back over the clean. filepath.Clean collapses a
	// leading `//` to `/` on POSIX and keeps it on Windows, so a record naming
	// `//nas/share/x` compared differently depending on which machine asked —
	// and Python's normpath and the C++ cleaner both keep it.
	s := strings.ReplaceAll(p, `\`, "/")
	unc := ""
	if strings.HasPrefix(s, "//") && !strings.HasPrefix(s, "///") {
		unc, s = "/", s[1:]
	}
	s = unc + strings.ReplaceAll(filepath.Clean(s), `\`, "/")
	for len(s) > 1 && strings.HasSuffix(s, "/") {
		s = s[:len(s)-1]
	}
	if runtime.GOOS == "windows" {
		s = strings.ToLower(s)
	}
	return s
}

// relativeEverywhere reports whether p is relative under BOTH conventions.
//
// filepath.IsAbs alone is not enough, because it answers for the OS running it:
// on Linux `D:\models\x.gguf` is "relative", and joining it onto the store root
// would silently produce a directory literally named `D:\models` on the NAS.
// A path that is absolute anywhere is treated as absolute everywhere, so a
// mistake surfaces as a plain "no such file" rather than as a strange one.
func relativeEverywhere(p string) bool {
	if strings.HasPrefix(p, "/") || strings.HasPrefix(p, `\`) {
		return false // POSIX absolute, or a UNC path
	}
	if len(p) >= 2 && p[1] == ':' {
		c := p[0]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			return false // Windows drive letter
		}
	}
	return !filepath.IsAbs(p)
}

// PartialFor names the file the bytes accumulate in before they earn the final
// name, for a caller that did not choose one. It is a wire-visible choice: the
// name goes into the record, and a successor in another language finds what a
// predecessor left by reading it. scripts/spec-conformance.sh compares the three
// implementations' answers.
//
// Two cases, because "beside the store" and "beside the artifact" are both
// right and neither is right for the other one.
//
// A RELATIVE final resolves under the store root on whichever machine picks the
// job up, so the partial goes in the store's own work directory. Baking this
// machine's answer into the record is what would stop a NAS from adopting it.
//
// An ABSOLUTE final names one machine's filesystem already, and the store may be
// on a different volume — a model going to D:\ with the store on C:. Delivery
// across volumes cannot be a rename, so it degrades to a copy INTO THE FINAL
// NAME, and a crash halfway through leaves a truncated file under the name an
// application reads as an installed model. That is the exact failure this layer
// exists to refuse. Beside the artifact, delivery is a rename on one filesystem
// and the final name does not exist until the bytes are all there.
func PartialFor(final, id string) string {
	if relativeEverywhere(final) {
		return "work/" + id
	}
	return final + ".part"
}

// Portable puts a path into the one spelling a record uses: `/` is the only
// separator, everywhere, whatever wrote it.
//
// Absolute paths used to be exempt, on the argument that they already name one
// machine and respelling them buys nothing. What that missed is that the
// separator then records WHICH machine wrote the record — a job delegated to the
// NAS came back spelled `C:/Users/...` and the same file fetched here was
// `C:\Users\...` — and that two spellings of one destination do not compare
// equal, so "are we already fetching this?" answers no and the artifact is
// fetched twice. An adopter joining a native directory to a file name with a
// hardcoded `/` produced a path that changed convention halfway through, and
// nothing refused it.
//
// Nothing is lost by the rewrite: a drive letter and a UNC root still say
// Windows afterwards — windowsShaped reads `//server/share` as UNC for exactly
// this reason — and Windows accepts either separator in every path it is given.
//
// A POSIX-rooted path is returned untouched, because there a backslash is a
// legal character in a file's name and rewriting it would name a different file.
func Portable(p string) string {
	if strings.HasPrefix(p, "/") {
		return p
	}
	return strings.ReplaceAll(p, `\`, "/")
}

// sameDigest compares two sha256 digests written by different implementations.
//
// The record says "sha256:<hex>" and Validate enforces it at submission, but an
// application writing records through its own implementation of the store never
// reaches that check — and the job layer will not look inside a spec to enforce
// it, deliberately. So a reader has to be liberal about a label it can infer,
// while staying exact about the part that carries the meaning.
func sameDigest(a, b string) bool {
	return bareHex(a) != "" && bareHex(a) == bareHex(b)
}

func bareHex(d string) string {
	s := strings.ToLower(strings.TrimSpace(d))
	s = strings.TrimPrefix(s, "sha256:")
	s = strings.TrimPrefix(s, "sha256-") // how Ollama names its blobs
	if len(s) != 64 {
		return ""
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ""
		}
	}
	return s
}

// NormalDigest is a digest reduced to the thing that carries the meaning.
//
// Exported because conformance across implementations is about meaning, not
// spelling: one may write "sha256:<hex>" and another the bare hex, and the
// contract is that they name the same artifact. Comparing the spelling instead
// is what deleted a correct 1.5 GB download.
//
// An unrecognised digest returns empty rather than itself, so a comparison
// cannot accidentally succeed on two things neither implementation understood.
func NormalDigest(d string) string {
	if h := bareHex(d); h != "" {
		return "sha256:" + h
	}
	return ""
}
