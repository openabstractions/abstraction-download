package download

import (
	"fmt"
	"path/filepath"
	"strings"

	job "github.com/ReinisLusis/abstraction-job"
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
	Scheme   string            `json:"scheme"`
	Locator  string            `json:"locator"`
	Attrs    map[string]string `json:"attrs,omitempty"`
	Priority int               `json:"priority,omitempty"`
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
// VerifiedPrefix is how many leading bytes of Partial are known to be the
// artifact's real bytes. A new owner may resume ONLY from there — anything past
// it was written by an owner that has since vanished, and nothing vouches for
// it. There is no field meaning "trust the part I did not check", so curl's
// `-C -` mistake is not expressible.
type Checkpoint struct {
	VerifiedPrefix int64 `json:"verified_prefix"`
}

func (s Spec) Validate() error {
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
	}
	if strings.TrimSpace(s.Sink.Final) == "" {
		return fmt.Errorf("download: sink final path is required")
	}
	if s.Artifact.Digest != "" && !strings.HasPrefix(s.Artifact.Digest, "sha256:") {
		return fmt.Errorf("download: digest %q is not sha256:<hex>", s.Artifact.Digest)
	}
	return nil
}

// Submit creates a download job. It fills in a partial path under the store's
// own work directory when the caller does not choose one, so a successor can
// find what a predecessor left without either of them agreeing on a convention.
func Submit(store job.Store, spec Spec, requires ...string) (string, error) {
	if err := spec.Validate(); err != nil {
		return "", err
	}
	id := job.NewID()
	// Slashes, always, for the paths that are relative. filepath.Join on Windows
	// produces `models\x.gguf`, and on Linux that is not a directory and a file
	// — it is ONE file whose name contains a backslash. The job would "succeed"
	// and put the weights somewhere nobody would ever look. A record is read by
	// machines that do not share this one's separator, so the separator is part
	// of the contract, not a local detail.
	spec.Sink.Partial = Portable(spec.Sink.Partial)
	spec.Sink.Final = Portable(spec.Sink.Final)
	if spec.Sink.Partial == "" {
		// Relative, deliberately. The store knows where its work directory is on
		// whichever machine is asking; baking this machine's answer into the
		// record is what would stop a NAS from ever adopting the job.
		spec.Sink.Partial = "work/" + id
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
	err := rec.DecodeCheckpoint(&c)
	return c, err
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
func (s Sink) Resolve(root string) (partial, final string) {
	return resolveUnder(root, s.Partial), resolveUnder(root, s.Final)
}

func resolveUnder(root, p string) string {
	if p == "" || !relativeEverywhere(p) {
		return p
	}
	// Forgive a backslash in a RELATIVE path, because records written before
	// Submit normalised them are on disk already, and because a backslash cannot
	// legally appear in a Windows filename anyway — so reading it as a separator
	// is the only interpretation that is ever right.
	return filepath.Join(root, filepath.FromSlash(strings.ReplaceAll(p, `\`, "/")))
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

// Portable puts a relative path into the one form every machine reads the same
// way. Absolute paths are left exactly as given — they already name a specific
// machine's filesystem, and rewriting their separators would not make them any
// more portable, only harder to recognise.
func Portable(p string) string {
	if !relativeEverywhere(p) {
		return p
	}
	return filepath.ToSlash(p)
}
