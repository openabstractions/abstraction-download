package download

import (
	"fmt"
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
func Submit(store *job.FileStore, spec Spec, requires ...string) (string, error) {
	if err := spec.Validate(); err != nil {
		return "", err
	}
	id := job.NewID()
	if spec.Sink.Partial == "" {
		spec.Sink.Partial = store.WorkPath(id)
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
