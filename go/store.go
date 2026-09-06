package download

import (
	"os"
	"path/filepath"
	"strings"

	job "github.com/openabstractions/abstraction-job/go"
	storage "github.com/openabstractions/abstraction-storage/go"
)

// This file is where bytes in motion meet bytes at rest.
//
// Two things happen here, and both used to be somebody else's problem:
//
//  1. Before fetching anything, ask whether these exact bytes are already on
//     this machine. If they are, the transfer becomes a local copy — or, on one
//     volume, a hardlink and no copy at all.
//
//  2. When a caller does not name a destination, let the store choose one.
//     Sink{Partial, Final} used to be two strings every caller filled in by
//     hand, which is how a layer that is pluggable about WHO fetches the bytes
//     ended up hardcoded about WHERE they land.
//
// The first of these existed, in the model layer, as Local.Augment. Nothing
// about "are these bytes already here" is AI-specific, and having it there meant
// dl never got it: a plain `dl <url>` with a known digest would re-download
// something Ollama already had.

// WithStorage gives a client somewhere to look for bytes it may not need to
// fetch, and somewhere to put the ones it does.
//
// Optional on purpose. A client without a store still works — it just always
// fetches, and the caller must name a destination.
func WithStorage(s storage.Store) Option {
	return func(o *Options) { o.Storage = s }
}

// alreadyHere puts every local copy in front of the remote sources, so the
// runner reaches for bytes on this disk before it touches the network.
//
// The ordering is the whole feature: Runner tries sources by priority, so
// cross-store dedup falls out of a design built for delegation, with no special
// case anywhere. A model already pulled by Ollama is not downloaded again for
// something else — it is copied, or hardlinked, and verified either way.
//
// Verified, note. A hit is trusted only as far as the store's own naming
// convention goes: Ollama's blobs hash to their own filenames, and Ollama does
// not check them, so a corrupt blob there is a real possibility and this must
// not be the last word on whether the bytes are right.
//
// The job store's own finished records come first. They are the one place a
// digest was actually checked, and they were the copies nobody looked at when
// 386 MB was fetched a third time on 2026-09-06.
func (s *client) alreadyHere(spec Spec) Spec {
	if spec.Artifact.Digest == "" {
		return spec
	}
	// A resolver may already have named the same file — the Ollama model
	// resolver hands back the very blob a scan would find. Offering one path
	// twice makes a caller think there are two copies, and makes a failed
	// source get retried against itself.
	seen := make(map[string]bool, len(spec.Sources))
	for _, src := range spec.Sources {
		if src.Scheme == "file" {
			seen[strings.ToLower(filepath.Clean(src.Locator))] = true
		}
	}
	var found []Source
	add := func(src Source) {
		key := strings.ToLower(filepath.Clean(src.Locator))
		if src.Locator == "" || seen[key] {
			return
		}
		seen[key] = true
		src.Priority = -100 + len(found) // ahead of anything a caller supplied
		found = append(found, src)
	}
	for _, src := range proven(s.runner.Store, spec.Artifact.Digest, "") {
		add(src)
	}
	// A store with no filesystem behind it cannot offer a file: source, and
	// pretending otherwise would hand the runner a locator nothing can open.
	if local, ok := s.opts.Storage.(storage.Local); ok {
		var refs []storage.Ref
		if all, ok := s.opts.Storage.(interface{ FindAll(string) []storage.Ref }); ok {
			refs = all.FindAll(spec.Artifact.Digest)
		} else if r, ok := s.opts.Storage.Find(spec.Artifact.Digest); ok {
			refs = []storage.Ref{r}
		}
		for _, r := range refs {
			add(Source{Scheme: "file", Locator: local.Path(r), Attrs: map[string]string{"store": r.Store}})
		}
	}
	spec.Sources = append(found, spec.Sources...)
	return spec
}

// proven is every file this store has already verified against digest: the
// final of a record that transferred these exact bytes, still there at the size
// it proved. A digest is an identity, so it does not matter whose destination
// the file was — a caller's earlier one, a delegate's own files/ — and the
// record is the proof, so nothing is re-hashed to offer it. except is the job
// asking, which must not be offered its own destination.
func proven(store job.Store, digest, except string) []Source {
	if digest == "" {
		return nil
	}
	all, err := store.List()
	if err != nil {
		return nil
	}
	var out []Source
	seen := map[string]bool{}
	for _, rec := range all {
		if rec.ID == except || rec.Kind != Kind || (rec.State != job.StateTransferred && rec.State != job.StateComplete) {
			continue
		}
		spec, err := SpecOf(rec)
		if err != nil || !sameDigest(spec.Artifact.Digest, digest) {
			continue
		}
		_, final, err := LocalSink(store, rec.ID, spec.Sink)
		if err != nil {
			continue
		}
		size := spec.Artifact.Size
		if size <= 0 {
			size = rec.Progress.Done
		}
		st, err := os.Stat(final)
		if err != nil || st.IsDir() || size <= 0 || st.Size() != size || seen[comparablePath(final)] {
			continue
		}
		seen[comparablePath(final)] = true
		out = append(out, Source{
			Scheme:   "file",
			Locator:  final,
			Priority: -200 + len(out),
			Attrs:    map[string]string{"store": "job", "job": rec.ID},
		})
	}
	return out
}

// intoStorage fills in a destination when the caller did not name one.
//
// "I want these bytes" and "I want them at this exact path" are different
// requests. The second is a legitimate thing for a person to ask — dl -o and
// modelget -o are not going anywhere — but it should not be the ONLY thing
// expressible, because then every application invents a filing system.
func (s *client) intoStorage(spec Spec) (Spec, error) {
	if spec.Sink.Final != "" || s.opts.Storage == nil {
		return spec, nil
	}
	ref, err := s.opts.Storage.Place(spec.Artifact.Digest, spec.Artifact.Size)
	if err != nil {
		return spec, err
	}
	local, ok := s.opts.Storage.(storage.Local)
	if !ok {
		return spec, storage.ErrReadOnly
	}
	spec.Sink.Final = local.Path(ref)
	return spec, nil
}
