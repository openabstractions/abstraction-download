package download

import (
	"path/filepath"
	"strings"

	storage "github.com/ReinisLusis/abstraction/storage/go"
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

// WithStorage gives a service somewhere to look for bytes it may not need to
// fetch, and somewhere to put the ones it does.
//
// Optional on purpose. A service without a store still works — it just always
// fetches, and the caller must name a destination.
func WithStorage(s storage.Store) func(*service) {
	return func(svc *service) { svc.store = s }
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
func (s *service) alreadyHere(spec Spec) Spec {
	if s.store == nil || spec.Artifact.Digest == "" {
		return spec
	}
	all, ok := s.store.(interface{ FindAll(string) []storage.Ref })
	var refs []storage.Ref
	if ok {
		refs = all.FindAll(spec.Artifact.Digest)
	} else if r, found := s.store.Find(spec.Artifact.Digest); found {
		refs = []storage.Ref{r}
	}
	local, _ := s.store.(storage.Local)
	if local == nil {
		// A store with no filesystem behind it cannot offer a file: source, and
		// pretending otherwise would hand the runner a locator nothing can open.
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
	found := make([]Source, 0, len(refs))
	for _, r := range refs {
		p := local.Path(r)
		if p == "" {
			continue
		}
		key := strings.ToLower(filepath.Clean(p))
		if seen[key] {
			continue
		}
		seen[key] = true
		found = append(found, Source{
			Scheme:   "file",
			Locator:  p,
			Priority: -100 + len(found), // ahead of anything a caller supplied
			Attrs:    map[string]string{"store": r.Store},
		})
	}
	spec.Sources = append(found, spec.Sources...)
	return spec
}

// intoStorage fills in a destination when the caller did not name one.
//
// "I want these bytes" and "I want them at this exact path" are different
// requests. The second is a legitimate thing for a person to ask — dl -o and
// modelget -o are not going anywhere — but it should not be the ONLY thing
// expressible, because then every application invents a filing system.
func (s *service) intoStorage(spec Spec) (Spec, error) {
	if spec.Sink.Final != "" || s.store == nil {
		return spec, nil
	}
	ref, err := s.store.Place(spec.Artifact.Digest, spec.Artifact.Size)
	if err != nil {
		return spec, err
	}
	local, ok := s.store.(storage.Local)
	if !ok {
		return spec, storage.ErrReadOnly
	}
	spec.Sink.Final = local.Path(ref)
	return spec, nil
}
