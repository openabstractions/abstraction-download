package nas

import (
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
)

// A delegate must be able to refuse work it could never do.
//
// Scheme and capabilities are not enough, and the gap stranded real work: the
// NAS serves "https" and promises to survive process exit, so it was handed a
// job whose source was http://127.0.0.1 — an address meaning THIS machine, and
// unreachable from any other. It could never fetch it, the record sat running
// forever, and every sweep reported reconciling it with nothing to show.
func TestTheNasRefusesSourcesOnlyThisMachineCanReach(t *testing.T) {
	// A UNC root: the far side really is another machine, which is when a
	// loopback source becomes unreachable.
	d := &Delegator{Root: `//nas/share/store`, Dir: DefaultDir}

	unreachable := []string{
		"http://127.0.0.1:8792/model.gguf",
		"http://localhost:8080/model.gguf",
		"https://LOCALHOST/model.gguf",
		"http://[::1]:9000/model.gguf",
		"http://0.0.0.0:8080/model.gguf",
	}
	for _, locator := range unreachable {
		spec := download.Spec{Sources: []download.Source{{Scheme: "http", Locator: locator}}}
		if d.CanServe(spec) {
			t.Errorf("accepted %s, which no other machine can reach", locator)
		}
	}

	// And it must not become squeamish. A NAS on the same LAN reaches a private
	// address perfectly well, and a delegate that refused work it could actually
	// do would be a worse bug than the one being fixed.
	reachable := []string{
		"https://huggingface.co/x/resolve/main/model.gguf",
		"http://192.168.1.50/model.gguf",
		"http://nas.local:8080/model.gguf",
		"https://10.0.0.5/model.gguf",
	}
	for _, locator := range reachable {
		spec := download.Spec{Sources: []download.Source{{Scheme: "https", Locator: locator}}}
		if !d.CanServe(spec) {
			t.Errorf("refused %s, which it could have fetched", locator)
		}
	}
}

// And the selection has to actually consult it, or the refusal is decoration.
func TestASelectiveDelegateIsNotOfferedWorkItRefused(t *testing.T) {
	// A UNC root: the far side really is another machine, which is when a
	// loopback source becomes unreachable.
	d := &Delegator{Root: `//nas/share/store`, Dir: DefaultDir}
	ds := download.NewDelegators(d)

	loopback := download.Source{Scheme: "http", Locator: "http://127.0.0.1:8792/x.gguf"}
	spec := download.Spec{Sources: []download.Source{loopback}}
	if _, ok := ds.ForSpec(spec, loopback, nil); ok {
		t.Fatal("a loopback job was still offered to the NAS; it would sit running forever")
	}

	public := download.Source{Scheme: "https", Locator: "https://huggingface.co/x.gguf"}
	ok := false
	if _, ok = ds.ForSpec(download.Spec{Sources: []download.Source{public}}, public, nil); !ok {
		t.Fatal("an ordinary public download was refused")
	}
}

// The refinement above has a cost worth pinning: when the store is a local
// directory, the far side IS this machine, and loopback must be accepted.
//
// This is how the delegator is tested, and it is a real deployment — one
// machine, one store, a supervisor that outlives the application. A rule that
// refused work it could actually do would be a worse bug than the stranding it
// was written to prevent.
func TestALocalStoreStillAcceptsLoopback(t *testing.T) {
	d := &Delegator{Root: t.TempDir(), Dir: DefaultDir}
	spec := download.Spec{Sources: []download.Source{
		{Scheme: "http", Locator: "http://127.0.0.1:8792/model.gguf"},
	}}
	if !d.CanServe(spec) {
		t.Fatal("a local store refused a loopback source it could have fetched")
	}
}
