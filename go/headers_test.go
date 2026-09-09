package download

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// headerSpy answers ranged requests and keeps every header it was sent.
type headerSpy struct {
	*httptest.Server
	mu   sync.Mutex
	seen []http.Header
}

func newHeaderSpy(t *testing.T, body []byte) *headerSpy {
	t.Helper()
	s := &headerSpy{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.seen = append(s.seen, r.Header.Clone())
		s.mu.Unlock()
		http.ServeContent(w, r, "blob.bin", time.Unix(0, 0), strings.NewReader(string(body)))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *headerSpy) requests() []http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]http.Header(nil), s.seen...)
}

// stubCreds is a credential that exists, so the test can tell "the secret
// arrived" apart from "nothing arrived at all".
type stubCreds struct{}

const stubSecret = "resolved-on-this-machine"

func (stubCreds) Lookup(name, host string) (map[string]string, bool) {
	if name != "spy" {
		return nil, false
	}
	return map[string]string{"Authorization": "Bearer " + stubSecret}, true
}

// leakyAttrs is every attribute this package defines, plus attributes chosen to
// look exactly like headers — including the one the transport sets for itself.
//
// The old shape copied anything it did not recognise into the request, so each
// of these was one forgotten switch case away from a third party. The rule is
// now the shape rather than the list, which is why this test does not name any
// particular attribute in its assertion: it asserts that NONE of them arrive.
var leakyAttrs = map[string]string{
	CredentialAttr:                "spy",
	CredentialHeaderAttr:          "X-Auth",
	"store":                       "ollama",
	BoundariesAttr:                "16777216",
	"X-Internal-Span-Table":       "a-span-table-nobody-outside-should-see",
	"Authorization":               "Bearer this-value-is-inert",
	"Cookie":                      "session=inert",
	"User-Agent":                  "leaky-agent-value",
	"Range":                       "bytes=0-5",
	"whatever-anyone-adds-next":   "the-value-that-would-have-leaked",
	"x-adopter-private-file-path": "a-path-from-somebody-machine",
}

// transportOwned are headers the HTTP client sets on its own behalf, so their
// presence proves nothing. Their VALUES still must never be a record's.
var transportOwned = map[string]bool{
	"user-agent": true, "range": true, "host": true,
	"accept-encoding": true, "connection": true,
}

func TestNoSourceAttributeEverReachesTheWire(t *testing.T) {
	for _, tc := range []struct {
		name        string
		size        int
		connections int
	}{
		{"one stream", 4096, 1},
		{"several connections", minParallel + 7, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, digest := payload(t, tc.size)
			spy := newHeaderSpy(t, body)
			r, store, root := newRunner(t)
			r.Connections = tc.connections
			r.Credentials = stubCreds{}

			id, err := Submit(store, Spec{
				Artifact: Artifact{Digest: digest, Size: int64(len(body))},
				Sources: []Source{{
					Scheme:  "https",
					Locator: spy.URL + "/blob.bin",
					Attrs:   leakyAttrs,
				}},
				Sink: Sink{Final: filepath.Join(root, "final.bin")},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Run(context.Background(), id); err != nil {
				t.Fatalf("run: %v", err)
			}

			reqs := spy.requests()
			if len(reqs) == 0 {
				t.Fatal("the server was never asked for anything")
			}
			for i, h := range reqs {
				for name := range leakyAttrs {
					// A name the transport sets for itself cannot be asserted
					// absent; the value check below is what catches a record
					// that overrode one.
					if transportOwned[strings.ToLower(name)] {
						continue
					}
					if got := h.Values(name); len(got) > 0 {
						t.Fatalf("request %d carried attribute %q as a header: %v", i, name, got)
					}
				}
				for name, values := range h {
					for _, v := range values {
						for attr, leaked := range leakyAttrs {
							if v == leaked {
								t.Fatalf("request %d sent header %s: %q, which is the value of attribute %q", i, name, v, attr)
							}
						}
					}
				}
				// The credential still arrives, under the header the source
				// named. A test that only proved nothing was sent would pass
				// just as well against a fetcher that sent no headers at all.
				if got := h.Get("X-Auth"); got != "Bearer "+stubSecret {
					t.Fatalf("request %d did not carry the resolved credential: %q", i, got)
				}
			}
		})
	}
}

// The other half: a header the caller named explicitly is sent, so the split
// removes the accident and not the capability.
func TestAnExplicitHeaderIsSent(t *testing.T) {
	body, digest := payload(t, 4096)
	spy := newHeaderSpy(t, body)
	r, store, root := newRunner(t)
	r.Connections = 1

	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources: []Source{{
			Scheme:  "https",
			Locator: spy.URL + "/blob.bin",
			Headers: map[string]string{"X-Repo": "openabstractions"},
			Attrs:   map[string]string{"store": "ollama"},
		}},
		Sink: Sink{Final: filepath.Join(root, "final.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("run: %v", err)
	}
	for i, h := range spy.requests() {
		if got := h.Get("X-Repo"); got != "openabstractions" {
			t.Fatalf("request %d did not carry the header the source named: %q", i, got)
		}
	}
}

// A record may not decide the headers this layer resolves for itself. Two of
// these would be a stored secret, which is the rule credentials.go exists for;
// Range would be a record telling the server a different story than the one the
// resume logic believes.
func TestARecordMayNotCarryAResolvedHeader(t *testing.T) {
	for _, name := range []string{"Authorization", "authorization", "Proxy-Authorization", "Cookie", "Range", " "} {
		spec := Spec{
			Sources: []Source{{Scheme: "https", Locator: "https://example.invalid/x", Headers: map[string]string{name: "v"}}},
			Sink:    Sink{Final: "models/x.gguf"},
		}
		if err := spec.Validate(); err == nil {
			t.Fatalf("a source carrying header %q was accepted", name)
		}
	}
	// Including the one the source itself nominated for its credential, which
	// is not in any fixed list.
	spec := Spec{
		Sources: []Source{{
			Scheme:  "https",
			Locator: "https://example.invalid/x",
			Attrs:   map[string]string{CredentialAttr: "hf", CredentialHeaderAttr: "X-Auth"},
			Headers: map[string]string{"x-auth": "Bearer this-would-shadow-the-credential"},
		}},
		Sink: Sink{Final: "models/x.gguf"},
	}
	if err := spec.Validate(); err == nil {
		t.Fatal("a source overrode its own credential header from the record")
	}
}

// What a reader does with a record written before this split: the attributes
// are attributes, and none of them are sent. Nothing this project ever wrote
// put a header in there — the audit is in feedback/2026-09-06-attrs-headers.md
// — so the old shape loses nothing by being read the new way.
func TestAnOldRecordsAttributesAreNotSent(t *testing.T) {
	const old = `{"artifact":{"size":4},"sources":[{"scheme":"https","locator":"https://example.invalid/x",
	  "attrs":{"store":"ollama","X-Legacy":"whatever-this-was"}}],"sink":{"final":"models/x.gguf"}}`
	var spec Spec
	if err := json.Unmarshal([]byte(old), &spec); err != nil {
		t.Fatal(err)
	}
	h, err := headersFor(spec.Sources[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 0 {
		t.Fatalf("a record written before headers existed still sent %v", h)
	}
}

// A spec that names no headers must encode exactly as it did before the field
// existed, or every record in every store changes bytes for nothing and the
// record conformance harness starts reporting differences that are not.
func TestASpecWithoutHeadersIsUnchangedOnTheWire(t *testing.T) {
	b, err := json.Marshal(Spec{
		Artifact: Artifact{Size: 4},
		Sources:  []Source{{Scheme: "https", Locator: "https://example.invalid/x"}},
		Sink:     Sink{Partial: "work/a", Final: "models/x.gguf"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "headers") {
		t.Fatalf("a spec that names no headers wrote the key anyway: %s", b)
	}
}

// And a spec that does name them survives the round trip through a record,
// because a successor in another process is the reader that matters.
func TestHeadersSurviveTheRecord(t *testing.T) {
	root := t.TempDir()
	store, err := job.NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id, err := Submit(store, Spec{
		Artifact: Artifact{Size: 4},
		Sources: []Source{{
			Scheme:  "https",
			Locator: "https://example.invalid/x",
			Headers: map[string]string{"X-Repo": "openabstractions"},
		}},
		Sink: Sink{Final: filepath.Join(root, "x.gguf")},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := SpecOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Sources[0].Headers["X-Repo"] != "openabstractions" {
		t.Fatalf("the header did not survive the record: %+v", spec.Sources[0])
	}
	if _, err := os.Stat(filepath.Join(root, "jobs")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

// The credential is resolved once per range, not once per job. A read token from
// a content-addressed store lasts minutes and a large artifact is hundreds of
// ranges over hours, so headers taken once start returning 401 two thirds of the
// way through.
type countingCreds struct {
	mu sync.Mutex
	n  int
}

func (c *countingCreds) Lookup(name, host string) (map[string]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return map[string]string{"Authorization": "Bearer token-" + strconv.Itoa(c.n)}, true
}

func TestEveryRangeResolvesTheCredentialAgain(t *testing.T) {
	body, digest := payload(t, minParallel+7)
	spy := newHeaderSpy(t, body)
	r, store, root := newRunner(t)
	r.Connections = 4
	creds := &countingCreds{}
	r.Credentials = creds

	id, err := Submit(store, Spec{
		Artifact: Artifact{Digest: digest, Size: int64(len(body))},
		Sources: []Source{{
			Scheme:  "https",
			Locator: spy.URL + "/blob.bin",
			Attrs:   map[string]string{CredentialAttr: "spy"},
		}},
		Sink: Sink{Final: filepath.Join(root, "final.bin")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), id); err != nil {
		t.Fatalf("run: %v", err)
	}

	ranges := 0
	for _, h := range spy.requests() {
		if strings.Contains(h.Get("Range"), "-") && h.Get("Range") != "bytes=0-0" {
			ranges++
		}
	}
	if ranges < 2 {
		t.Fatalf("the job did not run as several ranges: %d", ranges)
	}
	creds.mu.Lock()
	looked := creds.n
	creds.mu.Unlock()
	if looked <= 1 {
		t.Fatalf("the credential was resolved %d time(s) for %d ranges; a token that expires mid-download would fail every one after the first", looked, ranges)
	}
}
