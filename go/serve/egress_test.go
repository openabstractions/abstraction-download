package serve

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-download/go/internal/personfolders"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

const egressDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

// productionEgress turns the loopback fixture setting off for one test.
func productionEgress(t *testing.T) {
	t.Helper()
	previous := loopbackFixtures.Swap(false)
	t.Cleanup(func() { loopbackFixtures.Store(previous) })
}

// The service's admission applies the egress rules with one plain reason
// each: http needs a digest, this computer and link-local addresses are
// refused, and the local network opens only for a host the person allowed
// [DL-G1, DL-G4, DL-G5, DL-G8].
func TestServiceAdmissionAppliesTheEgressRules(t *testing.T) {
	productionEgress(t)
	if err := personfolders.Under(t.TempDir(), t.Setenv); err != nil {
		t.Fatal(err)
	}
	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	submit := func(key, digest, locator string) api.AcceptanceResult {
		t.Helper()
		scheme := strings.SplitN(locator, ":", 2)[0]
		result, err := p.Bind("alice").Submit(api.Submission{Identity: api.RequestIdentity{Key: key, HistoryEpoch: window.HistoryEpoch},
			Kind: download.Kind, Spec: request.Encode(&request.Request{Artifact: request.Artifact{Digest: digest},
				Sources: []request.Source{{Scheme: scheme, Locator: locator}}})})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	for _, c := range []struct {
		key, digest, locator string
		outcome              api.AcceptanceOutcome
		reason               string
	}{
		{"plain-http", "", "http://files.example/model", api.AcceptanceOutcomeInvalid, "Refused: an http download needs a SHA-256 digest"},
		{"plain-http-digest", egressDigest, "http://files.example/model", api.AcceptanceOutcomeAccepted, ""},
		{"https", "", "https://files.example/model", api.AcceptanceOutcomeAccepted, ""},
		{"loopback", egressDigest, "http://127.0.0.1:9/model", api.AcceptanceOutcomeUnavailable, "Refused: address is on this computer"},
		{"localhost", "", "https://localhost/model", api.AcceptanceOutcomeUnavailable, "Refused: address is on this computer"},
		{"metadata", egressDigest, "http://169.254.169.254/latest", api.AcceptanceOutcomeUnavailable, "Refused: address is link-local"},
		{"lan", "", "https://192.168.1.20/model", api.AcceptanceOutcomeUnavailable, "Refused: address is on your local network"},
	} {
		got := submit(c.key, c.digest, c.locator)
		if got.Outcome != c.outcome || (c.reason != "" && got.Reason != c.reason) {
			t.Errorf("%s: %v %q, want %v %q", c.key, got.Outcome, got.Reason, c.outcome, c.reason)
		}
	}
	if err := download.DefaultLocalHosts().Add("192.168.1.20", "the person's NAS"); err != nil {
		t.Fatal(err)
	}
	if got := submit("lan", "", "https://192.168.1.20/model"); got.Outcome != api.AcceptanceOutcomeAccepted {
		t.Fatalf("an allowed local host: %+v", got)
	}
}

// The service checks every redirect hop: a redirect to a cloud metadata
// address ends the attempt not now with the rule's reason, and nothing
// connects to it [DL-G3, DL-G4].
func TestServiceRefusesARedirectToAMetadataAddress(t *testing.T) {
	body := []byte("never served")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data", http.StatusFound)
	}))
	defer origin.Close()
	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	id := api.RequestIdentity{Key: "metadata", HistoryEpoch: window.HistoryEpoch}
	spec := request.Encode(&request.Request{Artifact: request.Artifact{Size: int64(len(body))},
		Sources: []request.Source{{Scheme: "http", Locator: origin.URL + "/artifact"}}})
	if v, err := p.Bind("alice").Submit(api.Submission{Identity: id, Kind: download.Kind, Spec: spec}); err != nil || v.Outcome != api.AcceptanceOutcomeAccepted {
		t.Fatalf("submit: %+v %v", v, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Execute(ctx) }()
	defer func() { cancel(); <-done }()
	for {
		v, err := p.BindOperations("alice").ObserveWork(id)
		if err != nil || v.Snapshot == nil {
			t.Fatalf("observe: %+v %v", v, err)
		}
		if f := v.Snapshot.Failure; f != nil {
			want := "download attempt failed: Refused: address is link-local"
			if f.Classification.String() != "retryable" || string(f.Cause) != "refused" || f.Message != want {
				t.Fatalf("got %s %s %q, want retryable refused %q", f.Classification, f.Cause, f.Message, want)
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(fmt.Sprintf("no failure observed: %+v", v.Snapshot))
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// A mirror on a local-network address the person named on the allowed local
// hosts serves a digest-bound http download, as the install journey gate's
// mirror does; the same mirror is refused while the list does not name it,
// and naming a loopback address opens nothing [DL-G1, DL-G4, DL-G5].
func TestServiceAllowedLocalHostOpensALocalNetworkMirror(t *testing.T) {
	productionEgress(t)
	if err := personfolders.Under(t.TempDir(), t.Setenv); err != nil {
		t.Fatal(err)
	}
	body := []byte("bytes from the gate's own mirror")
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(body))
	mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
	defer mirror.Close()
	// The mirror's local-network address, dialled on this test's loopback
	// listener: the policy sees 192.168.56.10, and no packet leaves.
	const lanHost = "192.168.56.10"
	port := mirror.Listener.Addr().(*net.TCPAddr).Port
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr != net.JoinHostPort(lanHost, strconv.Itoa(port)) {
			return nil, fmt.Errorf("fixture dials only the mirror, not %s", addr)
		}
		return (&net.Dialer{}).DialContext(ctx, network, mirror.Listener.Addr().String())
	}
	previous := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: transport}
	t.Cleanup(func() { http.DefaultClient = previous })

	p, err := acceptanceprovider.OpenWithExecutor(t.TempDir(), "owner", HTTPExecution{})
	if err != nil {
		t.Fatal(err)
	}
	window, err := p.Bind("alice").GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	submit := func(key, host string) (api.RequestIdentity, api.AcceptanceResult) {
		id := api.RequestIdentity{Key: key, HistoryEpoch: window.HistoryEpoch}
		spec := request.Encode(&request.Request{Artifact: request.Artifact{Digest: digest, Size: int64(len(body))},
			Sources: []request.Source{{Scheme: "http", Locator: "http://" + net.JoinHostPort(host, strconv.Itoa(port)) + "/native-qualification/artifact"}}})
		result, err := p.Bind("alice").Submit(api.Submission{Identity: id, Kind: download.Kind, Spec: spec})
		if err != nil {
			t.Fatal(err)
		}
		return id, result
	}
	if _, refused := submit("unlisted", lanHost); refused.Outcome != api.AcceptanceOutcomeUnavailable || refused.Reason != "Refused: address is on your local network" {
		t.Fatalf("an unlisted local-network mirror: %+v", refused)
	}
	for _, host := range []string{lanHost, "127.0.0.1", "localhost"} {
		if err := download.DefaultLocalHosts().Add(host, "install journey gate mirror"); err != nil {
			t.Fatal(err)
		}
	}
	if _, loopback := submit("loopback", "127.0.0.1"); loopback.Outcome != api.AcceptanceOutcomeUnavailable || loopback.Reason != "Refused: address is on this computer" {
		t.Fatalf("a listed loopback mirror: %+v", loopback)
	}
	id, accepted := submit("listed", lanHost)
	if accepted.Outcome != api.AcceptanceOutcomeAccepted {
		t.Fatalf("a listed local-network mirror: %+v", accepted)
	}
	execute(t, p)
	waitFor(t, "the mirror download", 20*time.Second, func() bool { return observe(t, p, id).State == api.WorkStateComplete })
}
