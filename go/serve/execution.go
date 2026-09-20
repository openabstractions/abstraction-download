package serve

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	"github.com/openabstractions/abstraction-download/go/netcost"
	job "github.com/openabstractions/abstraction-job/go"
)

// CredentialConsumer is the consumer contract the service names when it applies
// a credential to a download request (abstraction.credentials applier@1).
const CredentialConsumer = "abstraction.download/http-execution@1"

// CredentialGuarantee is the execution guarantee a request naming a credential
// requires (request.thrift credential_guarantees).
var CredentialGuarantee = request.CredentialGuarantees[0]

// NetworkCostGuarantee is the execution guarantee a request with network
// unmetered requires (request.thrift network_cost_guarantees) [DL-N2].
var NetworkCostGuarantee = request.NetworkCostGuarantees[0]

// PlatformNetworkCost opens the cost source an HTTPExecution with no
// NetworkCost of its own uses. The default opens this platform's source once
// per process and keeps it, and after a failure opens it again at most every
// two seconds; a host or test may replace it before any execution is
// configured.
var PlatformNetworkCost = sharedPlatformNetworkCost

// CredentialApplier applies a named credential, for the caller scope that
// submitted the work, to one outgoing request to host. It returns the headers
// for exactly that request, or a *download.CredentialError built with
// download.CredentialRefusal. The service calls it again for every request,
// redirect and resumed range, and never records what it returns.
// Implementations honor ctx and are safe for concurrent calls.
type CredentialApplier interface {
	ApplyCredential(ctx context.Context, scope, name, host string) (map[string]string, error)
}

// CredentialChecker is an optional part of a CredentialApplier: it reports,
// without reading a secret, whether ApplyCredential would apply the credential
// for scope to host now (abstraction.credentials applier@1 Check). A refusal is
// a *download.CredentialError. Admission asks it for every source naming a
// credential [JOB-A16, DL-K1].
type CredentialChecker interface {
	CheckCredential(ctx context.Context, scope, name, host string) error
}

// HTTPExecution is an explicitly configured service provider. Its input is the
// generated request payload; destinations are allocated by operation identity
// inside the private service root. It reuses the existing HTTP(S) runner.
// A source naming a credential needs Credentials and the
// abstraction.download/credentials@1 guarantee; external delegators require
// separately configured providers.
type HTTPExecution struct {
	OnError func(error)
	// Credentials applies named credentials through the credentials applier.
	// Nil refuses every request that names one.
	Credentials CredentialApplier
	// NetworkCost supplies the cost source network-constrained work waits on.
	// Nil uses PlatformNetworkCost. An error, such as netcost.ErrUnavailable,
	// leaves abstraction.download/network-cost@1 unadvertised.
	NetworkCost func() (netcost.Source, error)
}

// network is this execution's cost source, or nil when it has none.
func (e HTTPExecution) network() netcost.Source {
	open := e.NetworkCost
	if open == nil {
		open = PlatformNetworkCost
	}
	source, err := open()
	if err != nil {
		return nil
	}
	return source
}

func (HTTPExecution) Profile() string { return "download-http-request-v1" }

var credentialName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// Prepare accepts anonymous requests. A request naming a credential needs the
// credentials guarantee and the submitting scope, through PrepareScoped.
func (e HTTPExecution) Prepare(id, kind string, raw []byte) ([]byte, error) {
	work, _, err := prepareRequest("", id, kind, raw, false, false)
	return work, err
}

// ExecutionGuarantees advertises abstraction.download/credentials@1 when a
// credentials applier is configured, and abstraction.download/network-cost@1
// when this platform has a network cost source [DL-N2].
func (e HTTPExecution) ExecutionGuarantees() []string {
	guarantees := []string{}
	if e.Credentials != nil {
		guarantees = append(guarantees, CredentialGuarantee)
	}
	if e.network() != nil {
		guarantees = append(guarantees, NetworkCostGuarantee)
	}
	return guarantees
}

// PrepareWithGuarantees has no submitting scope, so it refuses credentials.
func (e HTTPExecution) PrepareWithGuarantees(id, kind string, raw []byte, required []string) ([]byte, []string, error) {
	return e.PrepareScoped("", id, kind, raw, required)
}

// RecoveryGuarantees are the execution guarantees this profile keeps for
// accepted work whatever answers now: work requiring network-cost@1 waits with
// network:unavailable until a cost source answers, and work naming a
// credential ends each attempt credential:unavailable until an applier is
// configured [JOB-A15, DL-N8].
func (HTTPExecution) RecoveryGuarantees() []string {
	return []string{CredentialGuarantee, NetworkCostGuarantee}
}

// PrepareScoped records each named credential and the submitting scope in the
// source attributes, which never go on the wire. An anonymous request prepares
// exactly as Prepare does. Preparation reads only the request and the required
// guarantees: whether an applier or a cost source answers now is decided by
// admission, so recovery prepares the same work while either is absent.
func (e HTTPExecution) PrepareScoped(scope, id, kind string, raw []byte, required []string) ([]byte, []string, error) {
	credentials, network := false, false
	for _, guarantee := range required {
		switch guarantee {
		case CredentialGuarantee:
			credentials = true
		case NetworkCostGuarantee:
			network = true
		default:
			return nil, nil, errors.New("unsupported execution guarantee")
		}
	}
	return prepareRequest(scope, id, kind, raw, credentials, network)
}

// ValidateRequest applies the profile's request checks without preparing work.
// A credential name is accepted when well formed.
func ValidateRequest(raw []byte) error {
	_, _, err := prepareRequest("validation", "validation", download.Kind, raw, true, true)
	return err
}

// prepareRequest returns the work and the requirements it persists. A request
// with network unmetered carries the constraint in its spec and
// abstraction.download/network-cost@1 in requires; network any and an absent
// constraint prepare exactly as before [DL-N1].
func prepareRequest(scope, id, kind string, raw []byte, credentials, network bool) ([]byte, []string, error) {
	if kind != download.Kind {
		return nil, nil, errors.New("unsupported work kind")
	}
	input, err := request.Decode(raw)
	if err != nil {
		return nil, nil, err
	}
	if id == "" || strings.ContainsAny(id, "/\\.:") {
		return nil, nil, errors.New("invalid operation identity")
	}
	spec := download.Spec{Artifact: download.Artifact{Digest: input.Artifact.Digest, Size: input.Artifact.Size},
		Sink: download.Sink{Partial: "work/" + id, Final: "results/" + id}}
	for _, source := range input.Sources {
		prepared := download.Source{Scheme: source.Scheme, Locator: source.Locator}
		if source.Credential != "" {
			if !credentials {
				return nil, nil, fmt.Errorf("a source naming a credential requires %s", CredentialGuarantee)
			}
			if !credentialName.MatchString(source.Credential) || scope == "" {
				return nil, nil, errors.New("invalid credential reference")
			}
			prepared.Attrs = map[string]string{download.CredentialAttr: source.Credential, download.CredentialScopeAttr: scope}
		}
		spec.Sources = append(spec.Sources, prepared)
	}
	var requires []string
	if input.Constraints != nil && input.Constraints.Network == request.NetworkUnmetered {
		if !network {
			return nil, nil, fmt.Errorf("a request with network unmetered requires %s", NetworkCostGuarantee)
		}
		spec.Constraints = &download.Constraints{Network: download.NetworkUnmetered}
		requires = []string{NetworkCostGuarantee}
	}
	if spec.Artifact.Size < 0 {
		return nil, nil, errors.New("negative artifact size")
	}
	if digest := spec.Artifact.Digest; digest != "" {
		decoded, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
		if !strings.HasPrefix(digest, "sha256:") || err != nil || len(decoded) != 32 {
			return nil, nil, errors.New("SHA-256 digest required")
		}
	}
	if err := spec.ValidateFor(id); err != nil {
		return nil, nil, err
	}
	for _, source := range spec.Sources {
		u, err := url.Parse(source.Locator)
		if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") || source.Scheme != u.Scheme {
			return nil, nil, errors.New("anonymous HTTP(S) source required")
		}
	}
	work, err := json.Marshal(spec)
	return work, requires, err
}

// Serve runs the service sweep. A cost source that cannot be opened yet is
// asked again until it answers; meanwhile work requiring an unmetered path
// waits with network:unavailable and other work proceeds [DL-N8].
func (e HTTPExecution) Serve(ctx context.Context, store job.Store) error {
	open := e.NetworkCost
	if open == nil {
		open = PlatformNetworkCost
	}
	network, stop := servedNetwork(ctx, open)
	defer stop()
	return serveExecutionWith(ctx, store, e.OnError, nil, httpTerminal, e.Credentials, network)
}

// httpTerminal ends the failures the download-http-request-v1 profile treats as
// final beyond the library's refusals [JOB-A8]: bytes that fail their digest and
// a source larger than the requested size. Trying the same operation again
// fetches the same answer; a caller retries through a new attempt [JOB-A7].
// Network errors and 5xx answers stay retryable.
// A mismatch over a resumed prefix stays retryable: the runner restarts from
// zero, and only a mismatch over bytes fetched from zero ends the operation.
func httpTerminal(err error) bool {
	mismatch := errors.Is(err, download.ErrDigestMismatch) && !errors.Is(err, download.ErrResumedMismatch)
	return mismatch || errors.Is(err, download.ErrOversize)
}

// configureRunner lets tests shorten the service runner's intervals.
var configureRunner = func(*download.Runner) {}

func serveExecution(ctx context.Context, store job.Store, onError func(error), delegates []download.Delegator) error {
	return serveExecutionWith(ctx, store, onError, delegates, nil, nil, nil)
}

func serveExecutionWith(ctx context.Context, store job.Store, onError func(error), delegates []download.Delegator, terminal func(error) bool, applier CredentialApplier, network netcost.Source) error {
	runner := download.NewRunner(store, "service-"+job.NewID())
	runner.Terminal = terminal
	// A service profile with its own terminal policy, and delegated execution
	// relaying a receiving service's refusal, also report typed causes.
	runner.RecordCause = terminal != nil || delegates != nil
	runner.Fetchers = download.NewFetchers(download.HTTP{})
	if delegates != nil {
		runner.Delegators = download.NewDelegators(delegates...)
	}
	runner.SharedStore = true
	runner.Credentials = noExecutionCredentials{}
	if applier != nil {
		runner.Credentials = appliedCredentials{ctx: ctx, applier: applier}
	}
	runner.Network = network
	configureRunner(runner)
	defer runner.Close()
	watch := job.Watch(store, download.Kind)
	defer watch.Close()
	// A cost change is a reason to sweep: work waiting for an unmetered path
	// is adopted on the notice that ends the wait [DL-N4].
	costChanged := make(chan struct{}, 1)
	if network != nil {
		costs := network.Watch()
		defer costs.Close()
		go func() {
			for {
				if _, err := costs.Next(ctx); err != nil {
					return
				}
				select {
				case costChanged <- struct{}{}:
				default:
				}
			}
		}()
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		_, _, _, _, problems := Pass(ctx, runner)
		for _, err := range problems {
			if onError != nil {
				onError(fmt.Errorf("download execution: %w", err))
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-watch.Changes():
		case <-costChanged:
		case <-ticker.C:
		}
	}
	return nil
}

type noExecutionCredentials struct{}

func (noExecutionCredentials) Lookup(string, string) (map[string]string, bool) { return nil, false }

// LookupSource answers a source naming a credential in a service with no
// applier: nothing can apply it now, and the attempt stays retryable.
func (noExecutionCredentials) LookupSource(src download.Source, _ string) (map[string]string, error) {
	return nil, download.CredentialRefusal(src.Attrs[download.CredentialAttr], "unavailable")
}

// appliedCredentials asks the applier for every request the runner sends to a
// source naming a credential, with the scope preparation recorded.
type appliedCredentials struct {
	ctx     context.Context
	applier CredentialApplier
}

func (appliedCredentials) Lookup(string, string) (map[string]string, bool) { return nil, false }

func (c appliedCredentials) LookupSource(src download.Source, host string) (map[string]string, error) {
	name, scope := src.Attrs[download.CredentialAttr], src.Attrs[download.CredentialScopeAttr]
	if scope == "" {
		return nil, download.CredentialRefusal(name, "forbidden")
	}
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	headers, err := c.applier.ApplyCredential(ctx, scope, name, host)
	if err != nil {
		var typed *download.CredentialError
		if !errors.As(err, &typed) {
			return nil, download.CredentialRefusal(name, "unavailable")
		}
		return nil, err
	}
	if len(headers) == 0 {
		return nil, download.CredentialRefusal(name, "unavailable")
	}
	return headers, nil
}

// requiresCredentials reports whether a guarantee list names the credentials guarantee.
func requiresCredentials(required []string) bool {
	return slices.Contains(required, CredentialGuarantee)
}
