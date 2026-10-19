package serve

import (
	"context"
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
	downloadarchive "github.com/openabstractions/abstraction-download/go/archive"
	"github.com/openabstractions/abstraction-download/go/netcost"
	job "github.com/openabstractions/abstraction-job/go"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
	"github.com/openabstractions/abstraction-resource/go/wake"
	rights "github.com/openabstractions/abstraction-rights/go/abstraction/rights/api"
	storage "github.com/openabstractions/abstraction-storage/go"
)

// CredentialConsumer is the consumer contract the service names when it applies
// a credential to a download request (abstraction.credentials applier@1).
const CredentialConsumer = "abstraction.download/http-execution@1"

// CredentialGuarantee is the execution guarantee a request naming a credential
// requires (request.thrift credential_guarantees).
var CredentialGuarantee = request.CredentialGuarantees[0]

// ScopedCredentialGuarantee explicitly selects credentials/applier@2 for the
// whole submitted request. It never changes credentials@1 preparation.
var ScopedCredentialGuarantee = request.ScopedCredentialGuarantees[0]

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
// redirect and resumed range, and never records what it returns [DL-K4].
// Implementations honor ctx and are safe for concurrent calls. Every applier
// decides the use without asking anyone: admission asked its first-use
// question, and a rule removed since then refuses not_permitted with no
// question raised while the job runs (DL-K4, abstraction-rights RIGHTS-R23).
type CredentialApplier interface {
	ApplyCredential(ctx context.Context, scope, name, host string) (map[string]string, error)
}

// SubjectCredentialApplier uses a subject retained by the receiving job
// provider. It must recheck current permission on every application.
type SubjectCredentialApplier interface {
	ApplySubjectCredential(ctx context.Context, subject acceptanceprovider.AuthenticatedSubject, name, host string) (map[string]string, error)
}

// CredentialChecker is an optional part of a CredentialApplier: it reports,
// without reading a secret, whether ApplyCredential would apply the credential
// for scope to host now (abstraction.credentials applier@1 Check). A refusal is
// a *download.CredentialError. Admission asks it for every source naming a
// credential [JOB-A16, DL-K1].
type CredentialChecker interface {
	CheckCredential(ctx context.Context, scope, name, host string) error
}

// SubjectCredentialChecker is CredentialChecker for a bound subject. A
// not_permitted refusal may also carry a PendingQuestions() []string method
// listing the first-use questions its decision raised or joined that are
// still pending; admission names each as rights:question_pending:<id>
// (INF-W11). ScopedSubjectCredentialChecker refusals carry it alike.
type SubjectCredentialChecker interface {
	CheckSubjectCredential(ctx context.Context, subject acceptanceprovider.AuthenticatedSubject, name, host string) error
}

// ScopedSubjectCredentialApplier applies the explicit credentials@2 route for
// the receiving job owner's subject, with current scoped authorization.
type ScopedSubjectCredentialApplier interface {
	ApplyScopedSubjectCredential(ctx context.Context, subject acceptanceprovider.AuthenticatedSubject, name, host string) (map[string]string, error)
}

type ScopedSubjectCredentialChecker interface {
	CheckScopedSubjectCredential(ctx context.Context, subject acceptanceprovider.AuthenticatedSubject, name, host string) error
}

// ScopedCredentialAvailability reports whether the scoped enforcer and its
// typed policy are configured. Temporary decision outages retain this route.
type ScopedCredentialAvailability interface {
	ScopedCredentialsAvailable() bool
}

func scopedCredentialsAvailable(applier CredentialApplier) bool {
	available, ok := applier.(ScopedCredentialAvailability)
	_, applies := applier.(ScopedSubjectCredentialApplier)
	_, checks := applier.(ScopedSubjectCredentialChecker)
	return ok && applies && checks && available.ScopedCredentialsAvailable()
}

// HTTPExecution is an explicitly configured service provider. Its input is the
// generated request payload; destinations are allocated by operation identity
// inside the private service root. It reuses the existing HTTP(S) runner.
// A source naming a credential needs Credentials and exactly one credential
// route guarantee; external delegators require
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
	// Archive is checked before the sources of every job that names its
	// SHA-256 and keeps a copy of what the sources delivered [DL-R44]. Nil
	// checks none.
	Archive         download.DownloadArchive
	CheckWriteSpace func(folder string, additional int64) error
}

// network is this execution's cost source, or nil when it has none.
func (e HTTPExecution) network() netcost.Source {
	open := e.NetworkCost
	if open == nil {
		open = PlatformNetworkCost
	}
	source, err := open()
	if err != nil || source == nil || source.Cost() == netcost.CostUnavailable {
		return nil
	}
	return source
}

func (HTTPExecution) Profile() string { return "download-http-request-v1" }

var credentialName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// Prepare accepts anonymous requests. A request naming a credential needs the
// credentials guarantee and the submitting scope, through PrepareScoped.
func (e HTTPExecution) Prepare(id, kind string, raw []byte) ([]byte, error) {
	work, _, err := prepareRequest("", id, kind, raw, "", false)
	return work, err
}

// ExecutionGuarantees advertises abstraction.download/credentials@1 when a
// credentials applier is configured, credentials@2 with its configured scoped
// checker and applier, and abstraction.download/network-cost@1
// when this platform has a network cost source [DL-N2].
func (e HTTPExecution) ExecutionGuarantees() []string {
	guarantees := []string{}
	if e.Credentials != nil {
		guarantees = append(guarantees, CredentialGuarantee)
	}
	if scopedCredentialsAvailable(e.Credentials) {
		guarantees = append(guarantees, ScopedCredentialGuarantee)
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
	return []string{CredentialGuarantee, ScopedCredentialGuarantee, NetworkCostGuarantee}
}

// PrepareScoped records each named credential and the submitting scope in the
// source attributes, which never go on the wire. An anonymous request prepares
// exactly as Prepare does. Preparation reads only the request and the required
// guarantees: whether an applier or a cost source answers now is decided by
// admission, so recovery prepares the same work while either is absent.
func (e HTTPExecution) PrepareScoped(scope, id, kind string, raw []byte, required []string) ([]byte, []string, error) {
	route, err := credentialRouteFor(required)
	if err != nil {
		return nil, nil, err
	}
	network := false
	for _, guarantee := range required {
		switch guarantee {
		case CredentialGuarantee, ScopedCredentialGuarantee:
		case NetworkCostGuarantee:
			network = true
		default:
			return nil, nil, errors.New("unsupported execution guarantee")
		}
	}
	return prepareRequest(scope, id, kind, raw, route, network)
}

// ValidateRequest applies the profile's request checks without preparing work.
// A credential name is accepted when well formed.
func ValidateRequest(raw []byte) error {
	_, _, err := prepareRequest("validation", "validation", download.Kind, raw, CredentialGuarantee, true)
	return err
}

// prepareRequest returns the work and the requirements it persists. A request
// with network unmetered carries the constraint in its spec and
// abstraction.download/network-cost@1 in requires; network any and an absent
// constraint prepare exactly as before [DL-N1].
func prepareRequest(scope, id, kind string, raw []byte, route string, network bool) ([]byte, []string, error) {
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
			if route == "" {
				return nil, nil, fmt.Errorf("a source naming a credential requires %s", CredentialGuarantee)
			}
			if !credentialName.MatchString(source.Credential) || scope == "" {
				return nil, nil, errors.New("invalid credential reference")
			}
			prepared.Attrs = map[string]string{download.CredentialAttr: source.Credential, download.CredentialScopeAttr: scope}
			if route == ScopedCredentialGuarantee {
				prepared.Attrs[download.CredentialRouteAttr] = route
			}
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
		if !storage.ValidDigest(digest) {
			return nil, nil, errors.New("SHA-256 digest required")
		}
	}
	if err := spec.ValidateFor(id); err != nil {
		return nil, nil, err
	}
	for _, source := range spec.Sources {
		u, err := url.Parse(source.Locator)
		if err != nil {
			return nil, nil, errors.New("invalid HTTP(S) source locator")
		}
		if u.User != nil {
			return nil, nil, errors.New("source userinfo is not supported; use a named credential")
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, nil, fmt.Errorf("unsupported source scheme %q: HTTP(S) required", u.Scheme)
		}
		if u.Host == "" {
			return nil, nil, errors.New("HTTP(S) source host required")
		}
		if source.Scheme != u.Scheme {
			return nil, nil, errors.New("source scheme does not match locator scheme")
		}
	}
	work, err := json.Marshal(spec)
	return work, requires, err
}

// Serve runs the service sweep. A cost source that cannot be opened yet is
// asked again until it answers; meanwhile work requiring an unmetered path
// waits with network:unavailable and other work proceeds [DL-N8].
func (e HTTPExecution) Serve(ctx context.Context, store job.Store) error {
	return e.serveWithLookup(ctx, store, nil)
}

func (e HTTPExecution) serveWithLookup(ctx context.Context, store job.Store, lookup acceptanceprovider.SubjectLookup) error {
	open := e.NetworkCost
	if open == nil {
		open = PlatformNetworkCost
	}
	network, stop := servedNetwork(ctx, open)
	defer stop()
	return serveExecutionArchived(ctx, store, e.OnError, nil, httpTerminal, e.Credentials, network, lookup, e.Archive, e.CheckWriteSpace)
}

func (e HTTPExecution) ServeWithSubjectLookup(ctx context.Context, store job.Store, lookup acceptanceprovider.SubjectLookup) error {
	return e.serveWithLookup(ctx, store, lookup)
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
	return serveExecutionWithLookup(ctx, store, onError, delegates, terminal, applier, network, nil)
}

func serveExecutionWithLookup(ctx context.Context, store job.Store, onError func(error), delegates []download.Delegator, terminal func(error) bool, applier CredentialApplier, network netcost.Source, lookup acceptanceprovider.SubjectLookup) error {
	return serveExecutionArchived(ctx, store, onError, delegates, terminal, applier, network, lookup, nil)
}

func serveExecutionArchived(ctx context.Context, store job.Store, onError func(error), delegates []download.Delegator, terminal func(error) bool, applier CredentialApplier, network netcost.Source, lookup acceptanceprovider.SubjectLookup, archive download.DownloadArchive, space ...func(string, int64) error) error {
	runner := download.NewRunner(store, "service-"+job.NewID())
	if len(space) != 0 {
		runner.CheckWriteSpace = space[0]
	}
	// Served work always needs its retained owner. A missing/legacy journal
	// holds no wake; it never substitutes this runtime's principal.
	runner.Wake = operationWake{lookup: lookup}
	runner.Terminal = terminal
	// A service profile with its own terminal policy, and delegated execution
	// relaying a receiving service's refusal, also report typed causes.
	runner.RecordCause = terminal != nil || delegates != nil
	runner.Fetchers = download.NewFetchers(download.HTTP{})
	if delegates != nil {
		runner.Delegators = download.NewDelegators(delegates...)
	}
	runner.SharedStore = true
	// The sweep closes out finished work; each job is closed as it finishes,
	// so a later, larger job in the same sweep does not hold it back.
	runner.DeliverEach = true
	runner.Credentials = noExecutionCredentials{}
	if applier != nil {
		runner.Credentials = appliedCredentials{ctx: ctx, applier: applier, lookup: lookup}
	}
	runner.Network = network
	runner.Archive = attributed(archive, lookup)
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

type operationWake struct {
	lookup      acceptanceprovider.SubjectLookup
	operationID string
}

func (w operationWake) ForOperation(id string) job.Holder { w.operationID = id; return w }

func (w operationWake) Hold(who, why string) (job.Held, error) {
	if w.lookup == nil || w.operationID == "" {
		return nil, errors.New("download wake: authenticated job owner unavailable")
	}
	subject, found, err := w.lookup(w.operationID, "")
	if err != nil || !found || subject == nil {
		return nil, errors.New("download wake: authenticated job owner unavailable")
	}
	return (wake.Holder{Subject: &rights.Subject{Account: subject.Account, Program: subject.Program}}).Hold(who, why)
}

func (noExecutionCredentials) Lookup(string, string) (map[string]string, bool) { return nil, false }

// LookupSource answers a source naming a credential in a service with no
// applier: nothing can apply it now, and the attempt stays retryable.
func (noExecutionCredentials) LookupSource(src download.Source, _ string) (map[string]string, error) {
	if _, err := download.CredentialRoute(src); err != nil {
		return nil, err
	}
	return nil, download.CredentialRefusal(src.Attrs[download.CredentialAttr], "unavailable")
}

// appliedCredentials asks the applier for every request the runner sends to a
// source naming a credential, with the scope preparation recorded.
type appliedCredentials struct {
	ctx         context.Context
	applier     CredentialApplier
	lookup      acceptanceprovider.SubjectLookup
	operationID string
}

func (c appliedCredentials) ForOperation(operationID string) download.Credentials {
	c.operationID = operationID
	return c
}

func (appliedCredentials) Lookup(string, string) (map[string]string, bool) { return nil, false }

func (c appliedCredentials) LookupSource(src download.Source, host string) (map[string]string, error) {
	name, scope := src.Attrs[download.CredentialAttr], src.Attrs[download.CredentialScopeAttr]
	route, err := download.CredentialRoute(src)
	if err != nil {
		return nil, err
	}
	if scope == "" {
		return nil, download.CredentialRefusal(name, "forbidden")
	}
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	var headers map[string]string
	if route == ScopedCredentialGuarantee {
		if !scopedCredentialsAvailable(c.applier) || c.lookup == nil || c.operationID == "" {
			return nil, download.CredentialRefusal(name, "unavailable")
		}
		subject, found, lookupErr := c.lookup(c.operationID, scope)
		if lookupErr != nil || !found || subject == nil {
			return nil, download.CredentialRefusal(name, "unavailable")
		}
		headers, err = c.applier.(ScopedSubjectCredentialApplier).ApplyScopedSubjectCredential(ctx, *subject, name, host)
	} else if c.lookup != nil {
		if c.operationID == "" {
			return nil, download.CredentialRefusal(name, "unavailable")
		}
		subject, found, lookupErr := c.lookup(c.operationID, scope)
		if lookupErr != nil || !found {
			return nil, download.CredentialRefusal(name, "unavailable")
		}
		if subject != nil {
			bound, ok := c.applier.(SubjectCredentialApplier)
			if !ok {
				return nil, download.CredentialRefusal(name, "unavailable")
			}
			headers, err = bound.ApplySubjectCredential(ctx, *subject, name, host)
		} else {
			headers, err = c.applier.ApplyCredential(ctx, scope, name, host)
		}
	} else {
		headers, err = c.applier.ApplyCredential(ctx, scope, name, host)
	}
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
	return slices.Contains(required, CredentialGuarantee) || slices.Contains(required, ScopedCredentialGuarantee)
}

func credentialRouteFor(required []string) (string, error) {
	original := slices.Contains(required, CredentialGuarantee)
	scoped := slices.Contains(required, ScopedCredentialGuarantee)
	if original && scoped {
		return "", errors.New("a request requires exactly one credential route")
	}
	if scoped {
		return ScopedCredentialGuarantee, nil
	}
	if original {
		return CredentialGuarantee, nil
	}
	return "", nil
}

// attributed names, for the archive's bytes-written count, the program that
// submitted each operation: the job owner the provider authenticated. The
// name stays in the runtime's own state; no app reads it back.
func attributed(kept download.DownloadArchive, lookup acceptanceprovider.SubjectLookup) download.DownloadArchive {
	service, ok := kept.(downloadarchive.Service)
	if !ok || service.App != nil || lookup == nil {
		return kept
	}
	service.App = func(operation string) string {
		subject, found, err := lookup(operation, "")
		if err != nil || !found || subject == nil {
			return ""
		}
		return subject.Program
	}
	return service
}
