package serve

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// RemoteJobSystem prefixes the job.Delegation.System of work handed to a remote
// OpenAbstractions job service.
const RemoteJobSystem = "openabstractions-remote-job"

// RemoteJobDelegator hands download work to a remote OpenAbstractions job
// service, whose own executor fetches the bytes [DL-K3]. The submission
// carries the generated request with each source's credential name and no
// secret, header or caller scope. The remote service decides with its own
// applier, in the scope its host maps from this machine's client certificate,
// and refuses at its own admission a name that scope may not apply. Delivery
// reads the remote result bytes into the local destination, where the runner
// verifies them.
//
// Transport is a frame exchanger reaching the remote service, normally the
// mutually authenticated TLS client of abstraction-identity/remote. It is safe
// for concurrent calls.
type RemoteJobDelegator struct {
	// Name distinguishes one remote service from another in records.
	Name      string
	Transport api.FrameExchanger

	// refused remembers, by request, a definite refusal the remote returned
	// at admission; see Refusal.
	refused sync.Map
}

var _ download.Delegator = (*RemoteJobDelegator)(nil)
var _ download.Locator = (*RemoteJobDelegator)(nil)
var _ download.SubmissionRefuser = (*RemoteJobDelegator)(nil)

func (d *RemoteJobDelegator) Close() error { return nil }

func (d *RemoteJobDelegator) System() string { return RemoteJobSystem + ":" + d.Name }

func (d *RemoteJobDelegator) Schemes() []string { return []string{"http", "https"} }

// Capabilities: the remote work survives this process, is found again by its
// request identity, and applies credentials by name on the remote side.
func (d *RemoteJobDelegator) Capabilities() []download.Capability {
	return []download.Capability{download.CapSurvivesProcessExit, download.CapRecoverableSubmission, download.Capability(CredentialGuarantee)}
}

// handle is the external identity of one remote submission: the remote
// history epoch and the request key, which is the local record id.
func handle(epoch, key string) string { return epoch + "/" + key }

func parseHandle(external string) (api.RequestIdentity, error) {
	epoch, key, ok := strings.Cut(external, "/")
	if !ok || epoch == "" || key == "" || strings.Contains(key, "/") {
		return api.RequestIdentity{}, errors.New("remote job: malformed handle")
	}
	return api.RequestIdentity{Key: key, HistoryEpoch: epoch}, nil
}

// remoteRequest is the generated request for one prepared source: the
// artifact, the locator and the credential name. Caller-supplied headers have
// no place in a remote request and refuse the handoff.
func remoteRequest(spec download.Spec) ([]byte, []string, error) {
	out := request.Request{Artifact: request.Artifact{Digest: spec.Artifact.Digest, Size: spec.Artifact.Size}}
	required := []string{acceptanceprovider.GuaranteeReconciliation}
	named := false
	for _, src := range spec.Sources {
		if len(src.Headers) != 0 {
			return nil, nil, errors.New("remote job: a source with request headers cannot be delegated")
		}
		source := request.Source{Scheme: src.Scheme, Locator: src.Locator, Credential: src.Attrs[download.CredentialAttr]}
		named = named || source.Credential != ""
		out.Sources = append(out.Sources, source)
	}
	if named {
		required = append(required, CredentialGuarantee)
	}
	return request.Encode(&out), required, nil
}

// Start submits the request under the local record id. A definite refusal is
// remembered for Refusal and returned; an uncertain reply is returned for
// Locate to settle.
func (d *RemoteJobDelegator) Start(ctx context.Context, spec download.Spec, from int64) (string, error) {
	if spec.Request == "" {
		return "", errors.New("remote job: request identity required")
	}
	raw, required, err := remoteRequest(spec)
	if err != nil {
		return "", err
	}
	acceptance := api.NewRecoverableAcceptanceClient(d.Transport)
	window, err := acceptance.GetHistoryWindow()
	if err != nil {
		return "", fmt.Errorf("remote job: history window: %w", err)
	}
	id := api.RequestIdentity{Key: spec.Request, HistoryEpoch: window.HistoryEpoch}
	result, err := acceptance.Submit(api.Submission{Identity: id, Kind: download.Kind, Spec: raw, RequiredGuarantees: required})
	if err != nil {
		return "", fmt.Errorf("remote job: submit: %w", err)
	}
	switch result.Outcome {
	case api.AcceptanceOutcomeAccepted:
		d.refused.Delete(spec.Request)
		return handle(id.HistoryEpoch, id.Key), nil
	case api.AcceptanceOutcomeUnknown:
		return "", fmt.Errorf("remote job: acceptance unknown: %s", result.Reason)
	}
	refusal := remoteRefusal(result)
	d.refused.Store(spec.Request, refusal)
	return "", refusal
}

// remoteCredential finds credential:<outcome>:<name> in a remote reason.
var remoteCredential = regexp.MustCompile(`^credential:([a-z_]{1,32}):([A-Za-z0-9_.-]{1,64})$`)

// remoteRefusal is the remote admission's answer as this layer's error. A
// credential reason keeps its outcome and name unchanged, so the local record
// reads cause credential; unavailable stays retryable.
func remoteRefusal(result api.AcceptanceResult) error {
	if word := remoteCredential.FindStringSubmatch(result.Reason); word != nil {
		return download.CredentialRefusal(word[2], word[1])
	}
	err := fmt.Errorf("remote job refused: %s: %s", result.Outcome, result.Reason)
	if result.Outcome == api.AcceptanceOutcomeUnavailable {
		return err
	}
	return download.Terminal(err)
}

// Refusal reports the remote admission's definite refusal of request, so
// recoverable work nothing else can take ends with the remote's reason.
func (d *RemoteJobDelegator) Refusal(request string) error {
	if v, ok := d.refused.Load(request); ok {
		return v.(error)
	}
	return nil
}

// Locate settles a Start whose reply was lost. A refusal Start received is
// certain non-acceptance and reads "", nil without asking; otherwise the remote
// reconciles the identity, which seals an absent one [JOB-A4].
func (d *RemoteJobDelegator) Locate(ctx context.Context, requestID string) (string, error) {
	if _, ok := d.refused.Load(requestID); ok {
		return "", nil
	}
	acceptance := api.NewRecoverableAcceptanceClient(d.Transport)
	window, err := acceptance.GetHistoryWindow()
	if err != nil {
		return "", err
	}
	id := api.RequestIdentity{Key: requestID, HistoryEpoch: window.HistoryEpoch}
	result, err := acceptance.Reconcile(id)
	if err != nil {
		return "", err
	}
	switch result.Outcome {
	case api.AcceptanceOutcomeAccepted:
		return handle(id.HistoryEpoch, id.Key), nil
	case api.AcceptanceOutcomeDefinitelyNotAccepted:
		return "", nil
	}
	return "", fmt.Errorf("remote job: reconcile %s: %s", result.Outcome, result.Reason)
}

// Poll observes the remote operation. The remote failure message, which names
// no path, travels as the delegate's error with its class.
func (d *RemoteJobDelegator) Poll(ctx context.Context, external string) (download.Status, error) {
	id, err := parseHandle(external)
	if err != nil {
		return download.Status{}, err
	}
	observed, err := api.NewOperationControlClient(d.Transport).ObserveWork(id)
	if err != nil {
		return download.Status{}, err
	}
	switch observed.Outcome {
	case api.ObservationOutcomeObserved:
	case api.ObservationOutcomeUnknown, api.ObservationOutcomeDefinitelyNotAccepted:
		return download.Status{State: download.DelegateGone}, nil
	default:
		return download.Status{}, fmt.Errorf("remote job: observe %s", observed.Outcome)
	}
	s := observed.Snapshot
	status := download.Status{Done: s.Progress.Done, Total: s.Progress.Total}
	switch s.State {
	case api.WorkStateComplete:
		status.State = download.DelegateTransferred
	case api.WorkStateFailed, api.WorkStateCancelled:
		status.State = download.DelegateFailed
		status.Err = "remote job " + s.State.String()
		if s.Failure != nil {
			status.Err = s.Failure.Message
			status.Permanent = s.Failure.Classification == api.FailureClassPermanent
		}
	default:
		status.State = download.DelegateRunning
	}
	return status, nil
}

// Finalize reads the remote result into dest.
func (d *RemoteJobDelegator) Finalize(ctx context.Context, external, dest string) error {
	return d.FinalizeReporting(ctx, external, dest, nil)
}

// FinalizeReporting reads the remote result into dest in bounded chunks.
func (d *RemoteJobDelegator) FinalizeReporting(ctx context.Context, external, dest string, report func(done, total int64)) error {
	id, err := parseHandle(external)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	ops := api.NewOperationControlClient(d.Transport)
	var offset int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		read, err := ops.ReadResult(id, offset, acceptanceprovider.MaxResultBytes)
		if err != nil {
			return err
		}
		if read.Outcome != api.ResultOutcomeData || read.Chunk == nil || read.Chunk.Offset != offset {
			return fmt.Errorf("remote job: result read %s at %d", read.Outcome, offset)
		}
		if _, err := f.Write(read.Chunk.Data); err != nil {
			return err
		}
		offset += int64(len(read.Chunk.Data))
		if report != nil {
			report(offset, read.Chunk.Total)
		}
		if read.Chunk.EOF {
			return f.Sync()
		}
	}
}

// Abandon records cancellation intent on the remote operation.
func (d *RemoteJobDelegator) Abandon(ctx context.Context, external string) error {
	id, err := parseHandle(external)
	if err != nil {
		return err
	}
	result, err := api.NewRecoverableAcceptanceClient(d.Transport).CancelWork(id)
	if err != nil {
		return err
	}
	switch result.Outcome {
	case api.CancellationOutcomeRequested, api.CancellationOutcomeAlreadyTerminal, api.CancellationOutcomeUnknown:
		return nil
	}
	return fmt.Errorf("remote job: cancel %s", result.Outcome.String())
}
