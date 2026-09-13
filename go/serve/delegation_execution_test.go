package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	job "github.com/openabstractions/abstraction-job/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// The external ledger survives provider and adapter reconstruction. Its state
// is independent of the acceptance root; no network or owner resources exist.
type ledgerDelegate struct {
	path, system string
	body         []byte
	unknown      bool
	starts       int
}
type ledgerEntry struct {
	Request, Handle string
	Effects         int
}

func (d *ledgerDelegate) Close() error    { return nil }
func (d *ledgerDelegate) System() string  { return d.system }
func (*ledgerDelegate) Schemes() []string { return []string{"https"} }
func (*ledgerDelegate) Capabilities() []download.Capability {
	return []download.Capability{download.CapDelegates, download.CapSurvivesProcessExit, download.CapRecoverableSubmission}
}
func (d *ledgerDelegate) read() (ledgerEntry, error) {
	var entry ledgerEntry
	b, e := os.ReadFile(d.path)
	if e == nil {
		e = json.Unmarshal(b, &entry)
	}
	return entry, e
}
func (d *ledgerDelegate) Start(_ context.Context, s download.Spec, _ int64) (string, error) {
	d.starts++
	entry, err := d.read()
	if os.IsNotExist(err) {
		entry = ledgerEntry{Request: s.Request, Handle: "external-" + s.Request, Effects: 1}
		b, _ := json.Marshal(entry)
		err = os.WriteFile(d.path, b, 0600)
	}
	if err != nil {
		return "", err
	}
	if entry.Request != s.Request {
		return "", errors.New("request mismatch")
	}
	return "", errors.New("external effect committed; reply lost")
}
func (d *ledgerDelegate) Locate(_ context.Context, key string) (string, error) {
	if d.unknown {
		return "", errors.New("external history unavailable")
	}
	entry, err := d.read()
	if err != nil {
		return "", err
	}
	if entry.Request != key {
		return "", errors.New("request mismatch")
	}
	return entry.Handle, nil
}
func (d *ledgerDelegate) Poll(_ context.Context, id string) (download.Status, error) {
	entry, err := d.read()
	if err != nil || entry.Handle != id {
		return download.Status{}, errors.New("unknown external operation")
	}
	return download.Status{State: download.DelegateTransferred, Done: int64(len(d.body)), Total: int64(len(d.body))}, nil
}
func (d *ledgerDelegate) Finalize(_ context.Context, id, dest string) error {
	entry, err := d.read()
	if err != nil || entry.Handle != id {
		return errors.New("wrong external operation")
	}
	if err = os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		return err
	}
	return os.WriteFile(dest, d.body, 0600)
}
func (*ledgerDelegate) Abandon(context.Context, string) error {
	return errors.New("unexpected abandonment")
}

type delegatedExecution struct {
	DelegatedExecution
	delegates []download.Delegator
}

func (*delegatedExecution) Profile() string { return "test-durable-delegation-v1" }
func (e *delegatedExecution) Serve(ctx context.Context, s job.Store) error {
	r := download.NewRunner(s, "worker-"+job.NewID())
	defer r.Close()
	r.Delegators = download.NewDelegators(e.delegates...)
	r.Fetchers = download.NewFetchers() // Any unintended local adoption fails visibly.
	r.SharedStore = true
	_, _, _, _, problems := Pass(ctx, r)
	return errors.Join(problems...)
}

func TestDelegationExecutionRecoversLostReplyAfterProviderRestart(t *testing.T) {
	body := bytes.Repeat([]byte("external durable result\n"), 4096)
	external := filepath.Join(t.TempDir(), "ledger.json")
	original := &ledgerDelegate{path: external, system: "retained-external", body: body, unknown: true}
	fallback := &ledgerDelegate{path: filepath.Join(t.TempDir(), "other.json"), system: "other-external", body: body}
	root := t.TempDir()
	p, err := acceptanceprovider.OpenWithExecutor(root, "logical-service", &delegatedExecution{delegates: []download.Delegator{original, fallback}})
	if err != nil {
		t.Fatal(err)
	}
	bound := p.Bind("authenticated-caller")
	history, err := bound.GetHistoryWindow()
	if err != nil {
		t.Fatal(err)
	}
	id := api.RequestIdentity{Key: "lost-external-reply", HistoryEpoch: history.HistoryEpoch}
	raw := request.Encode(&request.Request{Artifact: request.Artifact{Size: int64(len(body)), Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(body))}, Sources: []request.Source{{Scheme: "https", Locator: "https://unused.invalid/fixture"}}})
	accepted, err := bound.Submit(api.Submission{Identity: id, Kind: "download", Spec: raw, RequiredGuarantees: []string{acceptanceprovider.GuaranteeReconciliation, string(download.CapRecoverableSubmission)}})
	if err != nil || accepted.Receipt == nil {
		t.Fatal(accepted, err)
	}
	if !slices.Contains(accepted.Receipt.AcceptedGuarantees, string(download.CapRecoverableSubmission)) {
		t.Fatal("receipt omitted requested downstream guarantee")
	}
	for i := 0; i < 2; i++ {
		if err := p.Execute(context.Background()); err != nil && !errors.Is(err, download.ErrOutcomeUnknown) {
			t.Fatal(err)
		}
	}
	if original.starts != 1 || fallback.starts != 0 {
		t.Fatalf("unknown submission retried/switched: original=%d fallback=%d", original.starts, fallback.starts)
	}
	unknown, err := p.BindOperations("authenticated-caller").ObserveWork(id)
	if err != nil || unknown.Snapshot == nil || unknown.Snapshot.State != "running" {
		t.Fatal(unknown, err)
	}
	// New provider, runner and adapter instances know only their persisted roots.
	recovered := &ledgerDelegate{path: external, system: original.system, body: body, unknown: true}
	p, err = acceptanceprovider.OpenWithExecutor(root, "logical-service", &delegatedExecution{delegates: []download.Delegator{fallback, recovered}})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Execute(context.Background()); err == nil || !errors.Is(err, download.ErrOutcomeUnknown) {
		t.Fatalf("restarted unknown handoff: %v", err)
	}
	if recovered.starts != 0 || fallback.starts != 0 {
		t.Fatal("restart resubmitted uncertain work")
	}
	recovered.unknown = false
	for i := 0; i < 4; i++ {
		if err = p.Execute(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	result, err := p.Bind("authenticated-caller").Reconcile(id)
	if err != nil || result.Receipt == nil || result.Receipt.OperationId != accepted.Receipt.OperationId {
		t.Fatal(result, err)
	}
	observed, err := p.BindOperations("authenticated-caller").ObserveWork(id)
	if err != nil || observed.Snapshot == nil || observed.Snapshot.State != "complete" {
		t.Fatal(observed, err)
	}
	var got []byte
	for {
		part, err := p.BindOperations("authenticated-caller").ReadResult(id, int64(len(got)), 65536)
		if err != nil || part.Chunk == nil || part.Outcome != "data" {
			t.Fatal(part, err)
		}
		if part.Chunk.Receipt.OperationId != accepted.Receipt.OperationId {
			t.Fatal("receipt changed")
		}
		if part.Chunk.Offset != int64(len(got)) || part.Chunk.Total != int64(len(body)) || len(part.Chunk.Data) > 65536 || (!part.Chunk.Eof && len(part.Chunk.Data) == 0) {
			t.Fatal("result bounds changed")
		}
		got = append(got, part.Chunk.Data...)
		if part.Chunk.Eof {
			break
		}
	}
	entry, err := recovered.read()
	if err != nil || entry.Effects != 1 || original.starts != 1 || recovered.starts != 0 || fallback.starts != 0 || !bytes.Equal(got, body) {
		t.Fatalf("external execution changed: ledger=%+v err=%v starts=%d/%d/%d bytes=%d", entry, err, original.starts, recovered.starts, fallback.starts, len(got))
	}
}
