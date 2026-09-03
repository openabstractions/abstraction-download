// Package bits implements download.Delegator on top of Windows BITS, the
// Background Intelligent Transfer Service.
//
// BITS is the reference semantics the Delegator interface was designed against:
// an OS-owned, on-disk job queue with a GUID that any process running as the job
// owner can open, transfers that continue after the requesting process exits,
// automatic recovery from network loss and logoff, and a mandatory acknowledge
// step before the file becomes the caller's. Nothing in this package invents
// any of that. It drives what is already installed.
//
// # THE DEPENDENCY, STATED PLAINLY
//
// This binding shells out to powershell.exe and the BitsTransfer module. That
// is a dependency. It is not "zero dependencies because exec.Command is in the
// standard library" — this project has already thrown out one such claim, made
// by code that leaned on six undeclared binaries found on $PATH, and the claim
// was wrong then too. So, declared:
//
//   - powershell.exe must be on PATH (Windows PowerShell 5.1 or PowerShell 7+).
//   - The BitsTransfer module must be importable in that shell.
//   - The BITS service must be startable. It is demand-start on stock Windows,
//     so a Stopped service is normal and not a problem; Disabled is.
//
// Available() checks all three and returns a description of what is missing.
// Callers that cannot tolerate a missing dependency should call it at startup
// rather than discovering the gap at Start().
//
// # WHY THE CMDLETS AND NOT COM
//
// The production route is COM through github.com/go-ole/go-ole (MIT), which
// does COM via syscall rather than cgo. It was not taken here because the
// project's dependency policy is strict and this route adds no module to
// go.mod. That is a real trade, not a free one. What the cmdlet route gives up:
//
//   - IBackgroundCopyFile3::GetTemporaryName / SetValidationState. This is the
//     hook that lets you hash the in-flight file and tell BITS the verdict, so
//     that bytes served from a BranchCache peer get re-fetched from origin on a
//     bad digest. Without it, verification can only happen after Finalize, on a
//     file that is already at its final path. The Runner does exactly that, so
//     nothing is unsafe — it is just later and more expensive.
//   - IBackgroundCopyJob::TakeOwnership. An administrator cannot adopt another
//     user's job through these cmdlets. Get-BitsTransfer -AllUsers can list such
//     a job but Complete-BitsTransfer on it is unreliable, and the survey notes
//     that jobs owned by NT AUTHORITY accounts are awkward-to-impossible to
//     cancel from an admin shell. Consequently Poll here reports a job owned by
//     another user as DelegateGone, because to this process it is.
//   - IBackgroundCopyJob2::SetNotifyCmdLine wiring beyond what Start-BitsTransfer
//     exposes, and IBackgroundCopyCallback entirely (which needs a live process
//     pumping COM, so it was never the mechanism that composes here anyway).
//   - Per-file progress inside a multi-file job. Not used: one Spec is one file.
//   - Cheap polling. Every Poll is a process spawn, roughly 100-300 ms. Poll on
//     a timer measured in seconds, not milliseconds. COM polling is a vtable
//     call.
//
// # WHAT IS DELIBERATELY NOT DONE HERE
//
// Integrity. BITS "guarantees that the version of the file it transfers is
// consistent based on the file size and time stamp, not content (BITS does not
// protect against man-in-the-middle attacks)". The Runner hashes what Finalize
// delivered and refuses a mismatch. That belongs above every binding, not in
// the Windows one.
//
// Redirect resolution. BITS requires static content: the server's HEAD must
// return the size and its GET must honour Content-Range. Signed, expiring CDN
// URLs are the practical risk (see the note on Start). Resolving a redirect
// chain to a stable URL, if it ever becomes necessary, belongs in the facade.
package bits

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	download "github.com/ReinisLusis/abstraction-download"
)

// System is the value recorded in job.Delegation.System. It is what decides,
// possibly after a reboot and in another process, who can interpret a stored
// ExternalID.
const System = "bits"

// sizeUnknown is BG_SIZE_UNKNOWN, what BITS reports for BytesTotal before it
// has learned the length. It is 0xFFFFFFFFFFFFFFFF, so a naive conversion to a
// signed integer yields -1 and a naive progress bar yields 16 exabytes.
const sizeUnknown = ^uint64(0)

// Delegator hands downloads to BITS and watches them.
//
// The zero value is not usable; call New. It is safe for concurrent use: every
// operation is a fresh subprocess and the only shared state is the memoised
// availability probe.
type Delegator struct {
	// PowerShell is the executable to drive. Default "powershell.exe".
	PowerShell string

	// DisplayName is written into the BITS job so that a human running
	// Get-BitsTransfer can tell whose jobs these are. BITS shows it in its own
	// tooling; it has no effect on the transfer.
	DisplayName string

	// Priority is the BITS job priority: Foreground, High, Normal or Low. Empty
	// leaves the cmdlet's own default, which is Foreground — note that this is
	// NOT the BITS API default of Normal. Foreground opts out of the background
	// throttling that is the entire point of BITS, and is the right default for
	// a user waiting on a model; Normal or Low is right for a fleet agent.
	Priority string

	// Timeout bounds one PowerShell invocation. It does not bound the transfer,
	// which is the service's business and may run for days. Default 90s, which
	// is generous for a process spawn and allows for a cold BITS service start.
	Timeout time.Duration

	once     sync.Once
	availErr error
}

func New() *Delegator {
	return &Delegator{
		PowerShell:  "powershell.exe",
		DisplayName: "abstraction-download",
		Timeout:     90 * time.Second,
	}
}

// Compile-time proof that this is the interface the Runner will call. If the
// interface ever moves, this breaks here rather than at a registration site.
var _ download.Delegator = (*Delegator)(nil)

func (d *Delegator) System() string { return System }

// Schemes: BITS takes HTTP, HTTPS and SMB remote names in the same job type, so
// on Windows one binding covers the internet case and the NAS case. A UNC path
// is given to Start-BitsTransfer verbatim.
func (d *Delegator) Schemes() []string { return []string{"http", "https", "smb"} }

// Capabilities. CapSurvivesProcessExit is the one that matters and the one BITS
// actually has: "BITS continues to transfer files after an application exits as
// long as the user who initiated the transfer remains logged on and a network
// connection is maintained."
//
// CapVerifies is deliberately absent. BITS checks size and timestamp, not
// content, and claiming otherwise would tell the Runner it may skip the hash.
func (d *Delegator) Capabilities() []download.Capability {
	return []download.Capability{
		// NOT CapResume, though this claimed it for a long time.
		//
		// CapResume means "can start from a byte offset rather than from zero",
		// and BITS cannot. Start-BitsTransfer owns its own temporary file and
		// there is no way to hand it a partial somebody else wrote; Start
		// accepts a `from` argument here and has always ignored it. Measured: a
		// job handed over with 6,586,368 bytes proven produced a GET with no
		// Range header and re-fetched the lot.
		//
		// A false capability is worse than a missing one, because Requires
		// exists so that callers can trust these.
		download.CapSurvivesProcessExit,
		download.CapDelegates,
	}
}

// ErrUnavailable is the reason BITS cannot be driven on this machine.
var ErrUnavailable = errors.New("bits: unavailable")

// Available reports whether this machine can actually run a BITS job through
// the cmdlets, and says what is wrong if not. The result is memoised: it probes
// the environment, which does not change under a running process, and the probe
// costs a process spawn.
//
// Tests use this to skip rather than fail, which is the only correct behaviour
// for a suite that also runs on Linux and on Windows images without BITS.
func (d *Delegator) Available() error {
	d.once.Do(func() { d.availErr = d.probe() })
	return d.availErr
}

func (d *Delegator) probe() error {
	if runtime.GOOS != "windows" {
		return fmt.Errorf("%w: BITS is a Windows service and this is %s", ErrUnavailable, runtime.GOOS)
	}
	if _, err := exec.LookPath(d.shell()); err != nil {
		return fmt.Errorf("%w: %s not found on PATH: %v", ErrUnavailable, d.shell(), err)
	}
	// Importing the module is not enough on its own — it imports fine on an
	// image where the BITS service is disabled. Enumerating jobs actually talks
	// to the service, so it fails where a transfer would fail.
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout())
	defer cancel()
	script := `Get-BitsTransfer -ErrorAction Stop | Out-Null
[Console]::Out.Write('ok')`
	if _, err := d.run(ctx, script, nil); err != nil {
		return fmt.Errorf("%w: the BitsTransfer module could not reach the BITS service: %v", ErrUnavailable, err)
	}
	return nil
}

// Start creates a suspended BITS job, adds the one file, resumes it, and
// returns the job GUID. It does not wait for any bytes: the transfer is
// expected to outlive this call and usually this process.
//
// The job is created suspended and resumed as a separate step rather than let
// Start-BitsTransfer do both, so that a failure to add the file leaves nothing
// running — Start-BitsTransfer's own error path can leave a job behind, and a
// job nobody holds a GUID for is a leak until the 90-day reaper.
//
// # from IS IGNORED, AND THAT IS NOT A BUG
//
// The Runner's `from` is an offset into spec.Sink.Partial: how many leading
// bytes of the working file are proven. BITS never touches that file. It writes
// to spec.Sink.Final through a temporary of its own choosing, and it does its
// own resume, from its own state, using byte ranges it negotiates itself. There
// is no way to tell it "begin at byte N of a file you did not write", through
// the cmdlets or through COM. So handing a partially-fetched job to BITS starts
// the transfer over. That is correct, and it is the honest cost of delegating:
// BITS re-fetches, and what it delivers is a whole file rather than a suffix
// appended to bytes some other implementation left behind.
//
// # THE CDN CAVEAT
//
// BITS requires static content — "the HTTP server's Head method must return the
// file size and its Get method must support the Content-Range and
// Content-Length headers ... BITS only transfers static file content and
// generates an error if you try to transfer dynamic content". A signed,
// expiring, redirecting CDN URL of the HuggingFace or S3 kind satisfies this
// while the signature is valid: BITS follows the 302 (its default SecurityFlags
// include RedirectPolicyAllowSilent) and the CDN answers with Accept-Ranges and
// a Content-Length. Measured, not assumed: see TestSignedRedirectingCDN.
//
// The part that is still untested, and the part that will bite: the signature
// on that redirect was measured at roughly ONE HOUR of validity, not a day. A
// 40 GB model on a domestic link does not finish in an hour, and BITS is
// deliberately slow. Whether BITS re-requests the original huggingface.co URL —
// and so picks up a fresh signature — or retries the resolved CDN URL it was
// redirected to, is not documented and was not tested here. If it is the
// latter, the symptom is BG_JOB_STATE_ERROR with an HTTP 403 partway through a
// long download; Poll reports that as DelegateFailed with the HTTP status
// attached, and the Runner answers it by taking the work back in-process
// against the original source, which does still work. So the failure mode is
// slow and wasteful rather than silent or lossy — but it is a real reason a
// facade might resolve the redirect chain itself, or refuse to delegate a
// transfer it expects to outlive the signature.
func (d *Delegator) Start(ctx context.Context, spec download.Spec, from int64) (string, error) {
	if err := d.Available(); err != nil {
		return "", err
	}
	if len(spec.Sources) == 0 {
		return "", errors.New("bits: spec has no source")
	}
	src := spec.Sources[0]
	if strings.TrimSpace(spec.Sink.Final) == "" {
		return "", errors.New("bits: spec has no final path")
	}
	dest, err := filepath.Abs(spec.Sink.Final)
	if err != nil {
		return "", err
	}
	// BITS will not create the destination directory, and the failure it gives
	// for a missing one arrives asynchronously in the job's error state rather
	// than from the create call.
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return "", err
	}

	env := map[string]string{
		"ABSTRACTION_BITS_SOURCE":  src.Locator,
		"ABSTRACTION_BITS_DEST":    dest,
		"ABSTRACTION_BITS_DISPLAY": d.displayName(),
	}
	priority := ""
	if p := strings.TrimSpace(d.Priority); p != "" {
		env["ABSTRACTION_BITS_PRIORITY"] = p
		priority = " -Priority $env:ABSTRACTION_BITS_PRIORITY"
	}

	// Every value crosses into PowerShell as an environment variable rather
	// than as text interpolated into the script. A locator is attacker-shaped
	// data: it comes from a job record that some other process wrote. There is
	// no quoting rule for PowerShell that is worth trusting with it.
	script := `$job = Start-BitsTransfer ` +
		`-Source $env:ABSTRACTION_BITS_SOURCE ` +
		`-Destination $env:ABSTRACTION_BITS_DEST ` +
		`-DisplayName $env:ABSTRACTION_BITS_DISPLAY` + priority + ` ` +
		`-Asynchronous -Suspended -ErrorAction Stop
try {
    Resume-BitsTransfer -BitsJob $job -Asynchronous -ErrorAction Stop | Out-Null
} catch {
    Remove-BitsTransfer -BitsJob $job -ErrorAction SilentlyContinue
    throw
}
[Console]::Out.Write((@{ jobId = $job.JobId.ToString() } | ConvertTo-Json -Compress))`

	out, err := d.run(ctx, script, env)
	if err != nil {
		return "", fmt.Errorf("bits: creating a job for %s: %w", src.Locator, err)
	}
	var res struct {
		JobID string `json:"jobId"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		return "", fmt.Errorf("bits: could not read the job id out of %q: %w", out, err)
	}
	if !isGUID(res.JobID) {
		return "", fmt.Errorf("bits: %q is not a job guid", res.JobID)
	}
	return res.JobID, nil
}

// pollResult is the shape the poll script emits. Byte counts cross as decimal
// strings on purpose: BG_SIZE_UNKNOWN is 0xFFFFFFFFFFFFFFFF, which is a valid
// PowerShell UInt64 and an overflow in every JSON decoder that assumes int64.
type pollResult struct {
	Gone         bool   `json:"gone"`
	State        string `json:"state"`
	Done         string `json:"done"`
	Total        string `json:"total"`
	Err          string `json:"err"`
	ErrCondition string `json:"errCondition"`
	ErrContext   string `json:"errContext"`
}

// Poll asks BITS how a job is doing. Any process running as the job owner may
// call it, including one that never started anything — that is the property a
// COM callback cannot have, because a callback dies with the process that
// registered it.
//
// A handle BITS does not know is DelegateGone, never an error. Jobs are reaped
// after 90 days, the queue database is discarded wholesale when it is found
// corrupt, machines get rebuilt, and Cancel/Complete both remove the job. The
// Runner answers Gone by taking the work back in-process; an error would strand
// the job instead, which is strictly worse than redoing it.
func (d *Delegator) Poll(ctx context.Context, externalID string) (download.Status, error) {
	if err := d.Available(); err != nil {
		return download.Status{}, err
	}
	id, ok := normalizeGUID(externalID)
	if !ok {
		// Not a GUID at all, so no BITS job can ever have it. Gone, for the same
		// reason an unknown GUID is gone: there is nothing to wait for and the
		// Runner can still do the work itself.
		return download.Status{State: download.DelegateGone}, nil
	}

	// The inner catch is narrowed to the one error that means "no such job".
	// Widening it would turn a BITS service outage into DelegateGone, and the
	// Runner would respond by clearing a delegation whose transfer is in fact
	// still queued on disk — losing the handle to a job that is still running.
	script := `try {
    $job = Get-BitsTransfer -JobId $env:ABSTRACTION_BITS_JOBID -ErrorAction Stop
} catch {
    if ($_.FullyQualifiedErrorId -like 'GetBitsTransferInvalidIdError*') {
        [Console]::Out.Write('{"gone":true}')
        exit 0
    }
    throw
}
[Console]::Out.Write((@{
    state        = $job.JobState.ToString()
    done         = [string]$job.BytesTransferred
    total        = [string]$job.BytesTotal
    err          = [string]$job.ErrorDescription
    errCondition = [string]$job.ErrorCondition
    errContext   = [string]$job.ErrorContextDescription
} | ConvertTo-Json -Compress))`

	out, err := d.run(ctx, script, map[string]string{"ABSTRACTION_BITS_JOBID": id})
	if err != nil {
		return download.Status{}, fmt.Errorf("bits: polling %s: %w", id, err)
	}
	var pr pollResult
	if err := json.Unmarshal([]byte(out), &pr); err != nil {
		return download.Status{}, fmt.Errorf("bits: could not read a poll answer out of %q: %w", out, err)
	}
	if pr.Gone {
		return download.Status{State: download.DelegateGone}, nil
	}

	st := download.Status{
		State:     stateOf(pr.State),
		Done:      bytesOf(pr.Done),
		Total:     bytesOf(pr.Total),
		Suspended: strings.EqualFold(strings.TrimSpace(pr.State), "suspended"),
	}
	if st.State == download.DelegateFailed {
		st.Err = failureText(pr)
	}
	return st, nil
}

// stateOf maps BG_JOB_STATE onto the four states the Runner acts on. The
// PowerShell BitsJobState enum has the same members and the same ordinals as
// the COM BG_JOB_STATE enum, so this mapping is the COM mapping.
func stateOf(name string) download.DelegateState {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "queued", "connecting", "transferring":
		return download.DelegateRunning

	case "suspended":
		// BITS suspends a job when its owner logs off and resumes it when they
		// log back on. It is also the state a job sits in when the cmdlets were
		// used from a non-interactive context and could not add files. Either
		// way it is not a failure and not a finish.
		return download.DelegateRunning

	case "transienterror":
		// The single most important line in this function. BITS recovers from
		// these by itself — "when a network connection is established, all jobs
		// in a BG_JOB_STATE_TRANSIENT_ERROR state ... are moved to the
		// BG_JOB_STATE_QUEUED state" — and its retry policy is measured in
		// hours. Reporting it as failure would make the Runner tear down a
		// transfer that is about to continue and throw away every byte of a
		// possibly multi-gigabyte download.
		return download.DelegateRunning

	case "transferred":
		// All the bytes are here. The file is NOT the caller's and does not
		// exist at its final path: "the transferred files are not available to
		// the client until the application calls the
		// IBackgroundCopyJob::Complete method". Finalize is what changes that.
		// This is precisely why DelegateTransferred is not DelegateComplete.
		return download.DelegateTransferred

	case "error":
		return download.DelegateFailed

	case "canceled", "cancelled":
		// Someone cancelled it, which deleted the partial and any completed
		// file with it. Failed rather than Gone: BITS does still know the
		// handle, and the Runner records the reason before retrying in-process.
		return download.DelegateFailed

	case "acknowledged":
		// Complete() has been called and the job is on its way out of the
		// queue. Reporting Transferred would ask the Runner to Finalize an
		// already finalised job, which cannot succeed. Gone is the same answer
		// this returns a moment later, once BITS has actually dropped the job
		// and Get-BitsTransfer stops finding it — so it is the consistent one.
		return download.DelegateGone
	}
	// An unrecognised state is not a licence to guess. Running is the only
	// answer that neither discards work nor claims bytes that may not exist.
	return download.DelegateRunning
}

// bytesOf turns a BITS byte count into an int64, mapping BG_SIZE_UNKNOWN to 0.
//
// 0 is what Spec and Status already mean by "unknown", and Runner.Reconcile
// only overwrites Progress.Total when the value is above zero. Passing the raw
// 0xFFFFFFFFFFFFFFFF through would put 18446744073709551615 — or, signed, -1 —
// into a progress record that other languages read.
func bytesOf(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	if n == sizeUnknown || n > uint64(math.MaxInt64) {
		return 0
	}
	return int64(n)
}

// failureText assembles something a human can act on. BITS splits the reason
// across three properties and any one of them alone tends to be useless:
// ErrorDescription is the HRESULT text, ErrorCondition says which side failed,
// and ErrorContextDescription says which phase it failed in.
func failureText(pr pollResult) string {
	parts := make([]string, 0, 3)
	for _, p := range []string{pr.Err, pr.ErrContext} {
		if p = strings.TrimSpace(p); p != "" {
			parts = append(parts, p)
		}
	}
	if c := strings.TrimSpace(pr.ErrCondition); c != "" && !strings.EqualFold(c, "NoError") {
		parts = append(parts, "condition "+c)
	}
	if len(parts) == 0 {
		return "the BITS job is in state " + pr.State + " and reported no reason"
	}
	return "bits: " + strings.Join(parts, "; ")
}

// Finalize is BITS Complete(). This is the call that makes the file the
// caller's: until it happens the bytes belong to the service, the final path
// holds nothing, and the job sits in the queue until the 90-day reaper takes
// it. Complete keeps the completed file and deletes any partial.
//
// It is not idempotent, and cannot be: after it succeeds the job is gone from
// the queue, so a second call cannot tell "already delivered" from "never
// existed". The Runner records Delegation.Delivered for exactly this reason.
//
// dest is ignored: BITS was told the destination at Start and has been holding
// the file there ever since. Complete() moves it into place, and pointing it
// somewhere else at this stage is not a thing the API offers.
func (d *Delegator) Finalize(ctx context.Context, externalID, dest string) error {
	if err := d.Available(); err != nil {
		return err
	}
	id, ok := normalizeGUID(externalID)
	if !ok {
		return fmt.Errorf("bits: %q is not a job guid", externalID)
	}
	script := `$job = Get-BitsTransfer -JobId $env:ABSTRACTION_BITS_JOBID -ErrorAction Stop
Complete-BitsTransfer -BitsJob $job -ErrorAction Stop
[Console]::Out.Write('ok')`
	if _, err := d.run(ctx, script, map[string]string{"ABSTRACTION_BITS_JOBID": id}); err != nil {
		return fmt.Errorf("bits: completing %s: %w", id, err)
	}
	return nil
}

// Abandon is BITS Cancel(). It removes the job from the queue and deletes what
// it had fetched — including a file that had finished transferring but was
// never completed. This is not a way to keep what has arrived so far; there
// isn't one.
//
// A job that is already gone is a success, not an error. Abandon is called from
// the Runner's recovery path, where the whole point is to leave nothing behind,
// and a handle that no longer resolves has already achieved that.
func (d *Delegator) Abandon(ctx context.Context, externalID string) error {
	if err := d.Available(); err != nil {
		return err
	}
	id, ok := normalizeGUID(externalID)
	if !ok {
		return nil // nothing that could exist, so nothing to cancel
	}
	script := `try {
    $job = Get-BitsTransfer -JobId $env:ABSTRACTION_BITS_JOBID -ErrorAction Stop
} catch {
    if ($_.FullyQualifiedErrorId -like 'GetBitsTransferInvalidIdError*') {
        [Console]::Out.Write('gone')
        exit 0
    }
    throw
}
Remove-BitsTransfer -BitsJob $job -ErrorAction Stop
[Console]::Out.Write('ok')`
	if _, err := d.run(ctx, script, map[string]string{"ABSTRACTION_BITS_JOBID": id}); err != nil {
		return fmt.Errorf("bits: cancelling %s: %w", id, err)
	}
	return nil
}

// ---------- driving powershell ----------

func (d *Delegator) shell() string {
	if s := strings.TrimSpace(d.PowerShell); s != "" {
		return s
	}
	return "powershell.exe"
}

func (d *Delegator) displayName() string {
	if s := strings.TrimSpace(d.DisplayName); s != "" {
		return s
	}
	return "abstraction-download"
}

func (d *Delegator) timeout() time.Duration {
	if d.Timeout > 0 {
		return d.Timeout
	}
	return 90 * time.Second
}

// run executes one script and returns its stdout.
//
// The script is passed as -EncodedCommand: UTF-16LE, base64. That is not
// decoration. A -Command string has to survive Go's argv assembly, Windows's
// CommandLineToArgvW, and PowerShell's own parser, and every layer has
// different rules about quotes and backticks. Encoding removes all three.
// Parameters go through the environment for the same reason.
func (d *Delegator) run(ctx context.Context, body string, env map[string]string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout())
	defer cancel()

	// -NonInteractive so a prompt fails instead of hanging forever on a machine
	// with nobody at the keyboard, and -NoProfile so a user's profile cannot
	// change the meaning of the script or slow every poll down by a second.
	full := `$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
try {
    Import-Module BitsTransfer -ErrorAction Stop
` + body + `
} catch {
    [Console]::Error.Write($_.Exception.Message + ' [' + $_.FullyQualifiedErrorId + ']')
    exit 1
}`

	cmd := exec.CommandContext(ctx, d.shell(),
		"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
		"-EncodedCommand", encodeCommand(full))
	if len(env) > 0 {
		e := os.Environ()
		for k, v := range env {
			e = append(e, k+"="+v)
		}
		cmd.Env = e
	}
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	out := strings.TrimSpace(stdout.String())
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = out
		}
		if ctx.Err() != nil {
			return out, fmt.Errorf("powershell timed out after %s: %v: %s", d.timeout(), err, msg)
		}
		if msg == "" {
			return out, err
		}
		return out, fmt.Errorf("%v: %s", err, msg)
	}
	return out, nil
}

// encodeCommand produces the UTF-16LE base64 that -EncodedCommand expects.
func encodeCommand(s string) string {
	u := utf16.Encode([]rune(s))
	b := make([]byte, 0, len(u)*2)
	for _, r := range u {
		b = append(b, byte(r), byte(r>>8))
	}
	return base64.StdEncoding.EncodeToString(b)
}

// ---------- handles ----------

var guidRe = regexp.MustCompile(`^\{?[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\}?$`)

// normalizeGUID accepts a job handle with or without the braces that bitsadmin
// and the COM API print, and returns the bare form the cmdlets take. Both forms
// appear in the wild and a handle read out of a job record may have been
// written by another implementation, in another language, years earlier.
func normalizeGUID(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if !guidRe.MatchString(s) {
		return "", false
	}
	return strings.Trim(s, "{}"), true
}

func isGUID(s string) bool {
	_, ok := normalizeGUID(s)
	return ok
}

// Suspend and Resume make BITS an implementation of download.Suspendable.
//
// This is the capability the pause button needs on Windows, and BITS has had it
// all along: Start already creates every job suspended and resumes it as a
// separate step, so nothing new is being asked of it. What was missing was
// anything above connecting a person's request to it — the job layer honoured
// intent only on transfers this process was performing itself, which on Windows
// is the tier that runs when BITS is unavailable.
//
// A handle that no longer resolves is not an error here. BITS reaps jobs, queue
// databases are replaced, machines get rebuilt; a job that is gone is already
// as suspended as it will ever be, and failing would turn a normal outcome into
// an alarm.
func (d *Delegator) Suspend(ctx context.Context, externalID string) error {
	return d.setPaused(ctx, externalID, true)
}

func (d *Delegator) Resume(ctx context.Context, externalID string) error {
	return d.setPaused(ctx, externalID, false)
}

func (d *Delegator) setPaused(ctx context.Context, externalID string, paused bool) error {
	if err := d.Available(); err != nil {
		return err
	}
	id, ok := normalizeGUID(externalID)
	if !ok {
		return nil
	}
	verb := "Resume-BitsTransfer -BitsJob $job -Asynchronous"
	what := "resuming"
	if paused {
		verb = "Suspend-BitsTransfer -BitsJob $job"
		what = "suspending"
	}
	script := `try {
    $job = Get-BitsTransfer -JobId $env:ABSTRACTION_BITS_JOBID -ErrorAction Stop
} catch {
    if ($_.FullyQualifiedErrorId -like 'GetBitsTransferInvalidIdError*') {
        [Console]::Out.Write('gone')
        exit 0
    }
    throw
}
` + verb + ` -ErrorAction Stop | Out-Null
[Console]::Out.Write('ok')`
	if _, err := d.run(ctx, script, map[string]string{"ABSTRACTION_BITS_JOBID": id}); err != nil {
		return fmt.Errorf("bits: %s %s: %w", what, id, err)
	}
	return nil
}

// Compile-time proof that the pause button has somewhere to land on Windows.
var _ download.Suspendable = (*Delegator)(nil)
