package serve

import (
	"errors"
	"io"
	"os"
	"regexp"
	"strings"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// credentialWord finds the credential:<outcome>:<name> word a credential
// failure records, where the applier's error writes it
// (download.CredentialError: "download: credential:<outcome>:<name>"): at
// the end of the recorded error, after any wrapping. The same text anywhere
// else in the error, a source's own message included, is not the word.
var credentialWord = regexp.MustCompile(`(?:^|: )download: (credential:[a-z_]{1,32}:[A-Za-z0-9_.-]{1,64})$`)

// OperationWaiting reports the word a waiting attempt records [DL-N5, JOB-A15].
func (HTTPExecution) OperationWaiting(record *job.Record) string {
	return download.Waiting(record)
}

// OperationFailure reports the recorded class, checked against the record
// state [JOB-A8], and the typed cause. Messages carry no locator or path.
// RefusedByRights reports a failed download that ended only because a rights
// decision refused it: the applier refused its credential's use with
// not_permitted (abstraction-job JOB-A19). The same request submitted again
// is then decided again rather than given this failure.
func (HTTPExecution) RefusedByRights(record *job.Record) bool {
	if record.State != job.StateFailed {
		return false
	}
	raw, ok := record.Extensions[download.FailureExtension]
	if !ok {
		return false
	}
	failure, err := download.DecodeFailure(raw)
	if err != nil || download.LastFailureCause(record) != string(api.FailureCauseCredential) {
		return false
	}
	word := credentialWord.FindStringSubmatch(failure.Error)
	return word != nil && strings.HasPrefix(word[1], "credential:not_permitted:")
}

func (HTTPExecution) OperationFailure(record *job.Record) *api.WorkFailure {
	if raw, ok := record.Extensions[download.FailureExtension]; ok {
		if failure, err := download.DecodeFailure(raw); err == nil {
			class := api.FailureClassUnknown
			switch terminal := record.State == job.StateFailed; {
			case terminal && failure.Permanent:
				class = api.FailureClassPermanent
			case !terminal && !failure.Permanent && !record.State.Terminal():
				class = api.FailureClassRetryable
			}
			cause := api.FailureCause(download.LastFailureCause(record))
			if cause != "" && !cause.Known() {
				cause = api.FailureCauseOther
			}
			message := "download attempt failed"
			if cause != "" {
				message += ": " + strings.ReplaceAll(string(cause), "_", " ")
			}
			// A credential failure names the applier's outcome and the credential,
			// never a secret: credential:<outcome>:<name>.
			if word := credentialWord.FindStringSubmatch(failure.Error); cause == api.FailureCauseCredential && word != nil {
				message = "download attempt failed: " + word[1]
			}
			return &api.WorkFailure{Classification: class, Message: message, Cause: cause}
		}
	}
	if record.Error != "" || record.State == job.StateFailed {
		return &api.WorkFailure{Classification: api.FailureClassUnknown, Message: "download reported an unclassified failure"}
	}
	return nil
}

// ResultRetentionMs declares the acceptance history window as the minimum time
// result files stay readable [JOB-A11]. HTTP execution removes no result file
// itself; a loss inside this window is damage to the service's storage.
func (HTTPExecution) ResultRetentionMs() int64 { return acceptanceprovider.MinimumRetentionMs }

func (HTTPExecution) ReadOperationResult(root string, record *job.Record, offset, maxBytes int64) ([]byte, int64, error) {
	if offset < 0 || maxBytes < 1 || maxBytes > acceptanceprovider.MaxResultBytes {
		return nil, 0, acceptanceprovider.ErrResultRange
	}
	if record.State != job.StateComplete || record.Kind != download.Kind || record.ID == "" || strings.ContainsAny(record.ID, "/\\.:") {
		return nil, 0, errors.New("result unavailable")
	}
	// The operation identity alone selects the destination; clients supply no path.
	return readUnderRoot(root, "results/"+record.ID, offset, maxBytes)
}

// OperationResultName names a complete download's result file under the
// provider root, the file ReadOperationResult reads, for a hard link
// [JOB-A17]. The operation identity alone selects it.
func (HTTPExecution) OperationResultName(record *job.Record) (string, error) {
	if record.State != job.StateComplete || record.Kind != download.Kind || record.ID == "" || strings.ContainsAny(record.ID, "/\\.:") {
		return "", errors.New("result unavailable")
	}
	return "results/" + record.ID, nil
}

// readUnderRoot reads one regular file named relative to the provider root.
// OpenRoot keeps lookup beneath the root, including link traversal.
func readUnderRoot(root, name string, offset, maxBytes int64) ([]byte, int64, error) {
	area, err := os.OpenRoot(root)
	if err != nil {
		return nil, 0, err
	}
	defer area.Close()
	info, err := area.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		// Absence of a complete operation's bytes is definitive [JOB-A10].
		return nil, 0, acceptanceprovider.ErrResultLost
	}
	if err != nil || !info.Mode().IsRegular() {
		return nil, 0, errors.New("result unavailable")
	}
	f, err := area.Open(name)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, 0, errors.New("result unavailable")
	}
	total := info.Size()
	if offset > total {
		return nil, 0, acceptanceprovider.ErrResultRange
	}
	count := min(maxBytes, total-offset)
	data := make([]byte, int(count))
	n, err := f.ReadAt(data, offset)
	if err != nil && !(err == io.EOF && n == len(data)) {
		return nil, 0, err
	}
	if n != len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	after, err := f.Stat()
	if err != nil || after.Size() != total || !after.ModTime().Equal(info.ModTime()) {
		return nil, 0, errors.New("result changed while reading")
	}
	return data, total, nil
}
