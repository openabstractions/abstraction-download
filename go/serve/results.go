package serve

import (
	"errors"
	"io"
	"os"
	"strings"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	api "github.com/openabstractions/abstraction-job/go/abstraction/job/acceptance"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

func (HTTPExecution) OperationFailure(record *job.Record) *api.WorkFailure {
	if raw, ok := record.Extensions[download.FailureExtension]; ok {
		if failure, err := download.DecodeFailure(raw); err == nil {
			class := "retryable"
			if download.Permanent(failure.Err()) {
				class = "permanent"
			}
			return &api.WorkFailure{Classification: class, Message: "download attempt failed"}
		}
	}
	if record.Error != "" || record.State == job.StateFailed {
		return &api.WorkFailure{Classification: "unknown", Message: "download reported an unclassified failure"}
	}
	return nil
}

func (HTTPExecution) ReadOperationResult(root string, record *job.Record, offset, maxBytes int64) ([]byte, int64, error) {
	if offset < 0 || maxBytes < 1 || maxBytes > acceptanceprovider.MaxResultBytes {
		return nil, 0, acceptanceprovider.ErrResultRange
	}
	if record.State != job.StateComplete || record.Kind != download.Kind || record.ID == "" || strings.ContainsAny(record.ID, "/\\.:") {
		return nil, 0, errors.New("result unavailable")
	}
	// OpenRoot keeps lookup beneath the provider root, including link traversal.
	// The operation identity alone selects the destination; clients supply no path.
	area, err := os.OpenRoot(root)
	if err != nil {
		return nil, 0, err
	}
	defer area.Close()
	name := "results/" + record.ID
	info, err := area.Lstat(name)
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
