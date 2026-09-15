package serve

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"

	download "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

// LegacySinkProfile names the managed HTTP profile of a root that holds
// operator-migrated legacy downloads. New submissions prepare exactly as
// HTTPExecution does. Only migrated journals reach PrepareLegacy.
const LegacySinkProfile = "download-http-request-v1+legacy-sinks-v1"

// LegacySinkExecution is HTTPExecution plus acceptance of converted legacy
// download records whose caller chose a relative sink inside the store root.
// Result files stay where the legacy caller put them. The service reads them
// under the provider root through normal result access; clients supply no path.
// Absolute and escaping sinks are refused: they name files outside the
// service-owned root.
type LegacySinkExecution struct {
	HTTPExecution
}

var _ acceptanceprovider.LegacyPreparer = LegacySinkExecution{}

func (LegacySinkExecution) Profile() string { return LegacySinkProfile }

// PrepareLegacy accepts the exact legacy download.Spec bytes a record carries.
// Submit never calls it; acceptanceprovider uses it only for migrated journals.
func (LegacySinkExecution) PrepareLegacy(id, kind string, raw []byte, required []string) ([]byte, []string, error) {
	if kind != download.Kind {
		return nil, nil, errors.New("unsupported legacy work kind")
	}
	if len(required) != 0 {
		return nil, nil, errors.New("legacy downloads carry no execution guarantees")
	}
	spec, err := legacySpec(raw)
	if err != nil {
		return nil, nil, err
	}
	if err := spec.ValidateFor(id); err != nil {
		return nil, nil, err
	}
	if err := containedSink(spec.Sink); err != nil {
		return nil, nil, err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, nil, err
	}
	return compact.Bytes(), nil, nil
}

func legacySpec(raw []byte) (download.Spec, error) {
	var spec download.Spec
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&spec); err != nil {
		return spec, err
	}
	if d.Decode(new(any)) != io.EOF {
		return spec, errors.New("trailing legacy specification data")
	}
	return spec, nil
}

func containedSink(sink download.Sink) error {
	if !download.PortableSink(sink) {
		return errors.New("legacy sink outside the service-owned root")
	}
	for _, p := range []string{sink.Final, sink.Partial} {
		if p == "" {
			continue
		}
		if err := download.EscapesRoot(p); err != nil {
			return err
		}
		if strings.Contains(p, `\`) {
			return errors.New("legacy sink must use portable separators")
		}
	}
	return nil
}

// ReadOperationResult reads results/<id> for new work and the retained relative
// sink for migrated legacy work. Preparation proved the sink contained.
func (e LegacySinkExecution) ReadOperationResult(root string, record *job.Record, offset, maxBytes int64) ([]byte, int64, error) {
	if offset < 0 || maxBytes < 1 || maxBytes > acceptanceprovider.MaxResultBytes {
		return nil, 0, acceptanceprovider.ErrResultRange
	}
	if record.State != job.StateComplete || record.Kind != download.Kind || record.ID == "" || strings.ContainsAny(record.ID, "/\\.:") {
		return nil, 0, errors.New("result unavailable")
	}
	spec, err := download.SpecOf(record)
	if err != nil {
		return nil, 0, errors.New("result unavailable")
	}
	if spec.Sink.Final == "results/"+record.ID {
		return e.HTTPExecution.ReadOperationResult(root, record, offset, maxBytes)
	}
	if containedSink(spec.Sink) != nil {
		return nil, 0, errors.New("result unavailable")
	}
	return readUnderRoot(root, spec.Sink.Final, offset, maxBytes)
}
