package serve

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	identity "github.com/openabstractions/abstraction-identity"
	job "github.com/openabstractions/abstraction-job/go"
	"github.com/openabstractions/abstraction-job/go/acceptanceprovider"
)

func TestResultReadRefusesSameSizeRestoredTimeRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result")
	if err := os.WriteFile(path, []byte("old bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	before, info, err := identity.FileStamp(f)
	if err != nil {
		t.Fatal(err)
	}
	read := func(data []byte, offset int64) (int, error) {
		// Observe the change clock, rather than guessing its tick with a sleep.
		deadline := time.Now().Add(2 * time.Second)
		for {
			if err := os.WriteFile(path, []byte("new bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
				t.Fatal(err)
			}
			after, current, err := identity.FileStamp(f)
			if err != nil {
				t.Fatal(err)
			}
			if current.Size() != info.Size() || !current.ModTime().Equal(info.ModTime()) {
				t.Fatal("fixture did not restore metadata")
			}
			if after != before {
				break
			}
			if time.Now().After(deadline) {
				t.Skip("filesystem does not expose an advanced change stamp")
			}
		}
		return f.ReadAt(data, offset)
	}
	if data, _, err := readResultFile(f, 0, 9, read); err == nil || len(data) != 0 || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("rewrite accepted: %q %v", data, err)
	}
}

func TestCompletedHTTPResultBoundsAndAbsence(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "results"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "results", "operation123")
	if err := os.WriteFile(path, []byte("0123456789"), 0600); err != nil {
		t.Fatal(err)
	}
	record := &job.Record{ID: "operation123", Kind: download.Kind, State: job.StateComplete}
	e := HTTPExecution{}
	data, total, err := e.ReadOperationResult(root, record, 7, 65536)
	if err != nil || total != 10 || string(data) != "789" {
		t.Fatalf("result: %q %d %v", data, total, err)
	}
	data, total, err = e.ReadOperationResult(root, record, 10, 1)
	if err != nil || total != 10 || len(data) != 0 {
		t.Fatalf("EOF: %q %d %v", data, total, err)
	}
	if _, _, err := e.ReadOperationResult(root, record, 11, 1); !errors.Is(err, acceptanceprovider.ErrResultRange) {
		t.Fatalf("past end: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ReadOperationResult(root, record, 0, 1); err == nil {
		t.Fatal("missing result became EOF")
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ReadOperationResult(root, record, 0, 1); err == nil {
		t.Fatal("directory exposed as result")
	}
}

func TestHTTPResultRefusesEscapingLink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "operation123"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "results")); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	record := &job.Record{ID: "operation123", Kind: download.Kind, State: job.StateComplete}
	if data, _, err := (HTTPExecution{}).ReadOperationResult(root, record, 0, 64); err == nil || len(data) != 0 {
		t.Fatal("result escaped private root")
	}
}

func TestHTTPFailureClassificationPreservesUnknown(t *testing.T) {
	for _, c := range []struct {
		raw, causeRaw, errorText string
		state                    job.State
		want, cause              string
	}{
		{`{"error":"private diagnostic","permanent":true}`, "", "", job.StateFailed, "permanent", ""},
		// Permanent class on nonterminal work contradicts JOB-A8.
		{`{"error":"private diagnostic","permanent":true}`, "", "", job.StatePending, "unknown", ""},
		{`{"error":""}`, "", "", job.StatePending, "retryable", ""},
		{`{"error":""}`, "", "", job.StateFailed, "unknown", ""},
		{`{"error":"private diagnostic"}`, `{"error":"private diagnostic","cause":"server_error"}`, "", job.StatePending, "retryable", "server_error"},
		{`{"error":"private diagnostic","permanent":true}`, `{"error":"private diagnostic","permanent":true,"cause":"digest_mismatch"}`, "", job.StateFailed, "permanent", "digest_mismatch"},
		{`{"error":"private diagnostic","permanent":true}`, `{"error":"private diagnostic","permanent":true,"cause":"future_word"}`, "", job.StateFailed, "permanent", "other"},
		// failure@2 names a cause; only failure@1 decides the class.
		{`{"error":"private diagnostic"}`, `{"error":"private diagnostic","permanent":true,"cause":"not_found"}`, "", job.StatePending, "retryable", "not_found"},
		{`{"error":"private diagnostic","permanent":true}`, `{"unreadable":1}`, "", job.StateFailed, "permanent", ""},
		{`{"invalid":true}`, "", "private diagnostic", job.StatePending, "unknown", ""},
	} {
		record := &job.Record{State: c.state, Error: c.errorText, Extensions: map[string]json.RawMessage{download.FailureExtension: json.RawMessage(c.raw)}}
		if c.causeRaw != "" {
			record.Extensions[download.FailureCauseExtension] = json.RawMessage(c.causeRaw)
		}
		failure := (HTTPExecution{}).OperationFailure(record)
		if failure == nil || failure.Classification.String() != c.want || string(failure.Cause) != c.cause || strings.Contains(failure.Message, "private diagnostic") {
			t.Fatalf("%s in %s: %+v", c.raw, c.state, failure)
		}
	}
}
