package download

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	job "github.com/openabstractions/abstraction-job/go"
	contentdigest "github.com/openabstractions/abstraction-storage/go/digest"
)

var ErrArchiveRestoreUnavailable = errors.New("download: the archive could not supply the exact artifact")

func archiveRestoreSpec(id string, artifact Artifact) Spec {
	return Spec{Artifact: artifact, ArchiveOnly: true,
		Sink: Sink{Partial: "work/" + id, Final: "results/" + id}}
}

func validArchiveRestore(id string, spec Spec) bool {
	return id != "" && contentdigest.Valid(spec.Artifact.Digest) && spec.Artifact.Size > 0 &&
		spec.Sink == archiveRestoreSpec(id, spec.Artifact).Sink &&
		spec.Constraints == nil && spec.Request == "" && len(spec.Sources) == 0
}

// SubmitArchiveRestore records archive-only work in the caller's private job
// store. The immutable admission is opaque to the Download runner.
func SubmitArchiveRestore(store job.Store, id string, artifact Artifact, admission json.RawMessage) (string, error) {
	spec := archiveRestoreSpec(id, artifact)
	spec.ArchiveAdmission = admission
	if !validArchiveRestore(id, spec) {
		return "", ErrArchiveRestoreUnavailable
	}
	record := job.Record{ID: id, Kind: Kind}
	record.Progress.Total = artifact.Size
	if err := record.SetSpec(spec); err != nil {
		return "", err
	}
	return store.Submit(record)
}

// RunArchiveRestore adopts the submitted private job. A transferred record
// can be delivered after restart without recopying its verified result.
func RunArchiveRestore(ctx context.Context, store job.Store, id string, archive DownloadArchive, checkSpace func(string, int64) error) (string, error) {
	got, err := store.Load(id)
	if err != nil {
		return "", err
	}
	spec, err := SpecOf(got)
	if err != nil || !spec.ArchiveOnly || !validArchiveRestore(id, spec) {
		return "", ErrArchiveRestoreUnavailable
	}
	if got.State != job.StateTransferred {
		if archive == nil {
			return "", ErrArchiveRestoreUnavailable
		}
		runner := NewRunner(store, "archive-restore")
		runner.Archive, runner.CheckWriteSpace, runner.archiveRestore = archive, checkSpace, true
		if err := runner.Run(ctx, id); err != nil {
			return "", err
		}
		got, err = store.Load(id)
		if err != nil {
			return "", err
		}
	}
	if got.State != job.StateTransferred {
		return "", ErrArchiveRestoreUnavailable
	}
	_, delivered, err := LocalSink(store, id, spec.Sink)
	return delivered, err
}

// CleanArchiveRestore drops only the native sink of a settled private job.
// The durable status is retained for Activity.
func CleanArchiveRestore(store *job.FileStore, record *job.Record) error {
	spec, err := SpecOf(record)
	if err != nil || !record.State.Terminal() || !spec.ArchiveOnly || !validArchiveRestore(record.ID, spec) {
		return ErrArchiveRestoreUnavailable
	}
	root, err := os.OpenRoot(store.Root())
	if err != nil {
		return err
	}
	defer root.Close()
	var failure error
	for _, path := range []string{spec.Sink.Partial, spec.Sink.Final} {
		if err := root.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			failure = errors.Join(failure, err)
		}
	}
	return failure
}
