package serve

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	download "github.com/openabstractions/abstraction-download/go"
	request "github.com/openabstractions/abstraction-download/go/abstraction/download/request"
	job "github.com/openabstractions/abstraction-job/go"
)

// HTTPExecution is an explicitly configured service provider. Its input is the
// generated request payload; destinations are allocated by operation identity
// inside the private service root. It reuses the existing HTTP(S) runner.
// Credentials and external delegators require separately configured providers.
type HTTPExecution struct {
	OnError func(error)
}

func (HTTPExecution) Profile() string { return "download-http-request-v1" }

func (HTTPExecution) Prepare(id, kind string, raw []byte) ([]byte, error) {
	if kind != download.Kind {
		return nil, errors.New("unsupported work kind")
	}
	input, err := request.Decode(raw)
	if err != nil {
		return nil, err
	}
	if id == "" || strings.ContainsAny(id, "/\\.:") {
		return nil, errors.New("invalid operation identity")
	}
	spec := download.Spec{Artifact: download.Artifact{Digest: input.Artifact.Digest, Size: input.Artifact.Size},
		Sink: download.Sink{Partial: "work/" + id, Final: "results/" + id}}
	for _, source := range input.Sources {
		spec.Sources = append(spec.Sources, download.Source{Scheme: source.Scheme, Locator: source.Locator})
	}
	if spec.Artifact.Size < 0 {
		return nil, errors.New("negative artifact size")
	}
	if digest := spec.Artifact.Digest; digest != "" {
		decoded, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
		if !strings.HasPrefix(digest, "sha256:") || err != nil || len(decoded) != 32 {
			return nil, errors.New("SHA-256 digest required")
		}
	}
	if err := spec.ValidateFor(id); err != nil {
		return nil, err
	}
	for _, source := range spec.Sources {
		u, err := url.Parse(source.Locator)
		if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") || source.Scheme != u.Scheme {
			return nil, errors.New("anonymous HTTP(S) source required")
		}
	}
	return json.Marshal(spec)
}

func (e HTTPExecution) Serve(ctx context.Context, store job.Store) error {
	return serveExecution(ctx, store, e.OnError, nil)
}

func serveExecution(ctx context.Context, store job.Store, onError func(error), delegates []download.Delegator) error {
	runner := download.NewRunner(store, "service-"+job.NewID())
	runner.Fetchers = download.NewFetchers(download.HTTP{})
	if delegates != nil {
		runner.Delegators = download.NewDelegators(delegates...)
	}
	runner.SharedStore = true
	runner.Credentials = noExecutionCredentials{}
	defer runner.Close()
	watch := job.Watch(store, download.Kind)
	defer watch.Close()
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
		case <-ticker.C:
		}
	}
	return nil
}

type noExecutionCredentials struct{}

func (noExecutionCredentials) Lookup(string, string) (map[string]string, bool) { return nil, false }
