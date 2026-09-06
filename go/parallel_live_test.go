package download

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// TestLiveStreamVersusParallel measures the two fetchers against one real
// artifact on whatever connection the machine actually has.
//
// Off by default, because it moves gigabytes. Set ABSTRACTION_LIVE_DOWNLOAD=1.
// The order is stream, parallel, stream: the first run pays for a cold CDN
// edge and the third is the honest comparison against the second.
func TestLiveStreamVersusParallel(t *testing.T) {
	if os.Getenv("ABSTRACTION_LIVE_DOWNLOAD") == "" {
		t.Skip("set ABSTRACTION_LIVE_DOWNLOAD=1 to move real bytes")
	}
	url := envOr("ABSTRACTION_LIVE_URL",
		"https://huggingface.co/TinyLlama/TinyLlama-1.1B-Chat-v1.0/resolve/main/model.safetensors")
	digest := envOr("ABSTRACTION_LIVE_DIGEST",
		"sha256:6e6001da2106d4757498752a021df6c2bdc332c650aae4bae6b0c004dcf14933")
	size := int64(2200119864)

	for _, run := range []struct {
		label       string
		connections int
	}{
		{"stream (cold edge)", 1},
		{"parallel", DefaultConnections},
		{"stream (warm edge)", 1},
	} {
		root := t.TempDir()
		store, err := job.NewFileStore(root)
		if err != nil {
			t.Fatal(err)
		}
		r := NewRunner(store, "measure")
		r.Connections = run.connections
		id, err := Submit(store, Spec{
			Artifact: Artifact{Digest: digest, Size: size},
			Sources:  []Source{{Scheme: "https", Locator: url}},
			Sink:     Sink{Final: filepath.Join(root, "model.safetensors")},
		})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := r.Run(context.Background(), id); err != nil {
			t.Fatalf("%s: %v", run.label, err)
		}
		took := time.Since(start)
		t.Logf("%-20s %d stream(s)  %s  %.1f MB/s",
			run.label, run.connections, took.Round(time.Millisecond),
			float64(size)/took.Seconds()/1e6)
	}
}

// TestLiveSecondPassCost prices the thing a gapped plan cannot avoid: reading
// the finished artifact once more to hash it in order.
func TestLiveSecondPassCost(t *testing.T) {
	if os.Getenv("ABSTRACTION_LIVE_DOWNLOAD") == "" {
		t.Skip("set ABSTRACTION_LIVE_DOWNLOAD=1 to write and read gigabytes")
	}
	const size = 2200119864
	path := filepath.Join(t.TempDir(), "artifact.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	chunk := make([]byte, 1<<20)
	for n := 0; n < size; n += len(chunk) {
		if _, err := f.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, _, err := hashFile(context.Background(), path, nil); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	t.Logf("second pass over %d bytes: %s  %.0f MB/s", size, took.Round(time.Millisecond),
		float64(size)/took.Seconds()/1e6)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
