package archive

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// inventoryFixture is fixture with the inventory on, hashing files of 1 KiB
// or more at a rate tests do not wait for.
func inventoryFixture(t *testing.T, async bool) string {
	t.Helper()
	folder := fixture(t)
	savedMin, savedRate, savedQuiet, savedPoll, savedAsync := inventoryMinBytes, inventoryRate, inventoryQuiet, inventoryPoll, inventoryAsync
	inventoryMinBytes, inventoryRate, inventoryQuiet, inventoryPoll, inventoryAsync = 1<<10, 1<<40, 0, 5*time.Millisecond, async
	t.Cleanup(func() {
		StopInventory()
		waitInventoryIdle(t)
		inventoryMinBytes, inventoryRate, inventoryQuiet, inventoryPoll, inventoryAsync = savedMin, savedRate, savedQuiet, savedPoll, savedAsync
		unhashable.Clear()
	})
	return folder
}

func waitInventoryIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		inv.mu.Lock()
		running := inv.running
		inv.mu.Unlock()
		if !running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the inventory did not end")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func recorded(t *testing.T, folder, rel string) string {
	t.Helper()
	index, ok := readIndex(folder)
	if !ok {
		t.Fatal("no listing")
	}
	file, _ := listed(index, rel)
	return file.SHA256
}

// The inventory walks a folder with no listing, hashes each plain-named
// file of the minimum size once and records it; files below the minimum
// stay unhashed, and the person's files are only read. A download then
// finds its file by the recorded hash before a same-size file that sorts
// first.
func TestInventoryRecordsPlainNamedFilesForLookupByHash(t *testing.T) {
	folder := inventoryFixture(t, false)
	body := bytes.Repeat([]byte("wan2.2 weights "), 4096)
	w := wantOf(body)
	model := putNamed(t, folder, "OA-models/comfyui/wan2.2-ti2v-5b/wan2.2_ti2v_5B_fp16.safetensors", body)
	putNamed(t, folder, "OA-models/a-decoy.safetensors", sameSizeWrong(body, 3))
	putNamed(t, folder, "OA-models/small.json", []byte("{}"))
	StartInventory()
	if got := recorded(t, folder, "OA-models/comfyui/wan2.2-ti2v-5b/wan2.2_ti2v_5B_fp16.safetensors"); got != w.SHA256 {
		t.Fatalf("the plain-named file's SHA-256 is %q, want %s", got, w.SHA256)
	}
	if got := recorded(t, folder, "OA-models/a-decoy.safetensors"); !validSHA256Hex(got) || got == w.SHA256 {
		t.Fatalf("the decoy's SHA-256 is %q", got)
	}
	if kept, _ := os.ReadFile(model); !bytes.Equal(kept, body) {
		t.Fatal("the person's file changed")
	}
	if _, err := os.Stat(filepath.Join(folder, "sha256")); !os.IsNotExist(err) {
		t.Fatalf("the inventory wrote into the archive: %v", err)
	}
	found := sources(context.Background(), folder, w, 2)
	if len(found) != 1 || found[0].path != model {
		t.Fatalf("the lookup did not go straight to the recorded file: %+v", found)
	}
	if got, ok := fill(t, w); !ok || !bytes.Equal(got, body) {
		t.Fatal("the recorded file did not fill the download")
	}
}

// The inventory waits while an archive copy runs and starts after it ends.
func TestInventoryWaitsForArchiveCopies(t *testing.T) {
	folder := inventoryFixture(t, true)
	body := bytes.Repeat([]byte("umt5 "), 4096)
	w := wantOf(body)
	putNamed(t, folder, "umt5_xxl_fp8_e4m3fn_scaled.safetensors", body)
	look(t)
	release := hold()
	StartInventory()
	time.Sleep(200 * time.Millisecond)
	if got := recorded(t, folder, "umt5_xxl_fp8_e4m3fn_scaled.safetensors"); got != "" {
		release()
		t.Fatal("the inventory read while an archive copy ran")
	}
	release()
	waitInventoryIdle(t)
	if got := recorded(t, folder, "umt5_xxl_fp8_e4m3fn_scaled.safetensors"); got != w.SHA256 {
		t.Fatalf("the inventory did not resume after the copy: %q", got)
	}
}

// The pass reads no faster than its rate.
func TestInventoryReadsAtItsRate(t *testing.T) {
	folder := inventoryFixture(t, false)
	inventoryRate = 4 << 20
	body := bytes.Repeat([]byte{7}, 2<<20)
	putNamed(t, folder, "vae.safetensors", body)
	start := time.Now()
	StartInventory()
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("2 MiB at 4 MiB/s took %v", elapsed)
	}
	if got := recorded(t, folder, "vae.safetensors"); got != wantOf(body).SHA256 {
		t.Fatalf("not recorded: %q", got)
	}
}

// A file that changed after its SHA-256 was recorded loses the record at
// the next lookup, is copied and verified as a size match, and its new
// SHA-256 is recorded.
func TestChangedFileLosesItsRecordedHash(t *testing.T) {
	folder := inventoryFixture(t, false)
	body := bytes.Repeat([]byte("ti2v "), 4096)
	w := wantOf(body)
	model := putNamed(t, folder, "wan.safetensors", body)
	StartInventory()
	if recorded(t, folder, "wan.safetensors") != w.SHA256 {
		t.Fatal("not recorded")
	}
	StopInventory()
	changed := sameSizeWrong(body, 11)
	if err := os.WriteFile(model, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(model, later, later); err != nil {
		t.Fatal(err)
	}
	forgetRun()
	if _, ok := fill(t, w); ok {
		t.Fatal("a changed file filled the download")
	}
	if got := recorded(t, folder, "wan.safetensors"); got != wantOf(changed).SHA256 {
		t.Fatalf("the changed file's record is %q, want its new SHA-256", got)
	}
}

// A folder with a SHA256SUMS file has its files' SHA-256s seeded from it at
// the walk: the inventory reads none of them, a download finds its file by
// the stated hash, and the copy at serve time still hashes every byte. A
// stated hash that is wrong is replaced by the one the copy computed.
func TestSHA256SUMSSeedsTheListingWithoutHashing(t *testing.T) {
	folder := inventoryFixture(t, false)
	body := bytes.Repeat([]byte("transformer weights "), 4096)
	vae := bytes.Repeat([]byte("audio vae weights "), 4096)
	w, wrong := wantOf(body), wantOf(vae)
	putNamed(t, folder, "models/set/transformer.gguf", body)
	putNamed(t, folder, "models/set/audio-vae.safetensors", vae)
	putNamed(t, folder, "models/set/unstated.safetensors", bytes.Repeat([]byte("x"), 4096))
	// The audio VAE's line states the transformer's hash: a stale manifest.
	sums := w.SHA256 + "  transformer.gguf\r\n" +
		w.SHA256 + " *audio-vae.safetensors\n" +
		wrong.SHA256 + "  ../escape.gguf\n" +
		"not a line\n"
	putNamed(t, folder, "models/set/SHA256SUMS", []byte(sums))
	hashed := 0
	savedLog := logf
	logf = func(format string, args ...any) {
		if format == "recorded the SHA-256 of %s" {
			hashed++
		}
	}
	t.Cleanup(func() { logf = savedLog })
	index := look(t)
	if got, _ := listed(index, "models/set/transformer.gguf"); got.SHA256 != w.SHA256 {
		t.Fatalf("the stated SHA-256 was not seeded: %+v", got)
	}
	if _, held := listed(index, "models/set/SHA256SUMS"); held {
		t.Fatal("the manifest itself is listed")
	}
	StartInventory()
	if hashed != 1 {
		t.Fatalf("the inventory hashed %d files; only the unstated one has no hash", hashed)
	}
	got, ok := fill(t, w)
	if !ok || !bytes.Equal(got, body) {
		t.Fatal("the seeded file did not fill the download")
	}
	if _, ok := fill(t, Want{SHA256: w.SHA256, Bytes: int64(len(vae))}); ok {
		t.Fatal("bytes that differ from the stated hash filled a download")
	}
	if got := recorded(t, folder, "models/set/audio-vae.safetensors"); got != wrong.SHA256 {
		t.Fatalf("the copy's computed SHA-256 did not replace the stale one: %q", got)
	}
	// A later walk keeps the computed hash over the manifest's.
	forgetRun()
	look(t)
	if got := recorded(t, folder, "models/set/audio-vae.safetensors"); got != wrong.SHA256 {
		t.Fatalf("a later walk put the stale hash back: %q", got)
	}
}
