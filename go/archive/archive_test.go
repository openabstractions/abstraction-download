package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openabstractions/abstraction-identity/fileid"
)

func TestArchiveCopyProbesOverflowWithoutWritingIt(t *testing.T) {
	for _, extra := range []string{"", "x"} {
		t.Run(fmt.Sprint(len(extra)), func(t *testing.T) {
			root := t.TempDir()
			entry, dst := filepath.Join(root, "entry"), filepath.Join(root, "dst")
			body := []byte("archive bytes")
			sum := sha256.Sum256(body)
			if err := os.WriteFile(entry, append(bytes.Clone(body), extra...), 0600); err != nil {
				t.Fatal(err)
			}
			var written int64
			_, err := copyArchiveEntry(t.Context(), func() (entryFile, error) { return os.Open(entry) }, dst,
				hex.EncodeToString(sum[:]), int64(len(body)), func(n int64) { written = max(written, n) }, nil)
			if written > int64(len(body)) {
				t.Fatal("archive overflow reached the destination", written)
			}
			if extra != "" {
				if !errors.Is(err, errDiffers) {
					t.Fatal("oversize archive was accepted", err)
				}
				if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("failed destination was retained", err)
				}
			} else if got, readErr := os.ReadFile(dst); err != nil || readErr != nil || !bytes.Equal(got, body) {
				t.Fatal("archive hit did not publish", err, readErr)
			}
		})
	}
}

// fixture gives the test its own state folder and an archive folder, and
// runs the write-back and the walks in the test's goroutine.
func fixture(t *testing.T) (folder string) {
	t.Helper()
	UseState(filepath.Join(t.TempDir(), "state"))
	t.Cleanup(func() { UseState("") })
	folder = filepath.Join(t.TempDir(), "archive")
	if err := os.MkdirAll(folder, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := SetFolder(context.Background(), folder); err != nil {
		t.Fatal(err)
	}
	saved, savedWalk, savedMin, savedKeep := writeAsync, walkAsync, indexMinBytes, keepMinBytes
	writeAsync, walkAsync, indexMinBytes, keepMinBytes = false, false, 1, 1
	savedSpace := volumeSpace
	volumeSpace = func(string) (int64, int64, error) { return 1 << 50, 1 << 50, nil }
	t.Cleanup(func() {
		writeAsync, walkAsync, indexMinBytes, keepMinBytes = saved, savedWalk, savedMin, savedKeep
		volumeSpace = savedSpace
		forgetRun()
	})
	return folder
}

// forgetRun clears what one process remembers about the archive, as a
// restart does; the stored listing stays.
func forgetRun() {
	renewAll()
	quietUntil.Clear()
	walks.Lock()
	clear(walks.started)
	clear(walks.silent)
	walks.Unlock()
	forgetIndex()
}

func wantOf(body []byte) Want {
	sum := sha256.Sum256(body)
	return Want{SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(body))}
}

// mustEntry is the content name of sha in folder.
func mustEntry(t *testing.T, folder, sha string) string {
	t.Helper()
	entry, err := entryPath(folder, sha)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func putEntry(t *testing.T, folder, sha string, body []byte) string {
	t.Helper()
	entry := mustEntry(t, folder, sha)
	if err := os.MkdirAll(filepath.Dir(entry), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return entry
}

// putNamed writes body at the folder-relative /-separated name.
func putNamed(t *testing.T, folder, name string, body []byte) string {
	t.Helper()
	file := filepath.Join(folder, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

// look walks the archive folder in the test's goroutine.
func look(t *testing.T) Listing {
	t.Helper()
	if done := Look(); done == nil {
		t.Fatal("no archive folder to look in")
	}
	index, ok := readIndex(Folder())
	if !ok {
		t.Fatal("the walk saved no listing")
	}
	return index
}

func listed(index Listing, rel string) (File, bool) {
	for _, file := range index.Files {
		if file.Path == rel {
			return file, true
		}
	}
	return File{}, false
}

// sameSizeWrong is body with one byte changed: the same size, another hash.
func sameSizeWrong(body []byte, at int) []byte {
	wrong := bytes.Clone(body)
	wrong[at] ^= 0xff
	return wrong
}

// fill runs Fill into a fresh file and returns its bytes.
func fill(t *testing.T, w Want) ([]byte, bool) {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "partial")
	n, ok := Fill(context.Background(), w, dst, nil)
	if !ok {
		if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a failed fill left %s: %v", dst, err)
		}
		return nil, false
	}
	got, err := os.ReadFile(dst)
	if err != nil || int64(len(got)) != n {
		t.Fatalf("filled %d bytes, read %d: %v", n, len(got), err)
	}
	return got, true
}

func queueLen(t *testing.T) int {
	t.Helper()
	queue, err := queueDir()
	if err != nil {
		t.Fatal(err)
	}
	return len(readQueue(queue))
}

func TestSettingReadsBackAndClears(t *testing.T) {
	UseState(filepath.Join(t.TempDir(), "state"))
	t.Cleanup(func() { UseState("") })
	if Folder() != "" {
		t.Fatal("a new state folder has an archive folder")
	}
	if err := SetFolder(context.Background(), "relative\\folder"); err == nil {
		t.Fatal("a relative archive folder was saved")
	}
	folder := filepath.Join(t.TempDir(), "archive")
	if err := SetFolder(context.Background(), " "+folder+" "); err != nil {
		t.Fatal(err)
	}
	if got := Folder(); got != folder {
		t.Fatalf("archive folder %q, want %q", got, folder)
	}
	if Reachable(folder) {
		t.Fatal("a folder that does not exist is reachable")
	}
	if err := SetFolder(context.Background(), ""); err != nil || Folder() != "" {
		t.Fatalf("the archive was not cleared: %q, %v", Folder(), err)
	}
}

func TestStateForFollowsTheRuntimeStateFolder(t *testing.T) {
	state := filepath.Join(t.TempDir(), "runtime")
	if got, err := StateFor(state); err != nil || got != filepath.Join(state, "setup") {
		t.Fatalf("StateFor(%q) = %q, %v", state, got, err)
	}
	if _, err := StateFor("relative"); err == nil {
		t.Fatal("a relative runtime state folder was accepted")
	}
	config, err := os.UserConfigDir()
	if err != nil {
		t.Skip(err)
	}
	if got, err := StateFor(""); err != nil || got != filepath.Join(config, "OpenAbstractions", "setup") {
		t.Fatalf("StateFor(\"\") = %q, %v", got, err)
	}
}

// A content-name entry is copied and hashed into the download's partial;
// the archive entry stays as it was.
func TestEntryFillsTheDownload(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("model bytes "), 4096)
	w := wantOf(body)
	entry := putEntry(t, folder, w.SHA256, body)
	got, ok := fill(t, w)
	if !ok || !bytes.Equal(got, body) {
		t.Fatal("the archive entry did not fill the download")
	}
	if kept, _ := os.ReadFile(entry); !bytes.Equal(kept, body) {
		t.Fatal("the archive entry changed")
	}
	// A download of unknown size is found by its content name too.
	if got, ok := fill(t, Want{SHA256: w.SHA256}); !ok || !bytes.Equal(got, body) {
		t.Fatal("a download of unknown size was not filled")
	}
	// Served copies never use up the archive for later downloads.
	if _, ok := fill(t, w); !ok {
		t.Fatal("a third download of the same file went to the network")
	}
}

// Bytes already in the partial from an earlier process are continued; a
// partial holding other bytes is copied again from the start.
func TestFillContinuesOrRestartsAPartial(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("0123456789"), 1000)
	w := wantOf(body)
	putEntry(t, folder, w.SHA256, body)
	for _, prefix := range [][]byte{body[:4000], sameSizeWrong(body, 5)[:4000]} {
		dst := filepath.Join(t.TempDir(), "partial")
		if err := os.WriteFile(dst, prefix, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, ok := Fill(context.Background(), w, dst, nil); !ok {
			t.Fatal("the fill failed")
		}
		if got, _ := os.ReadFile(dst); !bytes.Equal(got, body) {
			t.Fatal("the partial does not hold the wanted bytes")
		}
	}
}

// An entry whose bytes differ is rejected with one log line and Fill
// answers false; a verified download kept afterwards replaces the entry.
func TestCorruptEntryIsRejectedThenReplacedByTheDownload(t *testing.T) {
	folder := fixture(t)
	body := []byte("the reviewed package bytes")
	w := wantOf(body)
	entry := putEntry(t, folder, w.SHA256, sameSizeWrong(body, 3))
	var logged bytes.Buffer
	saved := log.Writer()
	log.SetOutput(&logged)
	defer log.SetOutput(saved)
	if _, ok := fill(t, w); ok {
		t.Fatal("differing archive bytes were accepted")
	}
	if !strings.Contains(logged.String(), "differ") {
		t.Fatalf("no log line names the mismatch: %q", logged.String())
	}
	if len(sources(context.Background(), folder, w, 1)) != 0 {
		t.Fatal("a rejected entry is offered again in this process")
	}
	source := putNamed(t, t.TempDir(), "download.bin", body)
	Keep(source, w.SHA256)
	if kept, _ := os.ReadFile(entry); !bytes.Equal(kept, body) {
		t.Fatal("the rejected entry was not replaced by the verified download")
	}
	if got, ok := fill(t, w); !ok || !bytes.Equal(got, body) {
		t.Fatal("the replaced entry is not offered")
	}
}

// A kept download is copied into the archive, continuing an earlier partial
// copy there, and leaves the queue.
func TestKeptDownloadIsWrittenResumingAPartial(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("0123456789"), 1000)
	w := wantOf(body)
	entry := mustEntry(t, folder, w.SHA256)
	if err := os.MkdirAll(filepath.Dir(entry), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry+".partial", body[:4000], 0o600); err != nil {
		t.Fatal(err)
	}
	Keep(putNamed(t, t.TempDir(), "download.bin", body), w.SHA256)
	if kept, err := os.ReadFile(entry); err != nil || !bytes.Equal(kept, body) {
		t.Fatalf("the archive entry is not the download: %v", err)
	}
	if _, err := os.Stat(entry + ".partial"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the partial copy stayed")
	}
	if n := queueLen(t); n != 0 {
		t.Fatalf("the write-back queue kept %d entries", n)
	}
}

// An unreachable archive costs one bounded probe: Fill answers false, the
// kept download stays queued for later, and stopping the archive drops the
// queue.
func TestUnreachableArchiveCostsOneBoundedProbe(t *testing.T) {
	fixture(t)
	offline := filepath.Join(t.TempDir(), "asleep")
	if runtime.GOOS == "windows" {
		offline = `\\nas.test\archive`
		saved := probeTimeout
		probeTimeout = 300 * time.Millisecond
		t.Cleanup(func() { probeTimeout = saved })
	}
	if err := SetFolder(context.Background(), offline); err != nil {
		t.Fatal(err)
	}
	body := []byte("pinned package")
	w := wantOf(body)
	start := time.Now()
	if _, ok := fill(t, w); ok {
		t.Fatal("an unreachable archive filled a download")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the unreachable archive held the download for %v", elapsed)
	}
	Keep(putNamed(t, t.TempDir(), "download.bin", body), w.SHA256)
	if n := queueLen(t); n != 1 {
		t.Fatalf("the write-back is not queued for later: %d entries", n)
	}
	if err := SetFolder(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if n := queueLen(t); n != 0 {
		t.Fatal("the queue outlived the archive setting")
	}
}

// The caller's deadline bounds the probe.
func TestProbeFollowsTheCallersDeadline(t *testing.T) {
	saved := probeTimeout
	probeTimeout = time.Hour
	t.Cleanup(func() { probeTimeout = saved })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := probeWithin(ctx, func(string) (os.FileInfo, error) { time.Sleep(10 * time.Second); return nil, nil }, "x")
	if !errors.Is(err, errSlow) || time.Since(start) > 5*time.Second {
		t.Fatalf("the probe ignored the deadline: %v after %v", err, time.Since(start))
	}
}

// An entry of another size is never filled.
func TestFillNeedsTheExactSize(t *testing.T) {
	folder := fixture(t)
	body := []byte("pinned")
	w := wantOf(body)
	putEntry(t, folder, w.SHA256, append(bytes.Clone(body), '!'))
	if _, ok := fill(t, w); ok {
		t.Fatal("an entry of another size filled the download")
	}
}

// A partial copy in the archive whose earlier bytes differ is written again
// from the start; a source that no longer has its SHA-256 is never published
// under the content name and leaves the queue.
func TestWriteBackVerifiesEveryByteBeforeTheRename(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("abcdefghij"), 1000)
	w := wantOf(body)
	entry := mustEntry(t, folder, w.SHA256)
	if err := os.MkdirAll(filepath.Dir(entry), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry+".partial", sameSizeWrong(body, 10)[:4000], 0o600); err != nil {
		t.Fatal(err)
	}
	source := putNamed(t, t.TempDir(), "download.zip", body)
	Keep(source, w.SHA256)
	if kept, err := os.ReadFile(entry); err != nil || !bytes.Equal(kept, body) {
		t.Fatalf("a differing partial was published or not rewritten: %v", err)
	}
	other := wantOf(body[:5000])
	if err := os.WriteFile(source, sameSizeWrong(body, 0)[:5000], 0o600); err != nil {
		t.Fatal(err)
	}
	Keep(source, other.SHA256)
	otherEntry := mustEntry(t, folder, other.SHA256)
	for _, name := range []string{otherEntry, otherEntry + ".partial"} {
		if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s holds bytes that are not its SHA-256: %v", name, err)
		}
	}
	if n := queueLen(t); n != 0 {
		t.Fatalf("the differing download stayed queued: %d", n)
	}
}

// A read from the archive that moves nothing for the stall limit fails.
func TestStalledReadIsRejected(t *testing.T) {
	saved := stallLimit
	stallLimit = 50 * time.Millisecond
	t.Cleanup(func() { stallLimit = saved })
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	defer r.Close()
	start := time.Now()
	if _, err := (&stallFile{f: r}).Read(make([]byte, 16)); !errors.Is(err, errStalled) {
		t.Fatalf("a silent read returned %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the stall limit did not bound the read")
	}
}

// The write-back queue drops a download queued more than a week ago, then
// the oldest links beyond its size limit.
func TestWriteBackQueueIsBoundedByAgeAndSize(t *testing.T) {
	fixture(t)
	queue, _ := queueDir()
	if err := os.MkdirAll(queue, 0o700); err != nil {
		t.Fatal(err)
	}
	put := func(fill byte, size int, age time.Duration) string {
		sha := hex.EncodeToString(bytes.Repeat([]byte{fill}, 32))
		if err := os.WriteFile(filepath.Join(queue, sha), bytes.Repeat([]byte{fill}, size), 0o600); err != nil {
			t.Fatal(err)
		}
		stamp := []byte(strconv.FormatInt(time.Now().Add(-age).Unix(), 10))
		if err := os.WriteFile(filepath.Join(queue, sha+".at"), stamp, 0o600); err != nil {
			t.Fatal(err)
		}
		return sha
	}
	stale := put(1, 10, 8*24*time.Hour)
	oldest := put(2, 600, 3*time.Hour)
	newer := put(3, 600, time.Hour)
	saved := queueMaxBytes
	queueMaxBytes = 1000
	t.Cleanup(func() { queueMaxBytes = saved })
	kept := pruneQueue(queue, readQueue(queue))
	if len(kept) != 1 || kept[0].sha != newer {
		t.Fatalf("kept %+v, want only the newest", kept)
	}
	for _, sha := range []string{stale, oldest} {
		if _, err := os.Stat(filepath.Join(queue, sha)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s still holds disk", sha)
		}
	}
}

func junction(t *testing.T, link, target string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		if out, err := exec.Command("cmd", "/c", "mklink", "/J", link, target).CombinedOutput(); err != nil {
			t.Fatalf("mklink: %v %s", err, out)
		}
	} else if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(link) })
}

func TestRestoreRefusesSubstitutedArchiveDirectory(t *testing.T) {
	folder := fixture(t)
	body := []byte("restore source held by exact native identity")
	w := wantOf(body)
	subdirectory := filepath.Join(folder, "sha256")
	if err := os.MkdirAll(subdirectory, 0700); err != nil {
		t.Fatal(err)
	}
	entry := filepath.Join(subdirectory, w.SHA256)
	if err := os.WriteFile(entry, body, 0600); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + w.SHA256
	selected, ok := (Service{}).SelectRestore(t.Context(), digest, int64(len(body)))
	if !ok {
		t.Fatal("could not observe managed source")
	}
	displaced := filepath.Join(folder, "displaced")
	if err := os.Rename(subdirectory, displaced); err != nil {
		t.Fatal(err)
	}
	junction(t, subdirectory, displaced)
	partial := filepath.Join(t.TempDir(), "partial")
	if _, ok := selected.Fill(t.Context(), digest, int64(len(body)), partial, nil); ok {
		t.Fatal("selected file was reopened through a substituted directory")
	}
	if _, ok := (Service{}).SelectRestore(t.Context(), digest, int64(len(body))); ok {
		t.Fatal("archive offered the linked directory as a managed source")
	}
}

func TestRestoreRefusesSameFileMovedIntoReplacementAncestry(t *testing.T) {
	for _, replace := range []string{"root", "parent"} {
		t.Run(replace, func(t *testing.T) {
			folder := fixture(t)
			body := []byte("the same physical file, in a different archive directory")
			want := wantOf(body)
			parent := filepath.Join(folder, "sha256")
			if err := os.Mkdir(parent, 0700); err != nil {
				t.Fatal(err)
			}
			entry := filepath.Join(parent, want.SHA256)
			if err := os.WriteFile(entry, body, 0600); err != nil {
				t.Fatal(err)
			}
			initial, err := os.Stat(entry)
			if err != nil {
				t.Fatal(err)
			}
			options := fileid.Options{Links: fileid.RefuseLinks}
			initialID, err := fileid.Resolve(t.Context(), fileid.Self, entry, options)
			if err != nil {
				t.Fatal(err)
			}
			digest := "sha256:" + want.SHA256
			size := int64(len(body))
			selected, ok := (Service{}).SelectRestore(t.Context(), digest, size)
			if !ok {
				t.Fatal("archive source unavailable")
			}
			dir, child := parent, want.SHA256
			if replace == "root" {
				dir, child = folder, "sha256"
			}
			displaced := dir + "-displaced"
			if err := os.Rename(dir, displaced); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(filepath.Join(displaced, child), filepath.Join(dir, child)); err != nil {
				t.Fatal(err)
			}
			currentID, err := fileid.Resolve(t.Context(), fileid.Self, entry, options)
			if err != nil {
				t.Fatal(err)
			}
			current, err := os.Stat(entry)
			if err != nil || !initialID.Equal(currentID) || !initial.ModTime().Equal(current.ModTime()) {
				t.Fatalf("fixture failed to keep the exact leaf: %v", err)
			}
			dst := filepath.Join(t.TempDir(), "restored")
			if _, ok := selected.Fill(t.Context(), digest, size, dst, nil); ok {
				t.Fatal("same file under replaced ancestry reused an old source observation")
			}
			fresh, ok := (Service{}).SelectRestore(t.Context(), digest, size)
			if !ok || fresh.Proof() == selected.Proof() {
				t.Fatal("replacement archive did not get a distinct observation")
			}
			if _, ok := fresh.Fill(t.Context(), digest, size, dst, nil); !ok {
				t.Fatal("the freshly selected exact archive source could not be read")
			}
		})
	}
}

func TestRestoreKeepsDirectoryCustodyThroughCopy(t *testing.T) {
	folder := fixture(t)
	parent := filepath.Join(folder, "sha256")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	body := []byte("move the archive after copying starts")
	want := wantOf(body)
	digest, size := "sha256:"+want.SHA256, int64(len(body))
	if err := os.WriteFile(filepath.Join(parent, want.SHA256), body, 0600); err != nil {
		t.Fatal(err)
	}
	selected, ok := (Service{}).SelectRestore(t.Context(), digest, size)
	if !ok {
		t.Fatal("archive source unavailable")
	}
	var attempted bool
	var moved error
	dst := filepath.Join(t.TempDir(), "restored")
	_, copied := selected.Fill(t.Context(), digest, size, dst, func(int64, int64) {
		if !attempted {
			attempted = true
			moved = os.Rename(parent, parent+"-displaced")
		}
	})
	if !attempted {
		t.Fatal("test did not reach source copying")
	}
	if runtime.GOOS == "windows" {
		if moved == nil || !copied {
			t.Fatalf("held Windows folder: move=%v copy=%v", moved, copied)
		}
	} else if moved != nil || copied {
		t.Fatalf("POSIX namespace revalidation: move=%v copy=%v", moved, copied)
	}
}

func TestRestoreMetadataTimeoutKeepsOneOutstandingProbe(t *testing.T) {
	folder := t.TempDir()
	saved := probeTimeout
	probeTimeout = 50 * time.Millisecond
	t.Cleanup(func() { probeTimeout = saved; quietUntil.Delete(folder) })
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	answer := make(chan RestoreSource, 1)
	go func() {
		answer <- probeRestore(t.Context(), folder, func(context.Context) RestoreSource {
			close(started)
			<-release
			return RestoreSource{proof: "would have offered"}
		})
	}()
	<-started
	extra := func(context.Context) RestoreSource { return RestoreSource{proof: "duplicate probe"} }
	if got := probeRestore(t.Context(), folder, extra); got.proof != "" {
		t.Fatal("a concurrent probe was started")
	}
	select {
	case result := <-answer:
		if result.proof != "" || !quiet(folder) {
			t.Fatal("a timed-out probe was not refused and silenced")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the probe deadline waited for the native metadata request")
	}
	quietUntil.Delete(folder)
	if got := probeRestore(t.Context(), folder, extra); got.proof != "" {
		t.Fatal("deadline released probe admission before native cleanup")
	}
}

// A junction at the archive's sha256 folder is never followed: the entry is
// skipped with one log line, and the write-back writes nothing through the
// link (security review 2026-11-09).
func TestLinkIsSkippedAndNothingIsWrittenThroughIt(t *testing.T) {
	folder := fixture(t)
	body := []byte("pinned package behind a link")
	w := wantOf(body)
	elsewhere := t.TempDir()
	if err := os.WriteFile(filepath.Join(elsewhere, w.SHA256), body, 0o600); err != nil {
		t.Fatal(err)
	}
	junction(t, filepath.Join(folder, "sha256"), elsewhere)
	var logged bytes.Buffer
	saved := log.Writer()
	log.SetOutput(&logged)
	defer log.SetOutput(saved)
	if len(sources(context.Background(), folder, w, 1)) != 0 {
		t.Fatal("an entry behind a junction was offered")
	}
	if lines := strings.Count(logged.String(), "\n"); lines != 1 || !strings.Contains(logged.String(), "link") {
		t.Fatalf("want one log line about the link, got %q", logged.String())
	}
	Keep(putNamed(t, t.TempDir(), "download.bin", body), w.SHA256)
	if _, err := os.Stat(filepath.Join(elsewhere, w.SHA256+".partial")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the write-back wrote through the link: %v", err)
	}
}

func TestArchiveManifestUsesMemberGrammarAndHeldDirectory(t *testing.T) {
	folder := fixture(t)
	member := filepath.Join(folder, "models")
	if err := os.Mkdir(member, 0700); err != nil {
		t.Fatal(err)
	}
	w := wantOf([]byte("archive manifest fixture"))
	upper := strings.ToUpper(w.SHA256)
	lines := upper + " *./valid.gguf\r\n" +
		upper + "  ../outside.gguf\n" +
		upper + "  sub/../outside.gguf\n" +
		upper + "  invalid:stream\n" +
		upper + "  sub\\invalid.gguf\n"
	if err := os.WriteFile(filepath.Join(member, sumsName), []byte(lines), 0600); err != nil {
		t.Fatal(err)
	}
	got := readSums(folder, "models")
	if len(got) != 1 || got["models/valid.gguf"] != w.SHA256 {
		t.Fatalf("manifest member grammar or digest canonicalization changed: %v", got)
	}
	junction(t, filepath.Join(folder, "linked-models"), member)
	if got := readSums(folder, "linked-models"); len(got) != 0 {
		t.Fatalf("manifest opened through a linked directory: %v", got)
	}
}

// A queued source whose bytes changed is hashed before streaming: nothing,
// not even a partial, is written to the archive.
func TestChangedSourceWritesNothing(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("abcdefghij"), 1000)
	w := wantOf(body)
	source := putNamed(t, t.TempDir(), "download.zip", sameSizeWrong(body, len(body)-1))
	entry := mustEntry(t, folder, w.SHA256)
	if err := os.MkdirAll(filepath.Dir(entry), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writePartial(source, entry+".partial", w.SHA256, w.Bytes); !errors.Is(err, errDiffers) {
		t.Fatalf("a changed source was not refused: %v", err)
	}
	for _, name := range []string{entry, entry + ".partial"} {
		if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s was written: %v", name, err)
		}
	}
}

// The owner's case: a model kept on the NAS under its own name in a nested
// folder is listed by the walk Fill starts, copied, hashed and handed over,
// and its SHA-256 is recorded against the file.
func TestRenamedModelInANestedFolderFillsTheDownload(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("qwen weights "), 8192)
	w := wantOf(body)
	file := putNamed(t, folder, "models/qwen/x.gguf", body)
	if Holds([]Want{w}) {
		t.Fatal("a file was held before any listing")
	}
	got, ok := fill(t, w)
	if !ok || !bytes.Equal(got, body) {
		t.Fatal("the named file did not fill the download")
	}
	if !Holds([]Want{w}) {
		t.Fatal("Holds misses a model held under another name")
	}
	index, _ := readIndex(folder)
	if entry, _ := listed(index, "models/qwen/x.gguf"); entry.SHA256 != w.SHA256 {
		t.Fatalf("the matching copy's SHA-256 is not recorded: %+v", entry)
	}
	if kept, _ := os.ReadFile(file); !bytes.Equal(kept, body) {
		t.Fatal("the person's file changed")
	}
	if _, err := os.Stat(filepath.Join(folder, "sha256")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the archive gained a content-name copy: %v", err)
	}
}

// A Hugging Face blobs/<hex> name and an Ollama sha256-<hex> name come
// before a same-size file chosen by size alone; with the size unknown only
// the hex names are candidates.
func TestHashNamedFilesAreChosenBeforeSizeMatches(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("blob "), 4096)
	w := wantOf(body)
	putNamed(t, folder, "a/first.gguf", sameSizeWrong(body, 1))
	hf := putNamed(t, folder, "hub/models--org--name/blobs/"+w.SHA256, body)
	ollama := putNamed(t, folder, "ollama/models/blobs/sha256-"+w.SHA256, body)
	look(t)
	found := sources(context.Background(), folder, w, 3)
	if len(found) != 3 || found[0].path != hf || found[1].path != ollama || filepath.Base(found[2].path) != "first.gguf" {
		t.Fatalf("lookup order %+v", found)
	}
	found = sources(context.Background(), folder, Want{SHA256: w.SHA256}, 3)
	if len(found) != 2 || found[0].path != hf || found[1].path != ollama {
		t.Fatalf("lookup order with the size unknown %+v", found)
	}
}

// A same-size file with other bytes is rejected and its SHA-256 recorded;
// the next candidate fills. A later run skips the recorded file without
// reading it until its time changes.
func TestSameSizeWrongFileIsRejectedRememberedAndTheNextTried(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("reviewed model "), 4096)
	w := wantOf(body)
	wrong := putNamed(t, folder, "a/wrong.gguf", sameSizeWrong(body, 7))
	putNamed(t, folder, "b/right.gguf", body)
	look(t)
	if got, ok := fill(t, w); !ok || !bytes.Equal(got, body) {
		t.Fatal("the next candidate did not fill")
	}
	index, _ := readIndex(folder)
	recorded, _ := listed(index, "a/wrong.gguf")
	if !validSHA256Hex(recorded.SHA256) || recorded.SHA256 == w.SHA256 {
		t.Fatalf("the rejected file's SHA-256 is not remembered: %+v", recorded)
	}
	forgetRun()
	found := sources(context.Background(), folder, w, 2)
	if len(found) != 1 || filepath.Base(found[0].path) != "right.gguf" {
		t.Fatalf("a remembered wrong file is offered again: %+v", found)
	}
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(wrong, later, later); err != nil {
		t.Fatal(err)
	}
	if found := sources(context.Background(), folder, w, 2); len(found) != 2 {
		t.Fatalf("a changed file stays skipped until a walk: %+v", found)
	}
	index = look(t)
	if relisted, _ := listed(index, "a/wrong.gguf"); relisted.SHA256 != "" {
		t.Fatalf("the walk kept a SHA-256 for a changed file: %+v", relisted)
	}
}

// Two rejected copies send the download to its sources; a third candidate
// is never read, and the next listing lets the archive be tried again.
func TestTwoRejectedCopiesSendTheDownloadToItsSources(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("network only "), 4096)
	w := wantOf(body)
	putNamed(t, folder, "a.gguf", sameSizeWrong(body, 1))
	putNamed(t, folder, "b.gguf", sameSizeWrong(body, 2))
	putNamed(t, folder, "c.gguf", sameSizeWrong(body, 3))
	look(t)
	if _, ok := fill(t, w); ok {
		t.Fatal("a wrong file filled the download")
	}
	index, _ := readIndex(folder)
	if third, _ := listed(index, "c.gguf"); third.SHA256 != "" {
		t.Fatal("a third candidate was read")
	}
	if copiesLeft(w.SHA256) != 0 {
		t.Fatal("a spent download is tried again in this process")
	}
	look(t)
	if copiesLeft(w.SHA256) == 0 {
		t.Fatal("a new listing did not let the download try the archive again")
	}
}

// The walk enters no reparse point and no hidden or NAS service folder, and
// stops at its depth and entry bounds, keeping what it listed.
func TestWalkSkipsLinksServiceFoldersAndKeepsItsBounds(t *testing.T) {
	folder := fixture(t)
	for _, name := range []string{"@eaDir/a.gguf", "#recycle/b.gguf", "#snapshot/c.gguf", ".snapshot/d.gguf", "$RECYCLE.BIN/e.gguf", "models/.hidden.gguf"} {
		putNamed(t, folder, name, []byte("skip me"))
	}
	putNamed(t, folder, "models/kept.gguf", []byte("kept"))
	outside := filepath.Join(t.TempDir(), "outside")
	putNamed(t, outside, "linked.gguf", []byte("through a link"))
	junction(t, filepath.Join(folder, "linked"), outside)
	deep := ""
	for i := 1; i <= walkDepth+1; i++ {
		deep += "d" + strconv.Itoa(i) + "/"
		putNamed(t, folder, deep+"at"+strconv.Itoa(i)+".gguf", []byte("deep"))
	}
	index := look(t)
	var names []string
	for _, file := range index.Files {
		names = append(names, file.Path)
	}
	want := []string{"models/kept.gguf"}
	for i, prefix := 1, ""; i <= walkDepth; i++ {
		prefix += "d" + strconv.Itoa(i) + "/"
		want = append(want, prefix+"at"+strconv.Itoa(i)+".gguf")
	}
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Fatalf("listed %q\nwant %q", names, want)
	}
	if !index.Incomplete || index.Limit != "depth" {
		t.Fatalf("the depth bound is not marked: %+v", index)
	}
	saved := walkEntries
	walkEntries = 3
	t.Cleanup(func() { walkEntries = saved })
	index = look(t)
	if !index.Incomplete || index.Limit != "files" || len(index.Files) > 3 {
		t.Fatalf("the entry bound kept %d files, %+v", len(index.Files), index.Limit)
	}
}

// A listing entry whose path leaves the folder is dropped, and a listing of
// another folder is not used.
func TestIndexDropsPathsOutsideTheFolder(t *testing.T) {
	raw := []byte(`{"folder":"D:\\Archive","walked_at":"2026-11-09T10:00:00Z","files":[` +
		`{"path":"../outside.gguf","size":5,"modified":1},{"path":"a/../../b.gguf","size":5,"modified":1},` +
		`{"path":"C:/x.gguf","size":5,"modified":1},{"path":"/root.gguf","size":5,"modified":1},` +
		`{"path":"models/ok.gguf","size":5,"modified":1}]}`)
	index, ok := parseIndex(raw, `D:\Archive`)
	if !ok || len(index.Files) != 1 || index.Files[0].Path != "models/ok.gguf" {
		t.Fatalf("parsed %+v %v", index.Files, ok)
	}
	if _, ok := parseIndex(raw, `D:\Other`); ok {
		t.Fatal("a listing of another folder was used")
	}
	if _, ok := parseIndex([]byte("{not json"), `D:\Archive`); ok {
		t.Fatal("an unreadable listing was used")
	}
}

// The write-back writes no content-name copy of a SHA-256 the listing holds
// under another name, and nothing at all with Keep a copy of new downloads
// here off.
func TestWriteBackSkipsHeldHashesAndKeepOff(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("held "), 4096)
	w := wantOf(body)
	held := putNamed(t, folder, "models/held.gguf", body)
	look(t)
	info, _ := os.Stat(held)
	recordHash(folder, "models/held.gguf", info, w.SHA256)
	Keep(putNamed(t, t.TempDir(), "download.gguf", body), w.SHA256)
	if _, err := os.Stat(mustEntry(t, folder, w.SHA256)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a held SHA-256 was written again: %v", err)
	}
	if n := queueLen(t); n != 0 {
		t.Fatalf("the held download stayed queued: %d", n)
	}
	if err := SetKeep(context.Background(), false); err != nil || KeepCopies() {
		t.Fatalf("keep off: %v", err)
	}
	other := []byte("a new download")
	ow := wantOf(other)
	Keep(putNamed(t, t.TempDir(), "other.gguf", other), ow.SHA256)
	if _, err := os.Stat(mustEntry(t, folder, ow.SHA256)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("keep off still wrote the archive: %v", err)
	}
	if n := queueLen(t); n != 0 {
		t.Fatalf("keep off kept a queue: %d", n)
	}
	if err := SetKeep(context.Background(), true); err != nil || !KeepCopies() {
		t.Fatalf("keep on: %v", err)
	}
	if err := SetFolder(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	path, _ := indexPath()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the listing outlived the setting: %v", err)
	}
}

// LookFor walks once for a file with no listed candidate, then not again
// within ten minutes.
func TestLookForWalksAtMostEveryTenMinutes(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("asked "), 4096)
	w := wantOf(body)
	putNamed(t, folder, "x.gguf", body)
	if done := LookFor([]Want{w}); done == nil {
		t.Fatal("the ask did not walk")
	}
	if !Holds([]Want{w}) {
		t.Fatal("the walk did not list the file")
	}
	if done := LookFor([]Want{wantOf([]byte("not in the folder"))}); done != nil {
		t.Fatal("a second walk started within ten minutes")
	}
}

// A listed file reached through a junction or symbolic link at an
// intermediate folder is refused with one log line, though the listing
// named it before the link appeared (reviewer B1, 2026-11-09).
func TestListedFileBehindAnIntermediateLinkIsRefused(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("linked weights "), 4096)
	w := wantOf(body)
	putNamed(t, folder, "models/qwen/x.gguf", body)
	look(t)
	if len(sources(context.Background(), folder, w, 1)) != 1 {
		t.Fatal("the listed file is not a candidate before the link")
	}
	elsewhere := t.TempDir()
	putNamed(t, elsewhere, "qwen/x.gguf", body)
	if err := os.RemoveAll(filepath.Join(folder, "models")); err != nil {
		t.Fatal(err)
	}
	junction(t, filepath.Join(folder, "models"), elsewhere)
	var logged bytes.Buffer
	saved := log.Writer()
	log.SetOutput(&logged)
	defer log.SetOutput(saved)
	if found := sources(context.Background(), folder, w, 2); len(found) != 0 {
		t.Fatalf("a file behind an intermediate link was offered: %+v", found)
	}
	if lines := strings.Count(logged.String(), "\n"); lines != 1 || !strings.Contains(logged.String(), "link") {
		t.Fatalf("want one log line about the link, got %q", logged.String())
	}
}

// The write-back stops before its rename when Keep a copy of new downloads
// here is turned off meanwhile, and never replaces a content-name entry
// that appeared while it wrote.
func TestWriteBackRechecksKeepAndNeverOverwritesAnAppearedEntry(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("kept "), 4096)
	w := wantOf(body)
	source := putNamed(t, t.TempDir(), "download.gguf", body)
	entry := mustEntry(t, folder, w.SHA256)
	if err := SetKeep(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if err := writeEntry(source, folder, w.SHA256, w.Bytes); !errors.Is(err, errStopped) {
		t.Fatalf("a stopped write-back renamed: %v", err)
	}
	for _, name := range []string{entry, entry + ".partial"} {
		if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s stayed after keep was turned off: %v", name, err)
		}
	}
	if err := SetKeep(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	appeared := []byte("someone else's bytes")
	probes := 0
	if err := os.MkdirAll(filepath.Dir(entry), 0o700); err != nil {
		t.Fatal(err)
	}
	beforeRename = func() {
		probes++
		if err := os.WriteFile(entry, appeared, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { beforeRename = nil })
	if err := writeEntry(source, folder, w.SHA256, w.Bytes); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(entry); probes != 1 || !bytes.Equal(got, appeared) {
		t.Fatalf("an entry that appeared was replaced (hook ran %d times)", probes)
	}
	if _, err := os.Stat(entry + ".partial"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the partial stayed: %v", err)
	}
}

// A delivered download is queued for the archive by its name: a queue link
// would be a second name of a file that later becomes a model folder's,
// which protecting that folder refuses.
func TestKeepQueuesByNameWithNoSecondName(t *testing.T) {
	folder := fixture(t)
	// An absent archive keeps the queue, as a sleeping share does.
	if err := os.RemoveAll(folder); err != nil {
		t.Fatal(err)
	}
	body := []byte("delivered model bytes")
	w := wantOf(body)
	staged := putNamed(t, t.TempDir(), "model.safetensors", body)
	Keep(staged, w.SHA256)
	queue, err := queueDir()
	if err != nil {
		t.Fatal(err)
	}
	queued := readQueue(queue)
	if len(queued) != 1 || queued[0].source != staged {
		t.Fatalf("the download was not queued by its name: %+v", queued)
	}
	stagedInfo, err := os.Stat(staged)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(queue)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		info, err := os.Stat(filepath.Join(queue, e.Name()))
		if err == nil && os.SameFile(info, stagedInfo) {
			t.Fatalf("the queue holds another name of the download: %s", e.Name())
		}
	}
}

// zeros writes a file of size zero bytes without holding them in memory and
// returns its SHA-256.
func zeros(t *testing.T, size int64) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "download.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return path, hex.EncodeToString(h.Sum(nil))
}

// The archive keeps copies of 50 MB and more: it holds models.
func TestCopiesUnderFiftyMegabytesAreSkipped(t *testing.T) {
	folder := fixture(t)
	keepMinBytes = 50 << 20
	small, smallSHA := zeros(t, 50<<20-1)
	KeepFor(small, smallSHA, "app")
	if n := queueLen(t); n != 0 {
		t.Fatalf("a copy under 50 MB was queued: %d", n)
	}
	if _, err := os.Stat(mustEntry(t, folder, smallSHA)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a copy under 50 MB was written: %v", err)
	}
	large, largeSHA := zeros(t, 50<<20)
	KeepFor(large, largeSHA, "app")
	if info, err := os.Stat(mustEntry(t, folder, largeSHA)); err != nil || info.Size() != 50<<20 {
		t.Fatalf("a 50 MB copy was not written: %v", err)
	}
}

// A copy that would leave the archive's volume with less than the smaller
// of 10% of its size and 50 GB free is skipped with one log line and leaves
// the queue; the archive is never evicted from.
func TestFreeSpaceReserveSkipsACopy(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("model "), 4096)
	w := wantOf(body)
	existing := putNamed(t, folder, "models/old.gguf", []byte("kept forever"))
	for _, c := range []struct {
		name        string
		free, total int64
		kept        bool
	}{
		{"small volume below 10%", 10<<30 + w.Bytes - 1, 100 << 30, false},
		{"small volume at 10%", 10<<30 + w.Bytes, 100 << 30, true},
		{"large volume below 50 GB", 50<<30 + w.Bytes - 1, 10 << 40, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			volumeSpace = func(string) (int64, int64, error) { return c.free, c.total, nil }
			entry := mustEntry(t, folder, w.SHA256)
			os.Remove(entry)
			var logged bytes.Buffer
			saved := log.Writer()
			log.SetOutput(&logged)
			KeepFor(putNamed(t, t.TempDir(), "download.bin", body), w.SHA256, "app")
			log.SetOutput(saved)
			_, err := os.Stat(entry)
			if c.kept != (err == nil) {
				t.Fatalf("kept %v, entry %v", c.kept, err)
			}
			if !c.kept {
				if lines := strings.Count(logged.String(), "\n"); lines != 1 || !strings.Contains(logged.String(), "free-space reserve") {
					t.Fatalf("want one log line about the reserve, got %q", logged.String())
				}
				if _, err := os.Stat(entry + ".partial"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("a skipped copy left a partial: %v", err)
				}
			}
			if n := queueLen(t); n != 0 {
				t.Fatalf("the queue kept %d entries", n)
			}
		})
	}
	if _, err := os.Stat(existing); err != nil {
		t.Fatalf("the reserve evicted a file: %v", err)
	}
}

// OA's content-name entry whose bytes or size differ moves to
// sha256\.bad\<hash> with one log line, and the download goes on to its
// sources.
func TestMismatchedEntryIsRenamedToBad(t *testing.T) {
	folder := fixture(t)
	for _, c := range []struct {
		name  string
		entry func([]byte) []byte
	}{
		{"bytes", func(body []byte) []byte { return sameSizeWrong(body, 9) }},
		{"size", func(body []byte) []byte { return append(bytes.Clone(body), '!') }},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := bytes.Repeat([]byte("entry "+c.name), 2048)
			w := wantOf(body)
			wrong := c.entry(body)
			entry := putEntry(t, folder, w.SHA256, wrong)
			var logged bytes.Buffer
			saved := log.Writer()
			log.SetOutput(&logged)
			_, ok := fill(t, w)
			log.SetOutput(saved)
			if ok {
				t.Fatal("a mismatched entry filled the download")
			}
			bad, _ := badPath(folder, w.SHA256)
			if bad != filepath.Join(folder, "sha256", ".bad", w.SHA256) {
				t.Fatalf("bad path %s", bad)
			}
			if got, err := os.ReadFile(bad); err != nil || !bytes.Equal(got, wrong) {
				t.Fatalf("the entry was not moved to %s: %v", bad, err)
			}
			if _, err := os.Stat(entry); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the mismatched entry stayed: %v", err)
			}
			if lines := strings.Count(logged.String(), "\n"); lines != 1 || !strings.Contains(logged.String(), ".bad") {
				t.Fatalf("want one log line about the move, got %q", logged.String())
			}
			// The verified download takes the content name.
			Keep(putNamed(t, t.TempDir(), "download.bin", body), w.SHA256)
			if got, _ := os.ReadFile(entry); !bytes.Equal(got, body) {
				t.Fatal("the verified download did not take the content name")
			}
		})
	}
}

func TestQuarantinePreservesReplacementAfterMismatch(t *testing.T) {
	for _, wrongSize := range []bool{false, true} {
		t.Run(strconv.FormatBool(wrongSize), func(t *testing.T) {
			folder := fixture(t)
			body := bytes.Repeat([]byte("expected archive member"), 1024)
			w := wantOf(body)
			wrong := sameSizeWrong(body, 4)
			if wrongSize {
				wrong = append(wrong, '!')
			}
			entry := putEntry(t, folder, w.SHA256, wrong)
			old := entry + ".displaced"
			saved := beforeQuarantine
			t.Cleanup(func() { beforeQuarantine = saved })
			calls := 0
			beforeQuarantine = func() {
				calls++
				if err := os.Rename(entry, old); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(entry, body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, ok := fill(t, w); ok || calls != 1 {
				t.Fatalf("mismatch must fall back exactly once: %d %v", calls, ok)
			}
			if got, err := os.ReadFile(entry); err != nil || !bytes.Equal(got, body) {
				t.Fatalf("replacement was changed: %v", err)
			}
			if got, err := os.ReadFile(old); err != nil || !bytes.Equal(got, wrong) {
				t.Fatalf("displaced original was changed: %v", err)
			}
			bad, _ := badPath(folder, w.SHA256)
			if _, err := os.Stat(bad); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the replacement reached quarantine: %v", err)
			}
		})
	}
}

func TestQuarantineKeepsOccupiedBadDestination(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("occupied quarantine"), 1024)
	w := wantOf(body)
	wrong := sameSizeWrong(body, 7)
	entry := putEntry(t, folder, w.SHA256, wrong)
	bad, _ := badPath(folder, w.SHA256)
	if err := os.MkdirAll(filepath.Dir(bad), 0700); err != nil {
		t.Fatal(err)
	}
	const previous = "previous observation"
	if err := os.WriteFile(bad, []byte(previous), 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok := fill(t, w); ok {
		t.Fatal("corrupt bytes filled the download")
	}
	if got, err := os.ReadFile(entry); err != nil || !bytes.Equal(got, wrong) {
		t.Fatalf("the checked source was changed: %v", err)
	}
	if got, err := os.ReadFile(bad); err != nil || string(got) != previous {
		t.Fatalf("the occupied destination was changed: %v", err)
	}
}

func TestRejectedManagedMemberStillDiscoversUnindexedManifestCopy(t *testing.T) {
	folder := fixture(t)
	look(t)
	body := bytes.Repeat([]byte("personal archive copy"), 1024)
	w := wantOf(body)
	entry := putEntry(t, folder, w.SHA256, append(bytes.Clone(body), '!'))
	personal := putNamed(t, folder, "models/personal.gguf", body)
	putNamed(t, folder, "models/SHA256SUMS", []byte(w.SHA256+"  personal.gguf\n"))
	removeIndex()
	forgetRun()
	saved := beforeQuarantine
	t.Cleanup(func() { beforeQuarantine = saved })
	replacement := sameSizeWrong(body, 9)
	attempts := 0
	beforeQuarantine = func() {
		attempts++
		if attempts != 1 {
			t.Fatal("the discovered listing retried the rejected managed entry")
		}
		if err := os.Rename(entry, entry+".displaced"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(entry, replacement, 0600); err != nil {
			t.Fatal(err)
		}
	}
	dst := filepath.Join(t.TempDir(), "partial")
	if n, ok := Fill(t.Context(), w, dst, nil); !ok || n != w.Bytes || attempts != 1 {
		t.Fatalf("the unindexed personal copy did not fill: %d %v after %d attempts", n, ok, attempts)
	}
	for path, want := range map[string][]byte{dst: body, personal: body, entry: replacement} {
		if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("unexpected bytes at %s: %v", path, err)
		}
	}
}

// Every archive path is built from a validated lowercase 64-hex digest; any
// other name reaches no file.
func TestPathsAreBuiltOnlyFromAValidatedDigest(t *testing.T) {
	folder := fixture(t)
	good := wantOf([]byte("good")).SHA256
	for _, sha := range []string{"", "..", `..\..\Windows`, "../../etc/passwd", strings.ToUpper(good), good[:63], good + "0",
		"sha256:" + good, good[:62] + "/x", good[:62] + `\x`, good[:63] + ".", good[:63] + "g"} {
		if _, err := entryPath(folder, sha); !errors.Is(err, errBadDigest) {
			t.Fatalf("entryPath(%q) = %v", sha, err)
		}
		if _, err := badPath(folder, sha); !errors.Is(err, errBadDigest) {
			t.Fatalf("badPath(%q) = %v", sha, err)
		}
		KeepFor(putNamed(t, t.TempDir(), "download.bin", []byte("bytes")), sha, "app")
		if n := queueLen(t); n != 0 {
			t.Fatalf("KeepFor(%q) queued a copy", sha)
		}
		if _, ok := fill(t, Want{SHA256: sha}); ok {
			t.Fatalf("Fill(%q) filled a download", sha)
		}
		if sha == strings.ToUpper(good) {
			// A service digest is lowercased before it is validated.
			continue
		}
		Service{}.Keep("op", putNamed(t, t.TempDir(), "download.bin", []byte("bytes")), "sha256:"+sha)
		if _, ok := (Service{}).Fill(context.Background(), "sha256:"+sha, 0, filepath.Join(t.TempDir(), "p"), nil); ok {
			t.Fatalf("Service.Fill(%q) filled a download", sha)
		}
		if n := queueLen(t); n != 0 {
			t.Fatalf("Service.Keep(%q) queued a copy", sha)
		}
	}
	var written []string
	filepath.WalkDir(folder, func(path string, d os.DirEntry, err error) error {
		if path != folder {
			written = append(written, path)
		}
		return nil
	})
	if len(written) != 0 {
		t.Fatalf("invalid digests wrote into the archive: %v", written)
	}
	if _, ok := digestHex("md5:" + good); ok {
		t.Fatal("another algorithm was accepted")
	}
}

// The runtime counts the bytes it writes for each app and reads the
// archive's size from the listing.
func TestUsageCountsBytesWrittenPerApp(t *testing.T) {
	folder := fixture(t)
	body := bytes.Repeat([]byte("counted "), 4096)
	w := wantOf(body)
	service := Service{App: func(operation string) string { return "app-of-" + operation }}
	service.Keep("op1", putNamed(t, t.TempDir(), "download.bin", body), "sha256:"+w.SHA256)
	// An entry already there is not written again and not counted.
	service.Keep("op2", putNamed(t, t.TempDir(), "download.bin", body), "sha256:"+w.SHA256)
	look(t)
	listed, written := Usage()
	if written["app-of-op1"] != w.Bytes || len(written) != 1 {
		t.Fatalf("written %v", written)
	}
	if listed != w.Bytes {
		t.Fatalf("listed %d bytes, want %d", listed, w.Bytes)
	}
	_ = folder
}

// watchedEntry is an archive file whose reads are slow, counted and sized.
type watchedEntry struct {
	f        entryFile
	delay    time.Duration
	block    chan struct{}
	mu       sync.Mutex
	sizes    []int
	inFlight int
	most     int
}

func (e *watchedEntry) ReadAt(p []byte, off int64) (int, error) {
	e.mu.Lock()
	e.sizes = append(e.sizes, len(p))
	e.inFlight++
	e.most = max(e.most, e.inFlight)
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.inFlight--
		e.mu.Unlock()
	}()
	if e.block != nil {
		<-e.block
	}
	time.Sleep(e.delay)
	return e.f.ReadAt(p, off)
}

func (e *watchedEntry) Close() error { return e.f.Close() }

func watchEntries(t *testing.T, delay time.Duration, block chan struct{}) *watchedEntry {
	t.Helper()
	watched := &watchedEntry{delay: delay, block: block}
	saved := archiveRead
	archiveRead = func(held *restoreFile) entryFile {
		watched.f = held
		return watched
	}
	t.Cleanup(func() { archiveRead = saved })
	return watched
}

// The download service's fill reads the archive in reads of readSize bytes,
// several in flight while earlier bytes are hashed and written, and reports
// progress by time: a share answers each read a round trip late, and one
// read at a time left the link idle (2026-11-11).
func TestFillReadsAheadInLargeReadsAndReportsProgressByTime(t *testing.T) {
	folder := fixture(t)
	body := testBytes(12<<20 + 5)
	w := wantOf(body)
	putEntry(t, folder, w.SHA256, body)
	watched := watchEntries(t, 20*time.Millisecond, nil)
	dst := filepath.Join(t.TempDir(), "partial")
	var reports int
	n, ok := Service{}.Fill(context.Background(), "sha256:"+w.SHA256, w.Bytes, dst, func(done, total int64) { reports++ })
	if !ok || n != w.Bytes {
		t.Fatalf("the fill failed: %d %v", n, ok)
	}
	if got, _ := os.ReadFile(dst); !bytes.Equal(got, body) {
		t.Fatal("the filled file differs from the archive entry")
	}
	chunks := (len(body) + readSize - 1) / readSize
	// Fill can return while speculative reads beyond EOF finish. Capture
	// their observations under the same lock used by ReadAt.
	watched.mu.Lock()
	sizes, most := slices.Clone(watched.sizes), watched.most
	watched.mu.Unlock()
	if len(sizes) < chunks {
		t.Fatalf("%d reads cannot have read %d chunks", len(sizes), chunks)
	}
	for _, size := range sizes {
		if size != readSize {
			t.Fatalf("an archive read asked for %d bytes, want %d: %v", size, readSize, sizes)
		}
	}
	if most < 2 {
		t.Fatalf("at most %d archive read was in flight; reads must run ahead", most)
	}
	// 13 reads of 20 ms, eight at a time, take about 40 ms: well inside one
	// progressEvery.
	if reports < 1 || reports > 2 {
		t.Fatalf("%d progress reports for %d chunks; progress is reported by time", reports, chunks)
	}
}

// A read ahead that does not answer within stallLimit fails the fill, and
// the partial file is removed.
func TestStalledReadAheadFailsTheFill(t *testing.T) {
	folder := fixture(t)
	saved := stallLimit
	stallLimit = 50 * time.Millisecond
	t.Cleanup(func() { stallLimit = saved })
	body := testBytes(3 << 20)
	w := wantOf(body)
	putEntry(t, folder, w.SHA256, body)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	watchEntries(t, 0, block)
	start := time.Now()
	if _, ok := fill(t, w); ok {
		t.Fatal("a silent archive filled the download")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the stall limit did not bound the fill")
	}
}

type blockedArchiveEntry struct {
	release, readEnded, closed chan struct{}
}

func (e *blockedArchiveEntry) ReadAt(p []byte, offset int64) (int, error) {
	if offset == 0 {
		<-e.release
		close(e.readEnded)
	}
	return 0, io.EOF
}

func (e *blockedArchiveEntry) Close() error {
	<-e.readEnded
	close(e.closed)
	return nil
}

func TestStalledArchiveReadHandsOffInputClose(t *testing.T) {
	savedLimit := stallLimit
	stallLimit = 50 * time.Millisecond
	entry := &blockedArchiveEntry{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	defer func() { stallLimit = savedLimit }()
	released := false
	defer func() {
		if !released {
			close(entry.release)
		}
	}()
	dst := filepath.Join(t.TempDir(), "partial")
	done := make(chan error, 1)
	go func() {
		_, err := copyArchiveEntry(t.Context(), func() (entryFile, error) { return entry, nil }, dst, "", 1, nil, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, errStalled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		close(entry.release)
		released = true
		<-done
		t.Fatal("archive fill waited for a stalled read during input close")
	}
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stalled copy left a partial", err)
	}
	close(entry.release)
	released = true
	select {
	case <-entry.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("the archive input was not closed when its reads finished")
	}
}

// testBytes is n bytes that differ from chunk to chunk.
func testBytes(n int) []byte {
	body := make([]byte, n)
	for i := range body {
		body[i] = byte(i*7 + i>>20)
	}
	return body
}

// A copy written into the archive says so in one log line naming its SHA-256
// and size, and no path (R34 live test D5: the copy-in wrote no line, so a
// person could not tell the archive copy happened).
func TestWriteBackLogsTheKeptCopy(t *testing.T) {
	folder := fixture(t)
	var lines []string
	savedLog := logf
	logf = func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	t.Cleanup(func() { logf = savedLog })
	body := bytes.Repeat([]byte("kept bytes"), 1000)
	w := wantOf(body)
	source := putNamed(t, t.TempDir(), "download.gguf", body)
	Keep(source, w.SHA256)
	if kept, err := os.ReadFile(mustEntry(t, folder, w.SHA256)); err != nil || !bytes.Equal(kept, body) {
		t.Fatalf("the copy was not written: %v", err)
	}
	want := fmt.Sprintf("kept %s: %d bytes copied in", w.SHA256, len(body))
	if !slices.Contains(lines, want) {
		t.Fatalf("log lines %q lack %q", lines, want)
	}
	for _, line := range lines {
		if strings.Contains(line, folder) || strings.Contains(line, source) {
			t.Fatalf("a log line names a path: %q", line)
		}
	}
}
