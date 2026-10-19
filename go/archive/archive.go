// Package archive is the download archive: a folder the person names, often
// on a NAS share, that the download service checks before the internet and
// keeps a copy of every download in (owner request 2026-11-09; rulings in
// backlog/network-storage-tier-nas-for-models-and-files.md; DL-R44).
//
// OA writes a file there under its content name, <folder>\sha256\<lowercase
// hex>; the person's own files keep their names and are found through a
// stored listing (index.go).
//
// Before a download that names its SHA-256 goes to the network, Fill looks
// for a candidate: the content name, then the stored listing's files by
// recorded SHA-256, by the hex in their name, and by the download's size. A
// candidate is copied, hashed while it is copied, into the download's own
// partial file. Only bytes whose full SHA-256 equals the digest stay there.
// The archive is never served in place, never deleted from, and never fails
// a download: an unreachable, asleep, short, unreadable or differing
// candidate logs one line, the next one is tried, and after two copies the
// download goes to its sources.
//
// After a network download is verified, Keep queues it, and a background
// writer copies it into the archive, resuming its <hex>.partial there after
// an interruption, and renames it into place. It replaces an entry only when
// the entry has the wrong size or this process rejected its bytes. With Keep
// a copy of new downloads here off, or with the listing holding the SHA-256
// under another name, nothing is written.
//
// The setting, the listing and the write-back queue live in one state folder
// (UseState, StateFor). The Panel sets the folder and walks it; the download
// service in the runtime reads them, fills downloads and writes copies back.
package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cas "github.com/openabstractions/abstraction-cas/go"
	contentdigest "github.com/openabstractions/abstraction-storage/go/digest"
)

// settingName is the state file that holds the folder.
const settingName = "download-archive.json"

// probeTimeout bounds a look at the archive: a sleeping share answers late
// or never, and a download must not wait for it.
var probeTimeout = 3 * time.Second

// writeAsync runs the write-back in the background. Tests run it in the
// calling goroutine.
var writeAsync = true

// state is the folder holding the setting, the listing and the write-back
// queue; nil or "" turns the archive off in this process.
var state atomic.Pointer[string]

var errNoState = errors.New("the download archive has no state folder in this process")

// UseState names the folder that keeps the archive's setting, listing and
// write-back queue. "" turns the archive off in this process.
func UseState(dir string) {
	if dir != "" {
		dir = filepath.Clean(dir)
	}
	if old := state.Swap(&dir); old == nil || *old != dir {
		forgetIndex()
	}
}

// StateFor is the archive's state folder for a runtime: <runtimeState>\setup
// for a runtime started with its own state folder, else the account's
// OpenAbstractions\setup configuration folder. The Panel's Setup keeps its
// state in the same folder, so both processes read one setting.
func StateFor(runtimeState string) (string, error) {
	if runtimeState != "" {
		if !filepath.IsAbs(runtimeState) {
			return "", errors.New("the runtime's state folder must be an absolute path")
		}
		return filepath.Join(filepath.Clean(runtimeState), "setup"), nil
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(config, "OpenAbstractions", "setup"), nil
}

func stateDir() (string, error) {
	dir := state.Load()
	if dir == nil || *dir == "" {
		return "", errNoState
	}
	return *dir, nil
}

type setting struct {
	Folder string `json:"folder"`
	// Keep is Keep a copy of new downloads here; absent is on.
	Keep *bool `json:"keep,omitempty"`
}

func settingPath() (string, error) {
	dir, err := stateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, settingName), nil
}

func validFolder(folder string) error {
	if len(folder) > 1024 || strings.ContainsFunc(folder, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return errors.New("the download archive folder is not a valid path")
	}
	if !filepath.IsAbs(folder) {
		return errors.New("the download archive needs a full folder path, such as D:\\Archive or \\\\nas\\share\\archive")
	}
	return nil
}

func readSetting() setting {
	path, err := settingPath()
	if err != nil {
		return setting{}
	}
	raw, err := cas.ReadLimit(path, 4096)
	if err != nil || len(raw) == 0 {
		return setting{}
	}
	var s setting
	if json.Unmarshal(raw, &s) != nil || s.Folder == "" || validFolder(s.Folder) != nil {
		return setting{}
	}
	s.Folder = filepath.Clean(s.Folder)
	return s
}

// Folder is the download archive folder the person set, or "".
func Folder() string { return readSetting().Folder }

// KeepCopies is Keep a copy of new downloads here: on unless the person
// turned it off.
func KeepCopies() bool {
	s := readSetting()
	return s.Keep == nil || *s.Keep
}

// changeSetting edits the stored setting through cas.
func changeSetting(ctx context.Context, edit func(*setting)) error {
	path, err := settingPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return cas.ChangeLimitContext(ctx, path, 4096, func(raw []byte) ([]byte, error) {
		var s setting
		if len(raw) > 0 && json.Unmarshal(raw, &s) != nil {
			s = setting{}
		}
		edit(&s)
		return json.Marshal(s)
	})
}

// SetFolder saves the download archive folder; "" stops using one. A mapped
// network drive is saved as its UNC path. Saving makes no request to the
// folder. Another folder, or none, deletes the stored listing; none also
// drops the write-back queue.
func SetFolder(ctx context.Context, folder string) error {
	folder = strings.TrimSpace(folder)
	if folder != "" {
		if err := validFolder(folder); err != nil {
			return err
		}
		folder = filepath.Clean(universalName(filepath.Clean(folder)))
	}
	before := Folder()
	if err := changeSetting(ctx, func(s *setting) { s.Folder = folder }); err != nil {
		return err
	}
	if folder != before {
		removeIndex()
		renewAll()
	}
	dropQueueWhenOff()
	return nil
}

// SetKeep saves Keep a copy of new downloads here. Off drops the write-back
// queue, so OA writes nothing in the folder.
func SetKeep(ctx context.Context, keep bool) error {
	if err := changeSetting(ctx, func(s *setting) {
		s.Keep = nil
		if !keep {
			s.Keep = &keep
		}
	}); err != nil {
		return err
	}
	dropQueueWhenOff()
	return nil
}

// Reachable reports whether the folder answers within the probe bound.
func Reachable(folder string) bool {
	info, err := statWithin(context.Background(), folder)
	return err == nil && info.IsDir()
}

var errSlow = errors.New("the download archive did not answer in time")

// quietUntil holds the time before which a folder that did not answer is
// not asked again, so a sleeping share costs one probe per minute.
var quietUntil sync.Map

const quietFor = time.Minute

func quiet(folder string) bool {
	until, ok := quietUntil.Load(folder)
	return ok && time.Now().Before(until.(time.Time))
}

func silence(folder string) {
	quietUntil.Store(folder, time.Now().Add(quietFor))
	logf("skipped %s: the folder did not answer in time", folder)
}

// statWithin is os.Stat bounded by the probe bound and ctx.
func statWithin(ctx context.Context, path string) (os.FileInfo, error) {
	return probeWithin(ctx, os.Stat, path)
}

// errLink marks a junction or symbolic link inside the archive.
var errLink = errors.New("it is a link, which the download archive does not follow")

// refuseLinks returns errLink when any of paths is a reparse point
// (junction or symbolic link). A missing path passes.
func refuseLinks(ctx context.Context, paths ...string) error {
	for _, path := range paths {
		info, err := probeWithin(ctx, os.Lstat, path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&(os.ModeSymlink|os.ModeIrregular) != 0 {
			return fmt.Errorf("%s: %w", path, errLink)
		}
	}
	return nil
}

// probeWithin runs stat bounded by probeTimeout, or by ctx's deadline when
// that comes first: the caller's deadline is the archive's too.
func probeWithin(ctx context.Context, stat func(string) (os.FileInfo, error), path string) (os.FileInfo, error) {
	type result struct {
		info os.FileInfo
		err  error
	}
	done := make(chan result, 1)
	go func() {
		info, err := stat(path)
		done <- result{info, err}
	}()
	timer := time.NewTimer(probeTimeout)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.info, r.err
	case <-timer.C:
		return nil, errSlow
	case <-ctx.Done():
		return nil, errSlow
	}
}

// logf writes the archive's log lines; tests may replace it.
var logf = func(format string, args ...any) {
	log.Printf("download archive: "+format, args...)
}

func validSHA256Hex(s string) bool {
	return contentdigest.Valid(contentdigest.Prefix + s)
}

func normalizeSHA256Hex(s string) string {
	if len(s) != hex.EncodedLen(sha256.Size) {
		return ""
	}
	return contentdigest.Normalize(s)
}

// Want is a download the archive may serve: its SHA-256 as hex, and its
// size, zero when unknown. A download of unknown size is found only by its
// content name, a recorded SHA-256 or the hex in a file's name.
type Want struct {
	SHA256 string
	Bytes  int64
}

func (w Want) hex() string { return normalizeSHA256Hex(w.SHA256) }

// sizeFits reports whether a file of size can hold w.
func (w Want) sizeFits(size int64) bool { return w.Bytes <= 0 || size == w.Bytes }

// errBadDigest refuses a path for anything but a lowercase 64-hex SHA-256.
var errBadDigest = errors.New("the download archive names files only by a lowercase 64-hex SHA-256")

// entryPath is <folder>\sha256\<sha>. Every archive path OA writes or
// moves is built here, from a validated digest and the person's folder.
func entryPath(folder, sha string) (string, error) {
	if !validSHA256Hex(sha) {
		return "", errBadDigest
	}
	return filepath.Join(folder, "sha256", sha), nil
}

// badPath is <folder>\sha256\.bad\<sha>, where an entry whose bytes differ
// from its name is moved. The walk never lists names that begin with a dot.
func badPath(folder, sha string) (string, error) {
	if !validSHA256Hex(sha) {
		return "", errBadDigest
	}
	return filepath.Join(folder, "sha256", ".bad", sha), nil
}

// quarantine moves OA's content-name entry for sha, whose bytes or size
// differ from its name, to sha256\.bad\<sha> with one log line, so the next
// write-back can put the right bytes there. An occupied destination or an
// uncertain occurrence leaves the source in place.
func quarantine(ctx context.Context, folder, sha, occurrence string, cause error) bool {
	entry, err := entryPath(folder, sha)
	if err != nil {
		return false
	}
	bad, _ := badPath(folder, sha)
	rejected.Store(entry, struct{}{})
	if beforeQuarantine != nil {
		beforeQuarantine()
	}
	if Folder() != folder {
		logf("skipped %s: %v; it stays in place: %v", entry, cause, errStopped)
		return false
	}
	if err := retireArchiveMember(ctx, folder, sha, occurrence); err != nil {
		logf("skipped %s: %v; it stays in place: %v", entry, cause, err)
		return false
	}
	logf("moved %s to %s: %v", entry, bad, cause)
	return true
}

var errArchiveOccurrence = errors.New("the checked archive member occurrence could not be held")
var beforeQuarantine func()

func retireArchiveMember(ctx context.Context, folder, sha, occurrence string) error {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	held := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- retireHeldArchiveMember(ctx, folder, sha, occurrence, func() error {
			if Folder() != folder {
				return errStopped
			}
			select {
			case held <- struct{}{}:
				return ctx.Err()
			case <-ctx.Done():
				return errSlow
			}
		})
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return errSlow
	case <-held:
		// Native custody is established. Wait for the one admitted rename's
		// answer before saying whether the archive member moved.
		return <-done
	}
}

// Prove a mismatch on the current held member when a caller has no read
// observation of its own. A queued rejection carries no file identity.
func currentCorruptMember(ctx context.Context, folder, sha string, size int64) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, stallLimit)
	defer cancel()
	held, info, key, err := openArchiveMember(ctx, folder, "sha256/"+sha, true, probeTimeout)
	if err != nil {
		return "", err
	}
	readStalled := false
	defer func() {
		if readStalled {
			go held.Close()
		} else {
			_ = held.Close()
		}
	}()
	if size >= 0 && info.Size() != size {
		return key, nil
	}
	sum := sha256.New()
	n, err := io.Copy(sum, readerContext{ctx, &stallFile{f: held.File}})
	readStalled = errors.Is(err, errStalled)
	if err != nil || n != info.Size() {
		return "", errors.Join(err, errArchiveOccurrence)
	}
	if actual := hex.EncodeToString(sum.Sum(nil)); actual == sha {
		return "", nil
	}
	after, err := archiveOccurrence(held)
	if err != nil || after != key {
		return "", errors.Join(err, errArchiveOccurrence)
	}
	return key, nil
}

// rejected holds entries whose bytes or reads failed in this process.
var rejected sync.Map

func reject(entry string, cause error) {
	rejected.Store(entry, struct{}{})
	logf("skipped %s: %v", entry, cause)
}

// Fill copies an archive file whose SHA-256 is w's into dst, hashing every
// byte, and reports the bytes it holds and whether dst now holds exactly the
// wanted file. It tries the content name and the stored listing's
// candidates; with none listed and a listing older than ten minutes it
// lists the folder first, within ctx. A failed or differing copy logs one
// line and moves to the next candidate; after two copies in this process
// the download goes to its sources. A partial dst left by an earlier
// process is rehashed and continued. Fill removes dst after a failed copy,
// except when ctx ends.
func Fill(ctx context.Context, w Want, dst string, progress func(done, total int64)) (int64, bool) {
	sha := w.hex()
	folder := Folder()
	if folder == "" || !validSHA256Hex(sha) || copiesLeft(sha) == 0 || ctx.Err() != nil {
		return 0, false
	}
	found, skipManaged := scanSources(ctx, folder, w, copiesLeft(sha), false)
	if len(found) == 0 && ctx.Err() == nil && !quiet(folder) {
		// A walk runs only in a folder that answers the probe now.
		if info, err := statWithin(ctx, folder); errors.Is(err, errSlow) {
			silence(folder)
			return 0, false
		} else if err != nil || !info.IsDir() {
			return 0, false
		}
		if done := LookFor([]Want{w}); done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				return 0, false
			}
			// A new listing clears global rejections. This Fill still
			// excludes the managed member it rejected before the walk.
			found, _ = scanSources(ctx, folder, w, copiesLeft(sha), skipManaged)
		}
	}
	for _, source := range found {
		if copiesLeft(sha) == 0 {
			break
		}
		countCopy(sha)
		size := w.Bytes
		if size <= 0 {
			size = source.info.Size()
		}
		open := func() (entryFile, error) {
			held, _, key, err := openArchiveMember(ctx, folder, source.relative, true, stallLimit)
			if err != nil {
				return nil, err
			}
			if key != source.key {
				return nil, errors.Join(errors.New("archive member changed before reading"), held.Close())
			}
			return archiveRead(held), nil
		}
		computed, err := copyArchiveEntry(ctx, open, dst, sha, size, func(done int64) {
			if progress != nil {
				progress(done, size)
			}
		}, nil)
		if ctx.Err() != nil {
			return 0, false
		}
		// The hash holds for the file's size and time only when the copy saw
		// them unchanged.
		if computed != "" {
			_, after, key, statErr := openArchiveMember(ctx, folder, source.relative, false, probeTimeout)
			if statErr == nil && key == source.key && after.Size() == source.info.Size() &&
				after.ModTime().Equal(source.info.ModTime()) {
				recordHash(folder, source.relative, source.info, computed)
			}
		}
		if err == nil {
			// Only failed copies count against the download's next fill.
			renew(sha)
			logf("served %s from %s", sha, source.path)
			return size, true
		}
		if source.relative == "sha256/"+sha && errors.Is(err, errDiffers) {
			quarantine(ctx, folder, sha, source.key, err)
			continue
		}
		reject(source.path, err)
	}
	if len(found) > 0 && copiesLeft(sha) == 0 {
		giveUp(sha)
		logf("%s has no candidate left; it downloads", sha)
	}
	return 0, false
}

// errDiffers marks archive bytes whose size or SHA-256 differs.
var errDiffers = errors.New("its bytes differ from the wanted SHA-256")

// errSizeDiffers marks a content-name entry whose size differs from the
// download.
var errSizeDiffers = errors.New("its size differs from the wanted download")

// copyArchiveEntry copies an archive member into dst, hashing every byte. Bytes already
// in dst are rehashed locally and continued; when the continued copy
// differs it is copied again from the start, since dst may hold another
// file's bytes. Reads run ahead of the hash and the local write
// (aheadReader); a read that moves nothing for stallLimit fails the copy.
// Any failure except cancellation removes dst. computed is the SHA-256 of
// the whole entry when this call read all of it from the start, else "".
func copyArchiveEntry(ctx context.Context, open func() (entryFile, error), dst, sha string, bytes int64, progress func(int64), check func(entryFile) bool) (computed string, err error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return "", err
	}
	mode := os.O_RDWR | os.O_CREATE
	if check != nil {
		// A bound restore verifies every byte of this selected file, including
		// any prefix an interrupted transfer could have kept.
		mode |= os.O_TRUNC
	}
	out, err := os.OpenFile(dst, mode, 0600)
	if err != nil {
		return "", err
	}
	defer func() {
		if out != nil {
			out.Close()
		}
		if err != nil && ctx.Err() == nil {
			os.Remove(dst)
		}
	}()
	h := sha256.New()
	offset, err := io.Copy(h, readerContext{ctx, out})
	if err != nil {
		return "", err
	}
	restart := func() error {
		if err := out.Truncate(0); err != nil {
			return err
		}
		if _, err := out.Seek(0, io.SeekStart); err != nil {
			return err
		}
		h.Reset()
		offset = 0
		return nil
	}
	if offset > bytes {
		if err := restart(); err != nil {
			return "", err
		}
	}
	in, err := open()
	if err != nil {
		return "", err
	}
	var reads sync.WaitGroup
	defer func() {
		// Windows Close can block indefinitely if it races a pending ReadAt.
		// The cleanup owns the input until every speculative read has returned.
		done := make(chan struct{})
		go func() {
			reads.Wait()
			in.Close()
			close(done)
		}()
		if errors.Is(err, errStalled) || ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(stallLimit)
		defer timer.Stop()
		select {
		case <-done:
		case <-ctx.Done():
			computed, err = "", ctx.Err()
		case <-timer.C:
			computed, err = "", errStalled
		}
	}()
	for {
		counted := &progressCounter{done: offset, report: progress}
		source := newAheadReader(ctx, in, offset, &reads)
		remaining := bytes - offset
		n, err := io.CopyBuffer(io.MultiWriter(out, h, counted), io.LimitReader(source, remaining), make([]byte, readSize))
		if err != nil {
			return "", err
		}
		// Probe for a longer source without writing its overflow byte.
		atEnd := false
		if n == remaining {
			var extra [1]byte
			_, err := io.ReadFull(source, extra[:])
			if err != nil && err != io.EOF {
				return "", err
			}
			atEnd = err == io.EOF
		}
		sum := hex.EncodeToString(h.Sum(nil))
		if offset == 0 && atEnd {
			computed = sum
		}
		if atEnd && sum == sha {
			break
		}
		if offset == 0 {
			return computed, errDiffers
		}
		if err := restart(); err != nil {
			return "", err
		}
	}
	if err := out.Sync(); err != nil {
		return computed, err
	}
	if check != nil && !check(in) {
		return "", errDiffers
	}
	closeErr := out.Close()
	out = nil
	return computed, closeErr
}

// progressEvery spaces Fill's progress reports, from which the caller renews
// its lease and records progress.
var progressEvery = 250 * time.Millisecond

// progressCounter reports the bytes copied at most once per progressEvery,
// the first chunk at once.
type progressCounter struct {
	done   int64
	last   time.Time
	report func(int64)
}

func (c *progressCounter) Write(p []byte) (int, error) {
	c.done += int64(len(p))
	if c.report != nil && time.Since(c.last) >= progressEvery {
		c.last = time.Now()
		c.report(c.done)
	}
	return len(p), nil
}

type readerContext struct {
	ctx context.Context
	r   io.Reader
}

func (r readerContext) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// keepMinBytes is the smallest download the archive keeps a copy of: the
// archive holds models (architect ruling of 2026-11-11, DL-R44).
var keepMinBytes int64 = 50 << 20

// Keep is KeepFor with no app named.
func Keep(source, sha string) { KeepFor(source, sha, "") }

// KeepFor queues a verified local file of at least 50 MB for the archive
// by its name and starts the write-back; app names the program whose
// download it was, for the bytes-written count. The file must keep its
// bytes until the copy is written; a file gone or changed by then leaves
// the queue.
func KeepFor(source, sha, app string) {
	if !validSHA256Hex(sha) || Folder() == "" || !KeepCopies() {
		return
	}
	if info, err := os.Stat(source); err != nil || !info.Mode().IsRegular() || info.Size() < keepMinBytes {
		return
	}
	queue, err := queueDir()
	if err != nil {
		return
	}
	if err := os.MkdirAll(queue, 0700); err != nil {
		logf("not kept %s: %v", sha, err)
		return
	}
	pending := filepath.Join(queue, sha)
	if _, err := os.Lstat(pending); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(pending+".source", []byte(source), 0600); err != nil {
			logf("not kept %s: %v", sha, err)
			return
		}
	}
	if app = cleanApp(app); app != "" {
		os.WriteFile(pending+".app", []byte(app), 0600)
	}
	stampQueued(queue, sha)
	ResumeWrites()
}

// cleanApp is app as the bytes-written count keeps it: printable and at
// most 256 bytes, else "".
func cleanApp(app string) string {
	if len(app) > 256 || strings.ContainsFunc(app, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return ""
	}
	return app
}

type writer struct {
	mu             sync.Mutex
	running, again bool
}

var writes writer

// ResumeWrites starts the archive write-back for queued downloads. The
// download service calls it when it starts and with every download; a
// queued copy resumes where it stopped.
func ResumeWrites() {
	w := &writes
	w.mu.Lock()
	if w.running {
		w.again = true
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()
	if writeAsync {
		go w.run()
	} else {
		w.run()
	}
}

func (w *writer) run() {
	for {
		release := hold()
		writeQueued()
		release()
		w.mu.Lock()
		if !w.again {
			w.running = false
			w.mu.Unlock()
			return
		}
		w.again = false
		w.mu.Unlock()
	}
}

// dropQueueWhenOff empties the write-back queue when no archive is set or
// Keep a copy of new downloads here is off, so it holds no disk. It writes
// nothing to the archive; only the download service does.
func dropQueueWhenOff() {
	if Folder() != "" && KeepCopies() {
		return
	}
	queue, err := queueDir()
	if err != nil {
		return
	}
	for _, item := range readQueue(queue) {
		removeQueued(queue, item)
	}
}

// writeQueued copies each queued file into the archive, oldest first,
// after the queue's age and size limits. It stops at the first unreachable
// or stalled archive, keeping the queue for the next start; with no archive
// set, or with Keep a copy of new downloads here off, it drops the queue so
// it holds no disk. A SHA-256 the listing records under another name leaves
// the queue unwritten.
func writeQueued() {
	queue, err := queueDir()
	if err != nil {
		return
	}
	items := readQueue(queue)
	folder := Folder()
	if folder == "" || !KeepCopies() {
		for _, item := range items {
			removeQueued(queue, item)
		}
		return
	}
	for _, item := range pruneQueue(queue, items) {
		if held, ok := indexHolds(folder, item.sha); ok {
			dropQueued(queue, item, "the archive holds it as "+held)
			continue
		}
		info, err := os.Stat(item.source)
		if err != nil || !info.Mode().IsRegular() {
			dropQueued(queue, item, "the downloaded file is gone")
			continue
		}
		err = writeEntryFor(item.source, folder, item.sha, info.Size(), item.app)
		if errors.Is(err, errStopped) {
			for _, rest := range readQueue(queue) {
				removeQueued(queue, rest)
			}
			return
		}
		if errors.Is(err, errDiffers) {
			dropQueued(queue, item, "the downloaded file no longer has its SHA-256")
			continue
		}
		if errors.Is(err, errNoRoom) {
			dropQueued(queue, item, err.Error())
			continue
		}
		if err != nil {
			logf("not kept %s yet: %v", item.sha, err)
			return
		}
		removeQueued(queue, item)
	}
}

// errStopped marks a write-back the person stopped while it ran: the folder
// changed or Keep a copy of new downloads here was turned off.
var errStopped = errors.New("the download archive no longer keeps copies here")

// beforeRename runs just before writeEntry's last checks; tests use it to
// change the folder while a copy is written.
var beforeRename func()

// writeEntry copies source to <folder>\sha256\<sha> through <sha>.partial,
// continuing a partial already there, and renames it into place only when
// the SHA-256 of every byte in the partial, the earlier ones read back from
// the archive, equals sha. A differing partial is removed and written again
// from the start once. Before the rename the setting is read again: a
// stopped write-back removes its partial, and an entry that appeared
// meanwhile is left as it is.
func writeEntry(source, folder, sha string, size int64) error {
	return writeEntryFor(source, folder, sha, size, "")
}

// writeEntryFor is writeEntry counting the bytes it writes for app.
func writeEntryFor(source, folder, sha string, size int64, app string) error {
	ctx := context.Background()
	entry, err := entryPath(folder, sha)
	if err != nil {
		return err
	}
	_, wasRejected := rejected.Load(entry)
	info, err := statWithin(ctx, entry)
	switch {
	case err == nil && !wasRejected && info.Mode().IsRegular() && info.Size() == size:
		return nil
	case err == nil:
		key, err := currentCorruptMember(ctx, folder, sha, size)
		if err != nil {
			return err
		}
		if key == "" {
			return nil
		}
		if !quarantine(ctx, folder, sha, key, errDiffers) {
			return nil
		}
	case !errors.Is(err, os.ErrNotExist):
		return err
	}
	if !Reachable(folder) {
		return errors.New("the folder is not reachable")
	}
	if err := CheckWriteSpace(folder, size); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(entry), 0755); err != nil {
		return err
	}
	partial := entry + ".partial"
	if err := refuseLinks(ctx, filepath.Dir(entry), entry, partial); err != nil {
		return err
	}
	err = writePartial(source, partial, sha, size)
	if errors.Is(err, errResumeDiffers) {
		os.Remove(partial)
		err = writePartial(source, partial, sha, size)
	}
	if errors.Is(err, errResumeDiffers) {
		err = errDiffers
	}
	if err != nil {
		if errors.Is(err, errDiffers) {
			os.Remove(partial)
		}
		return err
	}
	if beforeRename != nil {
		beforeRename()
	}
	if Folder() != folder || !KeepCopies() {
		os.Remove(partial)
		return errStopped
	}
	if _, err := probeWithin(ctx, os.Lstat, entry); !errors.Is(err, os.ErrNotExist) {
		os.Remove(partial)
		logf("left %s as it is: it appeared while the copy was written", entry)
		return err
	}
	if err := os.Rename(partial, entry); err != nil {
		return err
	}
	rejected.Delete(entry)
	renew(sha)
	if app != "" {
		recordWritten(app, size)
	}
	logf("kept %s: %d bytes copied in", sha, size)
	return nil
}

// errResumeDiffers marks a partial whose earlier bytes differ.
var errResumeDiffers = errors.New("the partial copy in the archive differs")

func writePartial(source, partial, sha string, size int64) error {
	// The source is hashed before any byte reaches the archive, so bytes
	// that no longer match are never written there, even briefly.
	if err := sourceMatches(source, sha, size); err != nil {
		return err
	}
	offset := int64(0)
	if info, err := statWithin(context.Background(), partial); err == nil {
		offset = info.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if offset > size {
		os.Remove(partial)
		offset = 0
	}
	out, err := openWithin(partial, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer out.Close()
	guarded := &stallFile{f: out}
	buf := make([]byte, buffer)
	h := sha256.New()
	if offset > 0 {
		n, err := io.CopyBuffer(h, io.LimitReader(guarded, offset), buf)
		if err != nil {
			return err
		}
		if n != offset {
			return errResumeDiffers
		}
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	if _, err := in.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	n, err := io.CopyBuffer(io.MultiWriter(guarded, h), io.LimitReader(in, size-offset), buf)
	if err != nil {
		return err
	}
	if offset+n != size {
		return errors.New("the downloaded file changed while it was kept")
	}
	if hex.EncodeToString(h.Sum(nil)) != sha {
		if offset > 0 {
			return errResumeDiffers
		}
		return errDiffers
	}
	if _, err := guarded.wait(func() (int, error) { return 0, out.Sync() }); err != nil {
		return err
	}
	if _, err := guarded.wait(func() (int, error) { return 0, out.Close() }); err != nil {
		return err
	}
	return nil
}

func sourceMatches(source, sha string, size int64) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, io.LimitReader(in, size+1), make([]byte, buffer))
	if err != nil {
		return err
	}
	if n != size || hex.EncodeToString(h.Sum(nil)) != sha {
		return errDiffers
	}
	return nil
}

// Service is the archive as the download service's runner uses it
// (download.DownloadArchive). Digests are "sha256:<hex>"; any other
// algorithm is neither filled nor kept. App, when set, names the program
// that submitted an operation, for the bytes-written count.
type Service struct {
	App func(operation string) string
}

func digestHex(digest string) (string, bool) {
	sha, ok := strings.CutPrefix(digest, contentdigest.Prefix)
	sha = normalizeSHA256Hex(sha)
	return sha, ok && sha != ""
}

// Fill is the package's Fill for a digest.
func (Service) Fill(ctx context.Context, digest string, size int64, partial string, progress func(done, total int64)) (int64, bool) {
	sha, ok := digestHex(digest)
	if !ok {
		return 0, false
	}
	// A folder the Panel set since the last download starts the copies
	// queued while none was set.
	ResumeWrites()
	// The inventory waits while the archive copies.
	defer hold()()
	return Fill(ctx, Want{SHA256: sha, Bytes: size}, partial, progress)
}

// Keep is the package's KeepFor for a digest and the operation's app.
func (s Service) Keep(operation, final, digest string) {
	sha, ok := digestHex(digest)
	if !ok {
		return
	}
	app := ""
	if s.App != nil {
		app = s.App(operation)
	}
	KeepFor(final, sha, app)
}
