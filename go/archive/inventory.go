package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The inventory (architect's item of 2026-11-11, DL-R44): a NAS keeps
// models under their plain names, and a name says nothing about the bytes.
// The download service hashes every listed file of 50 MB or more once, in
// the background, and records path, size, time and SHA-256 in the stored
// listing. A download that names one of those digests then finds the file
// by its recorded hash. The copy into the download still hashes every byte
// and decides (copyArchiveEntry). The person's files are only read: never
// moved, renamed or written.
//
// A recorded SHA-256 holds while the file's size and time are unchanged. The
// walk drops it for a changed file, and a lookup that finds the file
// changed forgets it (forgetHash), so the inventory hashes the file again.
//
// The pass is throttled to inventoryRate, waits while an archive copy or
// write-back runs and for inventoryQuiet after the last one, and reads with
// no GPU, engine or download slot. Only the runtime's download service runs
// it (StartInventory); the Panel walks the folder and never hashes.

var (
	// inventoryMinBytes is the smallest listed file the pass hashes: the
	// archive holds models.
	inventoryMinBytes int64 = 50 << 20
	// inventoryRate is the most bytes per second the pass reads.
	inventoryRate int64 = 16 << 20
	// inventoryQuiet is how long the pass waits after the last archive
	// copy or write-back ended.
	inventoryQuiet = 30 * time.Second
	// inventoryPoll is how often a waiting pass looks again.
	inventoryPoll = time.Second
	// inventoryAsync runs the pass in the background. Tests run it in the
	// calling goroutine.
	inventoryAsync = true
)

// activity counts the archive copies and write-backs running in this
// process; the inventory waits for them.
var activity struct {
	sync.Mutex
	running int
	last    time.Time
}

// hold marks an archive copy or write-back as running until the returned
// func is called.
func hold() func() {
	activity.Lock()
	activity.running++
	activity.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			activity.Lock()
			activity.running--
			activity.last = time.Now()
			activity.Unlock()
		})
	}
}

// busy reports whether an archive copy or write-back runs now or ended less
// than inventoryQuiet ago.
func busy() bool {
	activity.Lock()
	defer activity.Unlock()
	return activity.running > 0 || !activity.last.IsZero() && time.Since(activity.last) < inventoryQuiet
}

type inventory struct {
	mu                      sync.Mutex
	enabled, running, again bool
	stop                    chan struct{}
}

var inv inventory

// StartInventory lets this process hash the listed files in the background
// and starts a pass. The runtime's download service calls it when it
// starts; a walk that saves a new listing starts the next pass.
func StartInventory() {
	inv.mu.Lock()
	inv.enabled = true
	if inv.stop == nil {
		inv.stop = make(chan struct{})
	}
	inv.mu.Unlock()
	resumeInventory()
}

// StopInventory ends the pass at its next read and lets no new pass start.
func StopInventory() {
	inv.mu.Lock()
	inv.enabled = false
	if inv.stop != nil {
		close(inv.stop)
		inv.stop = nil
	}
	inv.mu.Unlock()
}

// inventoryOn is whether this process hashes listed files; stop closes when
// it no longer does.
func inventoryOn() (bool, <-chan struct{}) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.enabled, inv.stop
}

// resumeInventory starts a pass when this process runs the inventory; a
// pass already running runs once more.
func resumeInventory() {
	v := &inv
	v.mu.Lock()
	if !v.enabled {
		v.mu.Unlock()
		return
	}
	if v.running {
		v.again = true
		v.mu.Unlock()
		return
	}
	v.running = true
	v.mu.Unlock()
	if inventoryAsync {
		go v.run()
	} else {
		v.run()
	}
}

func (v *inventory) run() {
	for {
		// Each pass takes the current stop: a StartInventory after a
		// StopInventory runs on the new one.
		v.mu.Lock()
		stop := v.stop
		v.mu.Unlock()
		if stop != nil {
			inventoryPass(stop)
		}
		v.mu.Lock()
		if !v.again || !v.enabled {
			v.running, v.again = false, false
			v.mu.Unlock()
			return
		}
		v.again = false
		v.mu.Unlock()
	}
}

// unhashable holds the listed files a pass could not hash in this process,
// by path, with the size and time it saw; a changed file is tried again.
var unhashable sync.Map

// nextToHash is the smallest listed file of inventoryMinBytes or more with
// no recorded SHA-256 that this process has not failed to hash.
func nextToHash(folder string, index Listing) (File, bool) {
	var next File
	found := false
	for _, file := range index.Files {
		if file.SHA256 != "" || file.Size < inventoryMinBytes {
			continue
		}
		full := filepath.Join(folder, filepath.FromSlash(file.Path))
		if _, spent := rejected.Load(full); spent {
			continue
		}
		if seen, failed := unhashable.Load(full); failed && seen.(File) == file {
			continue
		}
		if !found || file.Size < next.Size {
			next, found = file, true
		}
	}
	return next, found
}

// inventoryPass hashes the listed files that have no recorded SHA-256, one
// at a time, smallest first. A folder with no listing is walked first. It
// ends when every file is hashed or skipped, when the folder does not
// answer, when the setting changes or when stop closes.
func inventoryPass(stop <-chan struct{}) {
	folder := Folder()
	if folder == "" || quiet(folder) {
		return
	}
	index, ok := readIndex(folder)
	if !ok {
		select {
		case <-startWalk(folder):
		case <-stop:
			return
		}
		if index, ok = readIndex(folder); !ok {
			return
		}
	}
	for {
		file, found := nextToHash(folder, index)
		if !found {
			return
		}
		if !waitIdle(folder, stop) {
			return
		}
		if !hashListed(folder, file, stop) {
			return
		}
		if index, ok = readIndex(folder); !ok {
			return
		}
	}
}

// waitIdle waits while an archive copy or write-back runs or ended less
// than inventoryQuiet ago. It is false when the pass should end.
func waitIdle(folder string, stop <-chan struct{}) bool {
	for busy() {
		select {
		case <-time.After(inventoryPoll):
		case <-stop:
			return false
		}
	}
	select {
	case <-stop:
		return false
	default:
	}
	return Folder() == folder
}

// stillUnhashed reports whether the listing still holds file, unchanged and
// with no recorded SHA-256: a copy that ran meanwhile may have recorded it.
func stillUnhashed(folder string, file File) bool {
	index, ok := readIndex(folder)
	if !ok {
		return false
	}
	now, held := listedEntry(index, file.Path)
	return held && now.SHA256 == "" && now.Size == file.Size && now.Modified == file.Modified
}

// listedEntry is index's entry for the folder-relative path rel.
func listedEntry(index Listing, rel string) (File, bool) {
	for _, file := range index.Files {
		if file.Path == rel {
			return file, true
		}
	}
	return File{}, false
}

// hashListed reads every byte of one listed file under the rate limit and
// records its SHA-256 when its size and time stayed the same. It is false
// when the pass should end: the folder stopped answering, the setting
// changed or stop closed.
func hashListed(folder string, file File, stop <-chan struct{}) bool {
	full := filepath.Join(folder, filepath.FromSlash(file.Path))
	skip := func(err error) bool {
		unhashable.Store(full, file)
		if !errors.Is(err, os.ErrNotExist) {
			logf("did not hash %s: %v", full, err)
		}
		return true
	}
	ctx := context.Background()
	in, before, key, err := openArchiveMember(ctx, folder, file.Path, true, probeTimeout)
	if errors.Is(err, errSlow) {
		silence(folder)
		return false
	}
	if err != nil {
		return skip(err)
	}
	defer func() { go in.Close() }()
	if !before.Mode().IsRegular() || before.Size() < inventoryMinBytes {
		return skip(errors.New("it is no longer a file of the listed size"))
	}
	reader := &stallFile{f: in.File}
	h := sha256.New()
	buf := make([]byte, buffer)
	var read, paced int64
	start := time.Now()
	for {
		if busy() {
			if !waitIdle(folder, stop) {
				return false
			}
			if !stillUnhashed(folder, file) {
				return true
			}
			start, paced = time.Now(), read
		}
		select {
		case <-stop:
			return false
		default:
		}
		n, err := reader.Read(buf)
		h.Write(buf[:n])
		read += int64(n)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, errStalled) {
			silence(folder)
			return false
		}
		if err != nil {
			return skip(err)
		}
		if due := time.Duration(float64(read-paced) / float64(inventoryRate) * float64(time.Second)); due > time.Since(start) {
			select {
			case <-time.After(due - time.Since(start)):
			case <-stop:
				return false
			}
		}
	}
	_, after, currentKey, err := openArchiveMember(ctx, folder, file.Path, false, probeTimeout)
	if err != nil || key != currentKey || read != before.Size() || after.Size() != before.Size() ||
		!after.ModTime().Equal(before.ModTime()) {
		// The file changed while it was read; the next walk lists it again.
		unhashable.Store(full, file)
		return true
	}
	sum := hex.EncodeToString(h.Sum(nil))
	recordHash(folder, file.Path, before, sum)
	logf("recorded the SHA-256 of %s", full)
	return true
}
