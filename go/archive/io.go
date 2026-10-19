package archive

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
)

// stallLimit rejects an archive read or write that moves no byte for
// this long: a share that went to sleep mid-copy hands the download back to
// the download service instead of holding it.
var stallLimit = 30 * time.Second

var errStalled = errors.New("the download archive stopped answering")

// buffer is the copy buffer for archive files; each Read or Write
// below runs on its own goroutine, so a large buffer keeps those few.
const buffer = 1 << 20

// stallFile is an archive-side file whose Read and Write give up after
// stallLimit. An abandoned call keeps its own buffer, so the caller's
// buffer is never written after the call returned.
type stallFile struct {
	f       *os.File
	stalled atomic.Bool
}

type ioResult struct {
	n   int
	err error
}

func (s *stallFile) wait(call func() (int, error)) (int, error) {
	if s.stalled.Load() {
		return 0, errStalled
	}
	done := make(chan ioResult, 1)
	go func() {
		n, err := call()
		done <- ioResult{n, err}
	}()
	timer := time.NewTimer(stallLimit)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.n, r.err
	case <-timer.C:
		s.stalled.Store(true)
		return 0, errStalled
	}
}

func (s *stallFile) Read(p []byte) (int, error) {
	buf := make([]byte, len(p))
	n, err := s.wait(func() (int, error) { return s.f.Read(buf) })
	copy(p, buf[:n])
	return n, err
}

func (s *stallFile) Write(p []byte) (int, error) {
	buf := append([]byte(nil), p...)
	return s.wait(func() (int, error) { return s.f.Write(buf) })
}

// readAhead is how many archive reads of readSize bytes Fill keeps in flight.
// With one read at a time, each read waits a network round trip and the hash
// and local write wait behind it. On the owner's share a 138 MiB file filled
// in 4.9-5.5 s that way, with eight reads in flight in 2.4-4.8 s, and
// Windows copied it in 1.6-3.8 s (2026-11-11).
var (
	readAhead = 8
	readSize  = 1 << 20
)

// entryFile is an archive file Fill reads.
type entryFile interface {
	io.ReaderAt
	io.Closer
}

// Tests can observe Fill's reads without replacing its native file admission.
var archiveRead = func(held *restoreFile) entryFile { return held }

type aheadChunk struct {
	data []byte
	err  error
}

// aheadReader reads f from an offset in order, with up to readAhead reads of
// readSize bytes in flight, each on its own goroutine and buffer, so the
// network, the hash and the local write overlap. Waiting longer than
// stallLimit for the next read is errStalled; an abandoned read keeps its
// own buffer. ctx ends the wait.
type aheadReader struct {
	ctx    context.Context
	f      io.ReaderAt
	reads  *sync.WaitGroup
	next   int64
	queue  []chan aheadChunk
	cur    []byte
	err    error
	ending bool
}

func newAheadReader(ctx context.Context, f io.ReaderAt, offset int64, reads *sync.WaitGroup) *aheadReader {
	return &aheadReader{ctx: ctx, f: f, reads: reads, next: offset}
}

func (r *aheadReader) issue() {
	for !r.ending && len(r.queue) < readAhead {
		reply := make(chan aheadChunk, 1)
		offset := r.next
		r.next += int64(readSize)
		r.reads.Add(1)
		go func() {
			defer r.reads.Done()
			buf := make([]byte, readSize)
			n, err := r.f.ReadAt(buf, offset)
			if errors.Is(err, io.EOF) {
				err = nil
			}
			reply <- aheadChunk{buf[:n], err}
		}()
		r.queue = append(r.queue, reply)
	}
}

func (r *aheadReader) Read(p []byte) (int, error) {
	for len(r.cur) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		r.issue()
		timer := time.NewTimer(stallLimit)
		var chunk aheadChunk
		select {
		case chunk = <-r.queue[0]:
			timer.Stop()
		case <-timer.C:
			r.err = errStalled
			return 0, r.err
		case <-r.ctx.Done():
			timer.Stop()
			r.err = r.ctx.Err()
			return 0, r.err
		}
		r.queue = r.queue[1:]
		r.cur = chunk.data
		switch {
		case chunk.err != nil:
			r.err = chunk.err
			r.ending = true
		case len(chunk.data) < readSize:
			// ReadAt fills its buffer unless the file ends.
			r.err = io.EOF
			r.ending = true
		}
	}
	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

// openWithin is os.OpenFile bounded by stallLimit. A file that opens
// after the limit is closed by the abandoned call.
func openWithin(path string, flag int, perm os.FileMode) (*os.File, error) {
	type result struct {
		f   *os.File
		err error
	}
	done := make(chan result, 1)
	var abandoned atomic.Bool
	go func() {
		f, err := os.OpenFile(path, flag, perm)
		if err == nil && abandoned.Load() {
			f.Close()
			return
		}
		done <- result{f, err}
	}()
	timer := time.NewTimer(stallLimit)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.f, r.err
	case <-timer.C:
		abandoned.Store(true)
		select {
		case r := <-done:
			if r.f != nil {
				r.f.Close()
			}
		default:
		}
		return nil, errStalled
	}
}

// IsNetwork reports whether the folder is a share or a network
// drive. The Windows answer reads no file; elsewhere a slow answer counts
// as a network folder.
func IsNetwork(folder string) bool {
	done := make(chan bool, 1)
	go func() { done <- errors.Is(identity.LocalPath(folder), identity.ErrNotLocalPath) }()
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case network := <-done:
		return network
	case <-timer.C:
		return true
	}
}

// The write-back queue, <state>\downloads\archive-out, holds per download a
// <sha>.source naming the downloaded file (or a hard link <sha> an earlier
// release queued) and <sha>.at, the Unix time it was queued. While the
// archive is unreachable a link can hold a deleted download's bytes; the
// queue drops entries older than queueMaxAge, then the oldest links beyond
// queueMaxBytes.
var (
	queueMaxAge   = 7 * 24 * time.Hour
	queueMaxBytes = int64(32 << 30)
)

type queued struct {
	sha, pending, source, app string
	at                        time.Time
	held                      int64
}

func queueDir() (string, error) {
	dir, err := stateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "downloads", "archive-out"), nil
}

func stampQueued(queue, sha string) {
	stamp := filepath.Join(queue, sha+".at")
	if _, err := os.Stat(stamp); errors.Is(err, os.ErrNotExist) {
		os.WriteFile(stamp, []byte(strconv.FormatInt(time.Now().Unix(), 10)), 0600)
	}
}

// readQueue lists the queued downloads, oldest first.
func readQueue(queue string) []queued {
	entries, err := os.ReadDir(queue)
	if err != nil {
		return nil
	}
	var items []queued
	for _, entry := range entries {
		name := entry.Name()
		sha := strings.TrimSuffix(name, ".source")
		if !validSHA256Hex(sha) {
			continue
		}
		item := queued{sha: sha, pending: filepath.Join(queue, name), source: filepath.Join(queue, name)}
		if name != sha {
			raw, err := os.ReadFile(item.pending)
			if err != nil {
				continue
			}
			item.source = string(raw)
		} else if info, err := os.Stat(item.pending); err == nil {
			item.held = info.Size()
		}
		if raw, err := os.ReadFile(filepath.Join(queue, sha+".app")); err == nil {
			item.app = cleanApp(string(raw))
		}
		item.at = time.Now()
		if raw, err := os.ReadFile(filepath.Join(queue, sha+".at")); err == nil {
			if unix, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); err == nil {
				item.at = time.Unix(unix, 0)
			}
		} else {
			stampQueued(queue, sha)
		}
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].at.Before(items[j].at) })
	return items
}

func dropQueued(queue string, item queued, reason string) {
	logf("dropped %s from the write-back queue: %s", item.sha, reason)
	removeQueued(queue, item)
}

func removeQueued(queue string, item queued) {
	os.Remove(item.pending)
	os.Remove(filepath.Join(queue, item.sha+".app"))
	os.Remove(filepath.Join(queue, item.sha+".at"))
}

// pruneQueue applies the age and size limits and returns what stays.
func pruneQueue(queue string, items []queued) []queued {
	var kept []queued
	var held int64
	for _, item := range items {
		if time.Since(item.at) > queueMaxAge {
			dropQueued(queue, item, "it waited longer than a week")
			continue
		}
		kept = append(kept, item)
		held += item.held
	}
	for i := 0; i < len(kept) && held > queueMaxBytes; {
		if kept[i].held == 0 {
			i++
			continue
		}
		dropQueued(queue, kept[i], "the queue holds more than 32 GB")
		held -= kept[i].held
		kept = append(kept[:i], kept[i+1:]...)
	}
	return kept
}
