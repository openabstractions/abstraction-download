package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	cas "github.com/openabstractions/abstraction-cas/go"
	"github.com/openabstractions/abstraction-identity/fileid"
)

// The download archive's listing (ruling of 2026-11-09 in
// backlog/network-storage-tier-nas-for-models-and-files.md). A NAS keeps
// models under human names and in provider-specific cache layouts. The
// walk lists the folder's names, sizes and modification times, opening no
// file, and saves them in download-archive-index.json beside the setting.
// Fill, Holds and the Settings row read that stored listing. A walk starts
// when the person chooses a folder or presses Look again, and when a Setup
// page, a Setup plan or a download (LookFor) wants a file with no listed
// candidate while the last walk is older than ten minutes, and when the
// runtime's inventory (inventory.go) finds no listing. No timer starts one.
//
// Names and recorded hashes only choose a candidate. A candidate's bytes are
// copied into the download's partial file and hashed (copyArchiveEntry); the
// copy's own SHA-256 decides, and is recorded against the file's path, size
// and time.

const indexName = "download-archive-index.json"

// indexLimit bounds the stored listing; 50,000 entries fit well
// inside it.
const indexLimit = 16 << 20

var (
	walkDepth   = 8
	walkEntries = 50000
	walkTime    = 30 * time.Second
	// walkStale is the age after which a Setup page or plan that wants
	// a pin with no listed candidate looks again.
	walkStale = 10 * time.Minute
	// indexMinBytes is the smallest file the listing keeps: every
	// pinned download is larger, and small files only grow the index.
	indexMinBytes int64 = 1 << 20
)

// copiesPerWant is how many candidate files a pin copies in one run
// before it goes to the network.
const copiesPerWant = 2

type Listing struct {
	Folder     string    `json:"folder"`
	WalkedAt   time.Time `json:"walked_at"`
	Incomplete bool      `json:"incomplete,omitempty"`
	// Limit names the bound that stopped the walk: "files", "depth", "time"
	// or "stall".
	Limit string `json:"limit,omitempty"`
	Files []File `json:"files"`
}

// File is one listed regular file. SHA256 is recorded once OA
// has read the whole file and holds while Size and Modified are unchanged.
type File struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified"`
	SHA256   string `json:"sha256,omitempty"`
}

func indexPath() (string, error) {
	dir, err := stateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, indexName), nil
}

// validRel parses the archive index and SHA256SUMS member grammar. Actual
// access is resolved under the selected archive's open native directory.
func validRel(rel string) bool {
	if rel == "." || !fs.ValidPath(rel) {
		return false
	}
	for _, name := range strings.Split(rel, "/") {
		if fileid.SafeName(name) != nil {
			return false
		}
	}
	return true
}

// parseIndex is the stored listing of folder, or false for an index
// of another folder or one that does not parse. Entries that leave the
// folder or carry an invalid hash are dropped.
func parseIndex(raw []byte, folder string) (Listing, bool) {
	var index Listing
	if len(raw) == 0 || json.Unmarshal(raw, &index) != nil || index.Folder == "" || index.Folder != folder {
		return Listing{}, false
	}
	kept := index.Files[:0]
	for _, file := range index.Files {
		if validRel(file.Path) && file.Size > 0 && (file.SHA256 == "" || validSHA256Hex(file.SHA256)) {
			kept = append(kept, file)
		}
	}
	index.Files = kept
	return index, true
}

// indexCache keeps the last parsed listing while its file is
// unchanged: Setup's overview asks for every model offer.
var indexCache struct {
	sync.Mutex
	folder string
	size   int64
	mod    time.Time
	index  Listing
	ok     bool
}

// readIndex is the stored listing of folder.
func readIndex(folder string) (Listing, bool) {
	if folder == "" {
		return Listing{}, false
	}
	path, err := indexPath()
	if err != nil {
		return Listing{}, false
	}
	info, err := os.Stat(path)
	if err != nil {
		return Listing{}, false
	}
	c := &indexCache
	c.Lock()
	defer c.Unlock()
	if c.folder == folder && c.size == info.Size() && c.mod.Equal(info.ModTime()) {
		return c.index, c.ok
	}
	raw, err := cas.ReadLimit(path, indexLimit)
	if err != nil {
		return Listing{}, false
	}
	index, ok := parseIndex(raw, folder)
	c.folder, c.size, c.mod, c.index, c.ok = folder, info.Size(), info.ModTime(), index, ok
	return index, ok
}

func forgetIndex() {
	c := &indexCache
	c.Lock()
	c.folder, c.ok = "", false
	c.Unlock()
}

// removeIndex deletes the stored listing; the setting moved.
func removeIndex() {
	path, err := indexPath()
	if err != nil {
		return
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		logf("the listing was not removed: %v", err)
	}
	forgetIndex()
}

var errIndexUnchanged = errors.New("the archive listing is unchanged")

// changeIndex edits the stored listing of folder through cas. edit
// returning errIndexUnchanged leaves the file as it is.
func changeIndex(edit func(current Listing, ok bool) (Listing, error), folder string) error {
	path, err := indexPath()
	if err != nil {
		return err
	}
	err = cas.ChangeLimitContext(context.Background(), path, indexLimit, func(raw []byte) ([]byte, error) {
		current, ok := parseIndex(raw, folder)
		next, err := edit(current, ok)
		if err != nil {
			return nil, err
		}
		return json.Marshal(next)
	})
	forgetIndex()
	if errors.Is(err, errIndexUnchanged) {
		return nil
	}
	return err
}

// relIn is file's listed path under folder.
// recordHash records sha, the SHA-256 of every byte of file, against
// the size and time info gives. A file the listing does not hold is added.
func recordHash(folder, rel string, info os.FileInfo, sha string) {
	if !validRel(rel) || info == nil || !validSHA256Hex(sha) {
		return
	}
	err := changeIndex(func(index Listing, ok bool) (Listing, error) {
		if !ok {
			return index, errIndexUnchanged
		}
		entry := File{Path: rel, Size: info.Size(), Modified: info.ModTime().UnixNano(), SHA256: sha}
		at, found := slices.BinarySearchFunc(index.Files, rel, func(f File, rel string) int { return strings.Compare(f.Path, rel) })
		switch {
		case found && index.Files[at] == entry:
			return index, errIndexUnchanged
		case found:
			index.Files[at] = entry
		default:
			index.Files = slices.Insert(index.Files, at, entry)
		}
		return index, nil
	}, folder)
	if err != nil {
		logf("the SHA-256 of %s was not recorded: %v", filepath.Join(folder, rel), err)
	}
}

// forgetHash drops the SHA-256 recorded for file, whose size or time
// changed since; the inventory hashes it again.
func forgetHash(folder string, file File) {
	err := changeIndex(func(index Listing, ok bool) (Listing, error) {
		if !ok {
			return index, errIndexUnchanged
		}
		at, found := slices.BinarySearchFunc(index.Files, file.Path, func(f File, rel string) int { return strings.Compare(f.Path, rel) })
		if !found || index.Files[at] != file {
			return index, errIndexUnchanged
		}
		index.Files[at].SHA256 = ""
		return index, nil
	}, folder)
	if err != nil {
		logf("the changed file %s keeps its old SHA-256 in the listing: %v", file.Path, err)
		return
	}
	resumeInventory()
}

// listedCandidates is the stored listing's candidates for w, in lookup
// order, without touching the folder: the content name sha256/<hex>, then a
// file whose recorded SHA-256 is w's, then a file whose name holds w's
// lowercase hex, then files of w's size with no recorded SHA-256, in path
// order. A file of another size, one whose recorded SHA-256 differs, or one
// this process rejected, is skipped. With w's size unknown, files are
// chosen by name and recorded SHA-256 alone.
func listedCandidates(folder string, index Listing, w Want) []File {
	return listedFor(folder, index, w, false)
}

// listedFor is listedCandidates; with recorded it also returns, last,
// the same-size files whose recorded SHA-256 differs, for a caller that
// stats them and keeps only those whose size or time changed since.
func listedFor(folder string, index Listing, w Want, recorded bool) []File {
	sha := w.hex()
	if sha == "" {
		return nil
	}
	content := "sha256/" + sha
	var named, hashed, byName, bySize, differing []File
	for _, file := range index.Files {
		if !w.sizeFits(file.Size) {
			continue
		}
		if _, spent := rejected.Load(filepath.Join(folder, filepath.FromSlash(file.Path))); spent {
			continue
		}
		switch {
		case file.SHA256 != "" && file.SHA256 != sha:
			if recorded && w.Bytes > 0 {
				differing = append(differing, file)
			}
		case file.Path == content:
			named = append(named, file)
		case file.SHA256 == sha:
			hashed = append(hashed, file)
		case strings.Contains(file.Path, sha):
			byName = append(byName, file)
		case w.Bytes > 0:
			bySize = append(bySize, file)
		}
	}
	return slices.Concat(named, hashed, byName, bySize, differing)
}

// candidate is a candidate file stat'ed with w's size.
type candidate struct {
	path, relative, key string
	info                os.FileInfo
}

// openArchiveMember bounds the metadata and native relative open together.
// Read calls retain the exact file and ancestor handles for the caller.
func openArchiveMember(ctx context.Context, folder, relative string, read bool, timeout time.Duration) (*restoreFile, os.FileInfo, string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	type result struct {
		file *restoreFile
		info os.FileInfo
		key  string
		err  error
	}
	done := make(chan result)
	go func() {
		var got result
		got.file, got.err = openDownloadMember(ctx, folder, relative, read)
		if got.err == nil {
			got.info, got.err = got.file.Stat()
		}
		if got.err == nil {
			got.key, got.err = archiveOccurrence(got.file)
		}
		var nativeStatus interface{ Errno() syscall.Errno }
		if errors.As(got.err, &nativeStatus) {
			got.err = nativeStatus.Errno()
		}
		if got.file != nil && (!read || got.err != nil) {
			got.err = errors.Join(got.err, got.file.Close())
			got.file = nil
		}
		select {
		case done <- got:
		case <-ctx.Done():
			if got.file != nil {
				_ = got.file.Close()
			}
		}
	}()
	select {
	case got := <-done:
		return got.file, got.info, got.key, got.err
	case <-ctx.Done():
		return nil, nil, "", errSlow
	}
}

func archiveOccurrence(member *restoreFile) (string, error) {
	var keys []string
	for _, file := range append(slices.Clone(member.ancestors), member.File) {
		key, err := fileid.FileKeyOf(file)
		if err != nil || key == "" {
			return "", errors.Join(err, errArchiveOccurrence)
		}
		keys = append(keys, key)
	}
	info, err := member.Stat()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s\x00%d\x00%d", strings.Join(keys, "\x00"), info.Size(), info.ModTime().UnixNano()), nil
}

// sources are at most limit candidate files for w, in lookup order, each
// stat'ed under the probe bound and ctx: <folder>\sha256\<hex> first, then
// the stored listing's. A candidate reached through a junction or symbolic
// link at any level is refused; a listed file whose recorded SHA-256
// differs is a candidate again only once its size or time changed. A
// folder that does not answer is left quiet for a minute.
func sources(ctx context.Context, folder string, w Want, limit int) []candidate {
	found, _ := scanSources(ctx, folder, w, limit, false)
	return found
}

func scanSources(ctx context.Context, folder string, w Want, limit int, skipManaged bool) ([]candidate, bool) {
	sha := w.hex()
	if folder == "" || limit <= 0 || !validSHA256Hex(sha) || quiet(folder) {
		return nil, false
	}
	var found []candidate
	managedRejected := false
	look := func(relative string, listed *File) bool {
		file := filepath.Join(folder, filepath.FromSlash(relative))
		if _, spent := rejected.Load(file); spent {
			managedRejected = managedRejected || listed == nil
			return true
		}
		_, info, key, err := openArchiveMember(ctx, folder, relative, false, probeTimeout)
		if errors.Is(err, errLink) || errors.Is(err, fileid.ErrLink) {
			reject(file, err)
			managedRejected = managedRejected || listed == nil
			return true
		}
		if errors.Is(err, errSlow) {
			silence(folder)
			return false
		}
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				logf("skipped %s: %v", file, err)
			}
			return true
		}
		if !info.Mode().IsRegular() || !w.sizeFits(info.Size()) {
			if listed == nil && info.Mode().IsRegular() {
				// OA's own content-name entry of the wrong size moves aside.
				quarantine(ctx, folder, sha, key, errSizeDiffers)
				managedRejected = true
				return true
			}
			logf("skipped %s: its size differs from the download", file)
			return true
		}
		if listed != nil && listed.SHA256 != "" && (info.Size() != listed.Size || info.ModTime().UnixNano() != listed.Modified) {
			// The file changed since its SHA-256 was recorded.
			forgetHash(folder, *listed)
		} else if listed != nil && listed.SHA256 != "" && listed.SHA256 != sha {
			return true
		}
		found = append(found, candidate{file, relative, key, info})
		return len(found) < limit
	}
	content := "sha256/" + sha
	if !skipManaged && !look(content, nil) {
		return found, managedRejected
	}
	index, _ := readIndex(folder)
	for _, file := range listedFor(folder, index, w, true) {
		if file.Path == content {
			continue
		}
		if !look(file.Path, &file) {
			break
		}
	}
	return found, managedRejected
}

// copies counts this process's candidate copies per download; gaveUp holds
// the downloads whose candidates are spent, which go to the network until
// the folder is listed again or the entry is written.
var (
	copiesMu sync.Mutex
	copies   = map[string]int{}
	gaveUp   = map[string]bool{}
)

func copiesLeft(sha string) int {
	copiesMu.Lock()
	defer copiesMu.Unlock()
	if gaveUp[sha] {
		return 0
	}
	return max(copiesPerWant-copies[sha], 0)
}

func countCopy(sha string) {
	copiesMu.Lock()
	copies[sha]++
	copiesMu.Unlock()
}

func giveUp(sha string) {
	copiesMu.Lock()
	gaveUp[sha] = true
	copiesMu.Unlock()
}

// renew lets sha use the archive again: its entry was just written.
func renew(sha string) {
	copiesMu.Lock()
	delete(copies, sha)
	delete(gaveUp, sha)
	copiesMu.Unlock()
}

// renewAll lets every download use the archive again: the folder was listed
// again or changed, and files rejected before may have been replaced.
func renewAll() {
	copiesMu.Lock()
	clear(copies)
	clear(gaveUp)
	copiesMu.Unlock()
	rejected.Clear()
}

// skipName is a name the walk never enters or lists: hidden files,
// $RECYCLE.BIN and NAS service folders such as @eaDir, #recycle, #snapshot
// and .snapshot.
func skipName(name string) bool {
	return name == "" || strings.ContainsRune(".$@#", rune(name[0]))
}

type walkOutcome struct {
	files []File
	// sums are the SHA-256s the folder's SHA256SUMS files state, by
	// folder-relative path.
	sums       map[string]string
	incomplete bool
	limit      string
	// unanswered is a folder whose own listing failed or stalled.
	unanswered bool
}

var errWalkTime = errors.New("the walk reached its time limit")

// walk lists folder breadth first: names, sizes and times, opening
// no file. It follows no symlink, junction or other reparse point, skips
// skipName names, and stops at the depth, entry, time and stall
// bounds, keeping what it listed.
func walk(folder string) walkOutcome {
	deadline := time.Now().Add(walkTime)
	type pending struct {
		rel   string
		depth int
	}
	var out walkOutcome
	halted := false
	// stop marks the listing incomplete; every bound but depth ends the walk.
	stop := func(limit string) {
		out.incomplete = true
		if out.limit == "" || out.limit == "depth" {
			out.limit = limit
		}
		halted = halted || limit != "depth"
	}
	queue := []pending{{"", 0}}
	entries := 0
	for len(queue) > 0 && !halted {
		dir := queue[0]
		queue = queue[1:]
		listed, err := readDirWithin(filepath.Join(folder, filepath.FromSlash(dir.rel)), deadline, walkEntries-entries+1)
		if dir.rel == "" && err != nil && len(listed) == 0 {
			logf("the folder did not answer: %v", err)
			return walkOutcome{unanswered: true}
		}
		switch {
		case errors.Is(err, errWalkTime):
			stop("time")
		case errors.Is(err, errStalled):
			stop("stall")
		case err != nil:
			logf("skipped %s: %v", dir.rel, err)
		}
		for _, info := range listed {
			if entries++; entries > walkEntries {
				stop("files")
				break
			}
			name := info.Name()
			if skipName(name) || reparse(info) {
				continue
			}
			rel := path.Join(dir.rel, name)
			switch mode := info.Mode(); {
			case mode.IsDir():
				if dir.depth+1 > walkDepth {
					stop("depth")
					continue
				}
				queue = append(queue, pending{rel, dir.depth + 1})
			case mode.IsRegular() && name == sumsName && info.Size() <= sumsLimit:
				for rel, sha := range readSums(folder, dir.rel) {
					if out.sums == nil {
						out.sums = map[string]string{}
					}
					out.sums[rel] = sha
				}
			case mode.IsRegular() && info.Size() >= indexMinBytes && validRel(rel):
				out.files = append(out.files, File{Path: rel, Size: info.Size(), Modified: info.ModTime().UnixNano()})
			}
		}
	}
	slices.SortFunc(out.files, func(a, b File) int { return strings.Compare(a.Path, b.Path) })
	for i, file := range out.files {
		out.files[i].SHA256 = out.sums[file.Path]
	}
	return out
}

// sumsName is the manifest sha256sum writes: one "<hex>  <name>" line per
// file. A folder that holds one has its files' SHA-256s seeded from it, so
// the inventory reads none of them; the copy at serve time still hashes
// every byte and records what it computed (copyArchiveEntry, Fill).
const sumsName = "SHA256SUMS"

// sumsLimit bounds the manifest the walk reads.
const sumsLimit = 1 << 20

// readSums reads the SHA256SUMS file in the folder-relative folder rel,
// and answers each stated SHA-256 by the folder-relative path of its file.
// Lines in sha256sum's text and binary forms are read; a line naming a path
// that leaves dir or carrying no valid digest is skipped. A manifest that
// cannot be read seeds nothing.
func readSums(folder, rel string) map[string]string {
	if rel != "" && !validRel(rel) {
		return nil
	}
	member := path.Join(rel, sumsName)
	file := filepath.Join(folder, filepath.FromSlash(member))
	in, _, _, err := openArchiveMember(context.Background(), folder, member, true, probeTimeout)
	if err != nil {
		logf("skipped %s: %v", file, err)
		return nil
	}
	defer func() { go in.Close() }()
	raw, err := io.ReadAll(io.LimitReader(&stallFile{f: in.File}, sumsLimit+1))
	if err != nil || len(raw) > sumsLimit {
		logf("skipped %s: it cannot be read whole", file)
		return nil
	}
	sums := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if len(line) < 67 || line[64] != ' ' || line[65] != ' ' && line[65] != '*' {
			continue
		}
		sha, name := normalizeSHA256Hex(line[:64]), strings.TrimPrefix(line[66:], "./")
		if !validSHA256Hex(sha) || !validRel(name) {
			continue
		}
		listed := path.Join(rel, name)
		if !validRel(listed) {
			continue
		}
		sums[listed] = sha
	}
	return sums
}

// readDirWithin lists dir in batches, each bounded by stallLimit and
// all by deadline, and returns at most about limit entries. An abandoned
// batch keeps its own slice.
func readDirWithin(dir string, deadline time.Time, limit int) ([]os.FileInfo, error) {
	f, err := openWithin(dir, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { go f.Close() }()
	type batch struct {
		infos []os.FileInfo
		err   error
	}
	var listed []os.FileInfo
	for len(listed) < limit {
		if time.Now().After(deadline) {
			return listed, errWalkTime
		}
		done := make(chan batch, 1)
		go func() {
			entries, err := f.ReadDir(256)
			infos := make([]os.FileInfo, 0, len(entries))
			for _, entry := range entries {
				if info, err := entry.Info(); err == nil {
					infos = append(infos, info)
				}
			}
			done <- batch{infos, err}
		}()
		timer := time.NewTimer(stallLimit)
		select {
		case got := <-done:
			timer.Stop()
			listed = append(listed, got.infos...)
			if errors.Is(got.err, io.EOF) {
				return listed, nil
			}
			if got.err != nil {
				return listed, got.err
			}
		case <-timer.C:
			return listed, errStalled
		}
	}
	return listed, nil
}

// The walks: at most one per folder at a time, and the last outcome of each
// in this process.
var walks = struct {
	sync.Mutex
	running map[string]chan struct{}
	started map[string]time.Time
	silent  map[string]bool
}{running: map[string]chan struct{}{}, started: map[string]time.Time{}, silent: map[string]bool{}}

// walkAsync runs a walk in the background. Tests run it in the
// calling goroutine.
var walkAsync = true

// Look lists the archive folder again: the person chose it or pressed
// Look again. The returned channel closes when the walk ends; it is nil
// with no folder set.
func Look() <-chan struct{} {
	folder := Folder()
	if folder == "" {
		return nil
	}
	return startWalk(folder)
}

// LookFor lists the archive folder when one of wants has no listed
// candidate and the last walk began more than ten minutes ago. The Setup
// page, its plan and Fill call it. The channel closes when the walk ends; it
// is nil when no walk runs.
func LookFor(downloads []Want) <-chan struct{} {
	folder := Folder()
	if folder == "" || len(downloads) == 0 || quiet(folder) {
		return nil
	}
	index, ok := readIndex(folder)
	missing := !ok
	for _, download := range downloads {
		if missing {
			break
		}
		missing = len(listedCandidates(folder, index, download)) == 0
	}
	if !missing {
		return nil
	}
	walks.Lock()
	running, busy := walks.running[folder]
	last := walks.started[folder]
	walks.Unlock()
	if busy {
		return running
	}
	if ok && index.WalkedAt.After(last) {
		last = index.WalkedAt
	}
	if !last.IsZero() && time.Since(last) < walkStale {
		return nil
	}
	return startWalk(folder)
}

func startWalk(folder string) <-chan struct{} {
	walks.Lock()
	if running, busy := walks.running[folder]; busy {
		walks.Unlock()
		return running
	}
	done := make(chan struct{})
	walks.running[folder] = done
	walks.started[folder] = time.Now()
	walks.Unlock()
	run := func() {
		silent := runWalk(folder)
		walks.Lock()
		walks.silent[folder] = silent
		delete(walks.running, folder)
		walks.Unlock()
		close(done)
	}
	if walkAsync {
		go run()
	} else {
		run()
	}
	return done
}

var errWalkLost = errors.New("another walk or setting change came first")

// runWalk walks folder and saves the listing, carrying over the
// recorded hashes whose size and time are unchanged. A walk whose folder
// was changed, or whose listing another walk replaced meanwhile, drops its
// result. It returns whether the folder did not answer.
func runWalk(folder string) bool {
	before, _ := readIndex(folder)
	out := walk(folder)
	if out.unanswered {
		return true
	}
	err := changeIndex(func(current Listing, ok bool) (Listing, error) {
		if Folder() != folder || ok != !before.WalkedAt.IsZero() || ok && !current.WalkedAt.Equal(before.WalkedAt) {
			return current, errWalkLost
		}
		recorded := map[string]File{}
		for _, file := range current.Files {
			recorded[file.Path] = file
		}
		for i, file := range out.files {
			// A hash recorded for the unchanged file stands; a stated one
			// from SHA256SUMS seeds a file that has none.
			if old, held := recorded[file.Path]; held && old.Size == file.Size && old.Modified == file.Modified && old.SHA256 != "" {
				out.files[i].SHA256 = old.SHA256
			}
		}
		return Listing{Folder: folder, WalkedAt: time.Now().UTC(), Incomplete: out.incomplete, Limit: out.limit, Files: out.files}, nil
	}, folder)
	if errors.Is(err, errWalkLost) {
		logf("dropped a listing of %s: %v", folder, err)
	} else if err != nil {
		logf("the listing of %s was not saved: %v", folder, err)
	} else {
		renewAll()
		resumeInventory()
	}
	return false
}

// Walking reports whether this process is listing the archive folder now,
// and whether its last walk of the folder found it silent.
func Walking() (looking, unanswered bool) {
	folder := Folder()
	if folder == "" {
		return false, false
	}
	walks.Lock()
	defer walks.Unlock()
	_, looking = walks.running[folder]
	return looking, walks.silent[folder]
}

// ReadListing is the stored listing of the current archive folder; reading
// it touches no folder.
func ReadListing() (Listing, bool) {
	return readIndex(Folder())
}

// Candidates are the listing's files that may hold w, in lookup order.
func (l Listing) Candidates(w Want) []File {
	return listedCandidates(l.Folder, l, w)
}

// Holds is whether the stored listing has a candidate for every one of
// wants, under any name, so downloading them copies from the archive. It
// neither walks nor reads the folder; none is false.
func Holds(wants []Want) bool {
	if len(wants) == 0 {
		return false
	}
	index, ok := ReadListing()
	if !ok {
		return false
	}
	for _, w := range wants {
		if len(index.Candidates(w)) == 0 {
			return false
		}
	}
	return true
}

// indexHolds reports whether the stored listing records sha under a
// name other than its content name.
func indexHolds(folder, sha string) (string, bool) {
	index, ok := readIndex(folder)
	if !ok {
		return "", false
	}
	for _, file := range index.Files {
		if file.SHA256 == sha && file.Path != "sha256/"+sha {
			return file.Path, true
		}
	}
	return "", false
}
