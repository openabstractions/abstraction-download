package download

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// FilesDir is where a request that names no destination lands, relative to
// the store.
const FilesDir = "files"

// wantedDir is the drop folder: a text file put here is a request, and the
// folder answers it by renaming it.
const wantedDir = "wanted"

// requestLimit is as large as a request gets. A few hundred URLs fit; anything
// bigger is not a request and is not read.
const requestLimit = 64 << 10

// The answers. A request keeps its name and gains one of these.
const (
	Accepted = ".accepted"
	Done     = ".done"
	Failed   = ".failed"
	Refused  = ".refused"
)

// ErrRequestRefused is a dropped file this layer will not act on, with the line and
// the reason.
var ErrRequestRefused = errors.New("download: request refused")

// ParseWanted reads one dropped file as work.
//
// Lines beginning with # are ignored. A file whose first character is { is a
// spec exactly as the contract page spells it. Anything else is one download
// per line: a URL, then in either order an optional sha256:<hex> and an
// optional destination inside the store.
func ParseWanted(text string) ([]Spec, error) {
	type numbered struct {
		n    int
		text string
	}
	var lines []numbered
	for i, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if line != "" && !strings.HasPrefix(line, "#") {
			lines = append(lines, numbered{i + 1, line})
		}
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("%w: nothing to fetch", ErrRequestRefused)
	}
	if strings.HasPrefix(lines[0].text, "{") {
		var b strings.Builder
		for _, l := range lines {
			b.WriteString(l.text)
			b.WriteByte('\n')
		}
		var s Spec
		if err := json.Unmarshal([]byte(b.String()), &s); err != nil {
			return nil, fmt.Errorf("%w: not a spec: %v", ErrRequestRefused, err)
		}
		if err := wantedSpec(s); err != nil {
			return nil, fmt.Errorf("%w: spec: %v", ErrRequestRefused, err)
		}
		return []Spec{s}, nil
	}
	specs := make([]Spec, 0, len(lines))
	for _, l := range lines {
		s, err := wantedLine(l.text)
		if err != nil {
			return nil, fmt.Errorf("%w: line %d: %v", ErrRequestRefused, l.n, err)
		}
		specs = append(specs, s)
	}
	return specs, nil
}

func wantedLine(line string) (Spec, error) {
	f := strings.Fields(line)
	locator := f[0]
	if !strings.Contains(locator, "://") {
		return Spec{}, fmt.Errorf("not a URL: %s", locator)
	}
	var digest, dest string
	for _, x := range f[1:] {
		switch {
		case NormalDigest(x) != "" && digest == "":
			digest = NormalDigest(x)
		case strings.HasPrefix(strings.ToLower(x), "sha256:"):
			return Spec{}, fmt.Errorf("digest is not sha256:<64 hex>: %s", x)
		case dest == "":
			dest = x
		default:
			return Spec{}, fmt.Errorf("one destination per line: %s", x)
		}
	}
	if dest == "" {
		dest = FilesDir + "/"
	}
	if strings.HasSuffix(dest, "/") || strings.HasSuffix(dest, `\`) {
		dest += nameFrom(locator)
	}
	s := Spec{
		Artifact: Artifact{Digest: digest},
		Sources:  []Source{{Scheme: schemeFrom(locator), Locator: locator}},
		Sink:     Sink{Final: Portable(dest)},
	}
	return s, wantedSpec(s)
}

// wantedSpec is what the drop folder accepts, which is less than a record may
// carry: anyone who can write to a share can write here, and the fetching is
// done with the supervisor's authority on the supervisor's disk.
func wantedSpec(s Spec) error {
	for _, p := range []string{s.Sink.Final, s.Sink.Partial} {
		if err := wantedSink(p); err != nil {
			return err
		}
	}
	for i, src := range s.Sources {
		if src.Scheme != "http" && src.Scheme != "https" {
			return fmt.Errorf("source %d: %s is not fetched for a dropped request, only http and https", i+1, src.Scheme)
		}
		if len(src.Attrs) > 0 || len(src.Headers) > 0 {
			return fmt.Errorf("source %d: a dropped request names no credential and sets no header", i+1)
		}
	}
	return s.Validate()
}

func wantedSink(p string) error {
	switch {
	case p == "":
		return nil
	case !relativeEverywhere(p):
		return fmt.Errorf("destination is outside the store: %s", p)
	case EscapesRoot(p) != nil:
		return fmt.Errorf("destination escapes the store: %s", p)
	case ReservedSink("", p) != nil:
		return fmt.Errorf("destination is reserved by the store: %s", p)
	case intoWanted(p):
		return fmt.Errorf("destination is the drop folder itself: %s", p)
	}
	return nil
}

func intoWanted(p string) bool {
	first, _, _ := strings.Cut(path.Clean(strings.ReplaceAll(p, `\`, "/")), "/")
	return job.RootName(first) == wantedDir
}

// Wanted is a store's drop folder: how a person with no program asks.
type Wanted struct {
	Store  job.Store
	Submit func(Spec) (id string, err error)
}

// Dir is the folder, created if absent, or ErrNoLocalArea when the store has
// no directory to keep one in.
func (w Wanted) Dir() (string, error) {
	root := localRoot(w.Store)
	if root == "" {
		return "", ErrNoLocalArea
	}
	dir := filepath.Join(root, wantedDir)
	return dir, os.MkdirAll(dir, 0o755)
}

// TakeIn turns every new file in the folder into work and answers it in
// place: the file becomes <name>.accepted naming its jobs, or <name>.refused
// saying which line was wrong and why. It returns the ids of the jobs taken.
func (w Wanted) TakeIn() ([]string, error) {
	dir, entries, err := w.list()
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || isAnswered(name) || ignored(name) {
			continue
		}
		ids = append(ids, w.take(filepath.Join(dir, name))...)
	}
	return ids, nil
}

func (w Wanted) list() (string, []os.DirEntry, error) {
	dir, err := w.Dir()
	if err != nil {
		return "", nil, err
	}
	entries, err := os.ReadDir(dir)
	return dir, entries, err
}

func isAnswered(name string) bool {
	for _, s := range []string{Accepted, Done, Failed, Refused} {
		if strings.HasSuffix(name, s) {
			return true
		}
	}
	return false
}

// ignored is what editors, Finder and Explorer write into any folder they are
// shown, none of it a request.
func ignored(name string) bool {
	l := strings.ToLower(name)
	return strings.HasPrefix(name, ".") || strings.HasSuffix(name, "~") || l == "desktop.ini" || l == "thumbs.db"
}

func (w Wanted) take(p string) []string {
	st, err := os.Stat(p)
	if err != nil {
		return nil
	}
	if st.Size() > requestLimit {
		os.Rename(p, p+Refused)
		return nil
	}
	text, err := os.ReadFile(p)
	if err != nil {
		return nil
	}
	request := requestLines(string(text))
	specs, err := ParseWanted(string(text))
	if err != nil {
		w.answer(p, p+Refused, append(request, "# refused "+stamp()+": "+strings.TrimPrefix(err.Error(), ErrRequestRefused.Error()+": ")))
		return nil
	}
	if err := os.Rename(p, p+Accepted); err != nil {
		return nil
	}
	lines := append(request, "# accepted "+stamp())
	var ids []string
	for _, s := range specs {
		id, err := w.Submit(s)
		if err != nil {
			lines = append(lines, "# refused: "+err.Error())
			continue
		}
		ids = append(ids, id)
		lines = append(lines, "# job "+id+" -> "+s.Sink.Final)
	}
	if len(ids) == 0 {
		w.answer(p+Accepted, p+Refused, lines)
		return nil
	}
	write(p+Accepted, lines)
	return ids
}

// requestLines is what the person wrote: everything before the first answer.
func requestLines(text string) []string {
	var out []string
	for _, raw := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
		line := strings.TrimRight(raw, "\r")
		if strings.HasPrefix(line, "# accepted ") || strings.HasPrefix(line, "# refused ") {
			break
		}
		out = append(out, line)
	}
	return out
}

// answer writes the new name and then removes the old, so a crash between the
// two leaves the request visible twice rather than gone.
func (w Wanted) answer(from, to string, lines []string) {
	if write(to, lines) == nil {
		os.Remove(from)
	}
}

func write(p string, lines []string) error {
	tmp := filepath.Join(filepath.Dir(p), "."+filepath.Base(p)+".tmp")
	if err := os.WriteFile(tmp, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func stamp() string { return time.Now().Format(time.RFC3339) }

// Answer moves every accepted request on: to .done once every job it named
// has delivered, to .failed once one has ended without delivering, and
// otherwise rewrites its progress.
func (w Wanted) Answer() error {
	dir, entries, err := w.list()
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), Accepted) {
			w.follow(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

// follow keeps everything through the last `# job` line and regenerates what
// comes after it, so the request and its ids survive every rewrite.
func (w Wanted) follow(p string) {
	text, err := os.ReadFile(p)
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimRight(string(text), "\n"), "\n")
	last := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "# job ") {
			last = i
		}
	}
	if last < 0 {
		w.answer(p, strings.TrimSuffix(p, Accepted)+Refused, append(lines, "# refused "+stamp()+": no job was recorded"))
		return
	}
	kept := lines[:last+1]
	var status []string
	done, failed := 0, 0
	for _, line := range kept {
		if !strings.HasPrefix(line, "# job ") {
			continue
		}
		id, final, _ := strings.Cut(strings.TrimPrefix(line, "# job "), " -> ")
		s, end := w.progress(id, final)
		status = append(status, s)
		switch end {
		case Done:
			done++
		case Failed:
			failed++
		}
	}
	jobs := len(status)
	switch {
	case done == jobs:
		w.answer(p, strings.TrimSuffix(p, Accepted)+Done, append(kept, status...))
	case done+failed == jobs:
		w.answer(p, strings.TrimSuffix(p, Accepted)+Failed, append(kept, status...))
	default:
		if now := strings.Join(append(kept, status...), "\n") + "\n"; now != string(text) {
			write(p, append(kept, status...))
		}
	}
}

// progress is one job's line for the person watching, and which ending it has
// reached, if any: the bytes are at final (Done), it stopped without them
// (Failed), or "" while somebody may still finish it.
func (w Wanted) progress(id, final string) (string, string) {
	rec, err := w.Store.Load(id)
	if err != nil {
		return "# failed: " + final + " — its job is gone from the store", Failed
	}
	switch rec.State {
	case job.StateComplete, job.StateTransferred:
		line := fmt.Sprintf("# done %s: %s, %d bytes", stamp(), final, rec.Progress.Done)
		if spec, err := SpecOf(rec); err == nil && spec.Artifact.Digest != "" {
			line += ", " + spec.Artifact.Digest + " verified"
		}
		return line, Done
	case job.StateFailed, job.StateCancelled:
		return fmt.Sprintf("# failed %s: %s — %s", stamp(), final, orState(rec)), Failed
	}
	line := "# " + string(rec.State)
	if rec.Progress.Total > 0 {
		line += fmt.Sprintf(" %d%%", 100*rec.Progress.Done/rec.Progress.Total)
	}
	if rec.Error != "" {
		line += " — last attempt: " + rec.Error
	}
	return line, ""
}

func orState(rec *job.Record) string {
	if rec.Error != "" {
		return rec.Error
	}
	return string(rec.State)
}
