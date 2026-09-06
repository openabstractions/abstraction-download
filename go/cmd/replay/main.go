// replay applies a scripted sequence of operations to a job store and prints
// what an observer would have seen after each one.
//
// specread asks whether the implementations READ a spec the same way. Nothing
// asked whether they DO the same thing, and two divergences lived under that
// gap for as long as they existed: one implementation swept a paused job into
// its orphan list and the others did not, and one honoured a pause on adoption
// while another downloaded straight through it. Both are invisible to any test
// that compares vocabulary.
//
// The transcript is the surface. It carries only what a caller may branch on —
// the state, the epoch, whether a lease is live, what somebody asked for, the
// units done, whether an error was recorded, the checkpoint, and what the
// record declares it carries — and never a message, a timestamp, an owner or an
// id, because pinning those would make three languages agree forever about
// wording and clocks.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	dl "github.com/openabstractions/abstraction-download/go"
	job "github.com/openabstractions/abstraction-job/go"
	watch "github.com/openabstractions/abstraction-watch/go"
)

type replay struct {
	store  *job.FileStore
	work   string
	ids    map[string]string
	epochs map[string]int64
	subs   map[string]job.Subscription
	holds  map[string]*job.Hold
	// refused is the hosts a scenario switched off, one reason each: what the
	// window's file holds, held in memory so a driver never reads the machine.
	refused map[string]string
	out     *bufio.Writer
}

// fixture is where download/testdata/fixture.py is listening, or "" when the
// harness started no server. A driver that cannot reach one must not claim the
// wire capability: a scenario nobody ran is counted as unproven, and a scenario
// silently skipped reads as a pass.
func fixture() string { return os.Getenv("ABSTRACTION_FIXTURE") }

func capabilities() string {
	if fixture() == "" {
		return "store transfer"
	}
	return "store transfer wire wanted"
}

// models is the content-set roster: every name this implementation can read,
// and whether it may be marked critical. A record naming anything absent here
// in `critical` is refused, so the roster is the whole of what this reader
// negotiates over and it belongs where a harness can diff it against the other
// two.
func models() string {
	var lines []string
	for _, name := range job.KnownFeatures() {
		mark := "critical-ok"
		if job.NeverCritical(name) {
			mark = "never-critical"
		}
		lines = append(lines, name+" "+mark)
	}
	return strings.Join(lines, "\n")
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--capabilities" {
		fmt.Print(capabilities() + "\n")
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "--models" {
		fmt.Print(models() + "\n")
		return
	}
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: replay <workdir> <scenario> | replay --capabilities")
		os.Exit(2)
	}
	work, scenario := os.Args[1], os.Args[2]
	if err := os.MkdirAll(filepath.Join(work, "store"), 0o755); err != nil {
		fatal(err)
	}
	store, err := job.NewFileStore(filepath.Join(work, "store"))
	if err != nil {
		fatal(err)
	}
	script, err := os.ReadFile(scenario)
	if err != nil {
		fatal(err)
	}

	p := &replay{
		store:   store,
		work:    work,
		ids:     map[string]string{},
		epochs:  map[string]int64{},
		subs:    map[string]job.Subscription{},
		holds:   map[string]*job.Hold{},
		refused: map[string]string{},
		out:     bufio.NewWriter(os.Stdout),
	}
	defer p.out.Flush()

	n := 0
	for _, raw := range strings.Split(strings.ReplaceAll(string(script), "\r\n", "\n"), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		n++
		fmt.Fprintf(p.out, "%02d %s -> %s\n", n, line, p.do(strings.Fields(line)))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "replay:", err)
	os.Exit(1)
}

func (p *replay) do(f []string) string {
	switch f[0] {
	case "submit":
		return p.submit(f[1], args(f[2:]))
	case "claim":
		return p.claim(f[1], f[2], atoi(f[3]))
	case "renew":
		return p.renew(f[1], f[2], atoi(f[3]))
	case "progress":
		return p.progress(f[1], f[2], atoi(f[3]), rest(f, 4))
	case "hold":
		return p.hold(f[1])
	case "release":
		return p.release(f[1], f[2])
	case "finish":
		return p.finish(f[1], f[2], f[3])
	case "intent":
		return p.intent(f[1], f[2])
	case "recall":
		return p.recall(f[1], f[2], atoi(f[3]), rest(f, 4))
	case "state":
		return p.state(f[1])
	case "orphans":
		return p.orphans()
	case "run":
		return p.run(f[1], f[2])
	case "credential":
		return p.credential(f[1], arg(f, 2))
	case "runshared":
		return p.runShared(f[1], f[2])
	case "refuse":
		p.refused[f[1]] = rest(f, 2)
		return "ok"
	case "allow":
		delete(p.refused, f[1])
		return "ok"
	case "stage":
		return p.stage(f[1], atoi(f[2]), arg(f, 3))
	case "plant":
		return p.plant(f[1], f[2], f[3])
	case "watch":
		return p.watch(f[1], atoi(arg(f, 2)))
	case "next":
		return p.next(f[1])
	case "close":
		return p.closeWatch(f[1])
	case "sleep":
		time.Sleep(time.Duration(atoi(f[1])) * time.Millisecond)
		return "ok"
	case "drop":
		return p.drop(f[1], args(f[2:]))
	case "sweep":
		return p.sweep()
	}
	return "unknown-op"
}

// drop puts one request in the store's drop folder, spelled the way a person
// would spell it: the locator, then a digest and a destination if the scenario
// gave one. `text` replaces the whole line, for a request that is not one.
func (p *replay) drop(name string, a map[string]string) string {
	size := atoi(a["size"])
	body := artifact(size)
	line := a["text"]
	if line == "" {
		switch kind := a["src"]; {
		case kind == "file":
			line = "file:///" + filepath.ToSlash(filepath.Join(p.work, "artifact.bin"))
		case strings.HasPrefix(kind, "http:"):
			line = fmt.Sprintf("%s/%s/%d", fixture(), strings.TrimPrefix(kind, "http:"), size)
		default:
			line = kind
		}
		switch a["digest"] {
		case "good":
			sum := sha256.Sum256(body)
			line += " sha256:" + hex.EncodeToString(sum[:])
		case "bad":
			line += " sha256:" + strings.Repeat("0", 64)
		}
		if a["dest"] != "" {
			line += " " + a["dest"]
		}
	}
	dir := filepath.Join(p.work, "store", "wanted")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return outcome(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(line+"\n"), 0o644); err != nil {
		return outcome(err)
	}
	return "ok"
}

// sweep is one pass of the drop folder, then what a person sees in it: each
// request and the answer its name carries. An accepted request's first job
// becomes the alias, so the scenario can run and inspect it.
func (p *replay) sweep() string {
	w := dl.Wanted{Store: p.store, Submit: func(s dl.Spec) (string, error) { return dl.Submit(p.store, s) }}
	if err := w.Answer(); err != nil {
		return outcome(err)
	}
	if _, err := w.TakeIn(); err != nil {
		return outcome(err)
	}
	dir, _ := w.Dir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return outcome(err)
	}
	var seen []string
	for _, e := range entries {
		name, state, _ := strings.Cut(e.Name(), ".")
		seen = append(seen, name+"="+state)
		if state != "accepted" {
			continue
		}
		text, _ := os.ReadFile(filepath.Join(dir, e.Name()))
		for _, line := range strings.Split(string(text), "\n") {
			if id, _, ok := strings.Cut(strings.TrimPrefix(line, "# job "), " -> "); ok && strings.HasPrefix(line, "# job ") {
				p.ids[name] = id
				break
			}
		}
	}
	sort.Strings(seen)
	return "ok " + strings.Join(seen, " ")
}

func arg(f []string, i int) string {
	if i < len(f) {
		return f[i]
	}
	return ""
}

// rest is the remainder of the line, rejoined. A checkpoint is JSON and JSON
// carries spaces — an HTTP date is nothing but spaces — so the last argument of
// an operation cannot be one whitespace-separated word.
func rest(f []string, i int) string {
	if i >= len(f) {
		return ""
	}
	return strings.Join(f[i:], " ")
}

func args(f []string) map[string]string {
	m := map[string]string{}
	for _, a := range f {
		if k, v, ok := strings.Cut(a, "="); ok {
			m[k] = v
		}
	}
	return m
}

func atoi(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// outcome names a refusal in a vocabulary all three implementations share. The
// wording of an error is not a contract and must never become one; which class
// of refusal happened is exactly what a caller branches on.
func outcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, job.ErrNotFound):
		return "not-found"
	case errors.Is(err, job.ErrLeaseHeld):
		return "lease-held"
	case errors.Is(err, job.ErrStaleEpoch):
		return "stale-epoch"
	case errors.Is(err, job.ErrLeaseExpiry):
		return "lease-expired"
	case errors.Is(err, job.ErrTerminal):
		return "terminal"
	case errors.Is(err, job.ErrUnknownSchema):
		return "unknown-model"
	case errors.Is(err, job.ErrInvalid):
		return "invalid"
	}
	return "refused"
}

func compact(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "none"
	}
	var b bytes.Buffer
	if json.Compact(&b, raw) != nil {
		return "unreadable"
	}
	return b.String()
}

func (p *replay) fields(rec *job.Record) string {
	held := "no"
	if rec.Lease.Held(time.Now()) {
		held = "yes"
	}
	recorded := "none"
	if rec.Error != "" {
		recorded = "set"
	}
	recalled := "none"
	if rec.Lease.Recalled() {
		recalled = rec.Lease.Recall.Reason
	}
	awake := "no"
	if h := p.holds[rec.ID]; h != nil && h.Held() {
		awake = "yes"
	}
	return fmt.Sprintf("state=%s epoch=%d held=%s recall=%s want=%s done=%d err=%s cp=%s content=%s crit=%s awake=%s",
		rec.State, rec.Lease.Epoch, held, recalled, rec.Wants(), rec.Progress.Done, recorded, compact(rec.Checkpoint),
		strings.Join(rec.Content, ","), strings.Join(rec.Critical, ","), awake)
}

// hold keeps the machine awake for the lease the record carries right now,
// the way a runner does for the lease it just claimed.
func (p *replay) hold(alias string) string {
	rec, err := p.store.Load(p.ids[alias])
	if err != nil {
		return outcome(err)
	}
	p.holds[rec.ID] = job.KeepAwake(p.store, rec)
	return p.show(alias, nil)
}

// show reports the verdict and then what the record looks like from outside.
// The record is printed even when the operation was refused, because "what did
// a refusal leave behind" is the half of a refusal a caller has to live with.
func (p *replay) show(alias string, err error) string {
	rec, loadErr := p.store.Load(p.ids[alias])
	if loadErr != nil {
		return outcome(loadErr)
	}
	return outcome(err) + " " + p.fields(rec)
}

// artifact is the bytes a scenario transfers. Content is a function of the
// offset alone, so every implementation makes the same file and the same digest
// without any of them being told what it is.
func artifact(size int64) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// stage puts bytes in the partial before a run, so a resume has a prefix to
// continue from and sends the Range request the wire scenarios are about.
//
// `stale` writes bytes from a DIFFERENT artifact. That is the case a bare Range
// cannot see: the server replaced the file, the range it answers is honest and
// belongs to a version the prefix never came from, and the splice has the right
// length and the wrong contents.
func (p *replay) stage(alias string, n int64, kind string) string {
	body := artifact(n)
	if kind == "stale" {
		for i := range body {
			body[i] = byte((i + 7) % 251)
		}
	}
	path := filepath.Join(p.work, "store", dl.PartialFor("models/"+alias+".bin", p.ids[alias]))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return outcome(err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return outcome(err)
	}
	return p.show(alias, nil)
}

// plant forges what a newer writer would have written: a content-set name this
// implementation has never heard of, in `content` alone or in `critical` too.
//
// It edits the file rather than going through the store because no conforming
// writer can produce this record — Encode validates its own declaration and
// refuses a name it could not read back — so the refusal path exists and
// nothing in the language could reach it. The edit is textual on purpose: the
// encoding is fixed at two-space indent by [JOB-E1], so inserting one element
// at the head of an array is the same three lines in three languages.
func (p *replay) plant(alias, where, name string) string {
	path := filepath.Join(p.work, "store", "jobs", p.ids[alias]+".json")
	b, err := os.ReadFile(path)
	if err != nil {
		return outcome(err)
	}
	keys := []string{"content"}
	if where == "critical" {
		keys = append(keys, "critical")
	}
	lines := strings.Split(string(b), "\n")
	for _, key := range keys {
		var out []string
		for _, line := range lines {
			out = append(out, line)
			if line == `  "`+key+`": [` {
				out = append(out, `    "`+name+`",`)
			}
		}
		lines = out
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		return outcome(err)
	}
	return p.show(alias, nil)
}

// credential makes this process hold the canary token under name, bound to
// hosts -- what a machine that holds a secret looks like to the runner. The
// token is the fixture's, so what arrives on its wire is what the runner chose
// to send.
func (p *replay) credential(name, hosts string) string {
	key := "ABSTRACTION_CRED_" + strings.ToUpper(name)
	os.Setenv(key, "hf_thisMustNeverAppearOnDisk_EXAMPLE")
	if hosts == "-" {
		hosts = ""
	}
	os.Setenv(key+"_HOSTS", hosts)
	return "ok"
}

// finalFor names the sink. `foreign` is an absolute path in the OTHER
// platform's convention, and the driver spells it rather than the scenario
// because which spelling is foreign depends on the host: a scenario that named
// `/mnt/...` would assert one thing on Windows and its opposite on Linux, and
// the behaviour under test is the same on both.
func finalFor(alias, kind string) string {
	switch kind {
	case "foreign":
		if runtime.GOOS == "windows" {
			return "/mnt/store/models/" + alias + ".bin"
		}
		return `C:\store\models\` + alias + ".bin"
	case "abs":
		// THIS machine's own absolute convention, so foreign_path does not catch
		// it: the exact hole a shared-store runner must close. A record naming
		// this was written by another machine and points at a filesystem the
		// submitter does not own.
		if runtime.GOOS == "windows" {
			return `C:\abstraction-deputy\` + alias + ".bin"
		}
		return "/etc/cron.d/" + alias
	}
	return "models/" + alias + ".bin"
}

func (p *replay) submit(alias string, a map[string]string) string {
	size := atoi(a["size"])
	body := artifact(size)
	src := filepath.Join(p.work, "artifact.bin")
	if err := os.WriteFile(src, body, 0o644); err != nil {
		return outcome(err)
	}

	spec := dl.Spec{Sink: dl.Sink{Final: finalFor(alias, a["sink"])}}
	spec.Artifact.Size = size
	switch a["digest"] {
	case "good":
		sum := sha256.Sum256(body)
		spec.Artifact.Digest = "sha256:" + hex.EncodeToString(sum[:])
	case "bad":
		spec.Artifact.Digest = "sha256:" + strings.Repeat("0", 64)
	}
	switch kind := a["src"]; {
	case kind == "file":
		spec.Sources = []dl.Source{{Scheme: "file", Locator: src}}
	case kind == "missing":
		spec.Sources = []dl.Source{{Scheme: "file", Locator: filepath.Join(p.work, "absent.bin")}}
	case kind == "nofetcher":
		spec.Sources = []dl.Source{{Scheme: "gopher", Locator: "gopher://example.invalid/x"}}
	case strings.HasPrefix(kind, "http:"):
		// The behaviour is a path segment, so a new wire case needs a fixture
		// answer and a scenario and no driver in any language changes.
		spec.Sources = []dl.Source{{Scheme: "http",
			Locator: fmt.Sprintf("%s/%s/%d", fixture(), strings.TrimPrefix(kind, "http:"), size)}}
	}

	if c := a["cred"]; c != "" {
		for i := range spec.Sources {
			spec.Sources[i].Attrs = map[string]string{dl.CredentialAttr: c}
		}
	}
	id, err := dl.Submit(p.store, spec)
	if err != nil {
		return outcome(err)
	}
	p.ids[alias] = id
	return p.show(alias, nil)
}

func (p *replay) claim(alias, owner string, ttlMS int64) string {
	rec, err := p.store.Claim(p.ids[alias], owner, time.Duration(ttlMS)*time.Millisecond)
	if err == nil {
		p.epochs[owner] = rec.Lease.Epoch
	}
	return p.show(alias, err)
}

// renew is here because a verb no driver has is a verb no scenario can reach,
// and a divergence a scenario cannot reach is one no harness will ever report.
// It was the only store operation missing from the language, and the only reason
// anybody knew what the five implementations did with it is that a person read
// them side by side.
func (p *replay) renew(alias, owner string, ttlMS int64) string {
	_, err := p.store.Renew(p.ids[alias], p.epochs[owner], time.Duration(ttlMS)*time.Millisecond)
	return p.show(alias, err)
}

func (p *replay) progress(alias, owner string, done int64, checkpoint string) string {
	_, err := p.store.Update(p.ids[alias], p.epochs[owner], func(r *job.Record) error {
		r.Progress.Done = done
		r.Progress.UpdatedAt = job.At(time.Now())
		if checkpoint != "" {
			r.Checkpoint = json.RawMessage(checkpoint)
		}
		return nil
	})
	return p.show(alias, err)
}

func (p *replay) release(alias, owner string) string {
	return p.show(alias, p.store.Release(p.ids[alias], p.epochs[owner]))
}

func (p *replay) finish(alias, owner, state string) string {
	_, err := p.store.Update(p.ids[alias], p.epochs[owner], func(r *job.Record) error {
		st := job.State(state)
		if !st.Valid() {
			return fmt.Errorf("%w: state %q", job.ErrInvalid, state)
		}
		r.State = st
		return nil
	})
	return p.show(alias, err)
}

func (p *replay) intent(alias, want string) string {
	_, err := p.store.SetIntent(p.ids[alias], job.Want(want), "replay")
	return p.show(alias, err)
}

// recall is issued against the epoch the named owner holds, which is the epoch
// an issuer would have read off the record: a scenario naming an owner with no
// epoch is a recall decided against a holding that never existed.
func (p *replay) recall(alias, owner string, graceMS int64, reason string) string {
	_, err := p.store.Recall(p.ids[alias], p.epochs[owner], reason, "replay", time.Duration(graceMS)*time.Millisecond)
	return p.show(alias, err)
}

func (p *replay) state(alias string) string { return p.show(alias, nil) }

func (p *replay) watch(name string, budgetMillis int64) string {
	p.subs[name] = job.WatchQuiet(p.store, dl.Kind, time.Duration(budgetMillis)*time.Millisecond)
	return "ok"
}

// next prints what a listener was handed: the kind of notice, then every job
// the scenario named as state/done. Never the silence — clocks are not compared.
func (p *replay) next(name string) string {
	sub, ok := p.subs[name]
	if !ok {
		return "not-found"
	}
	n, err := sub.Next(context.Background())
	if errors.Is(err, watch.ErrClosed) {
		return "closed"
	}
	if err != nil {
		return "refused"
	}
	kind := "changed"
	if n.Quiet {
		kind = "quiet"
	}
	return kind + " " + p.present(n.Records)
}

func (p *replay) present(rs []*job.Record) string {
	byID := map[string]string{}
	for alias, id := range p.ids {
		byID[id] = alias
	}
	var parts []string
	for _, r := range rs {
		if alias, ok := byID[r.ID]; ok {
			parts = append(parts, fmt.Sprintf("%s=%s/%d", alias, r.State, r.Progress.Done))
		}
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

func (p *replay) closeWatch(name string) string {
	sub, ok := p.subs[name]
	if !ok {
		return "not-found"
	}
	sub.Close()
	return "ok"
}

func (p *replay) orphans() string {
	rs, err := p.store.Orphans()
	if err != nil {
		return outcome(err)
	}
	byID := map[string]string{}
	for alias, id := range p.ids {
		byID[id] = alias
	}
	var names []string
	for _, r := range rs {
		names = append(names, byID[r.ID])
	}
	sort.Strings(names)
	if len(names) == 0 {
		return "ok -"
	}
	return "ok " + strings.Join(names, " ")
}

func (p *replay) run(alias, owner string) string { return p.runWith(alias, owner, false) }

// runShared is a supervisor over a store several machines can write, so it
// refuses an absolute sink a foreign record chose — the confused-deputy fix at
// the sink field, one spelling past the containment that closed the relative case.
func (p *replay) runShared(alias, owner string) string { return p.runWith(alias, owner, true) }

func (p *replay) runWith(alias, owner string, shared bool) string {
	r := dl.NewRunner(p.store, owner)
	r.LeaseTTL = 5 * time.Second
	r.SharedStore = shared
	r.Reach = func(host string) error {
		if why, ok := p.refused[host]; ok {
			return errors.New(why)
		}
		return nil
	}
	if err := r.Run(context.Background(), p.ids[alias]); err != nil {
		return "transfer-failed " + p.fieldsOf(alias)
	}
	return p.show(alias, nil)
}

func (p *replay) fieldsOf(alias string) string {
	rec, err := p.store.Load(p.ids[alias])
	if err != nil {
		return outcome(err)
	}
	return p.fields(rec)
}
