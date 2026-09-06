package download

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	job "github.com/openabstractions/abstraction-job/go"
)

// BoundariesAttr is where a source publishes the ranges it is actually stored
// in: the end offset of each range, ascending, comma separated.
//
// HuggingFace's Xet reconstruction endpoint answers, before a payload byte
// moves, with a byte-exact partition of a 2.2 GB artifact into 310 ranges from
// 1.3 MB to 67 MB. Cutting an artifact like that on a grid of our own would put
// every single request across a storage boundary. Where a source says how it is
// stored, that is better than anything we can guess — and because boundaries
// are a property of the source, they belong on the source entry and no contract
// had to change to carry them.
//
// Nothing in this repository writes it yet — a resolver that reads a
// reconstruction manifest would. Absent it, gridPlan chooses.
const BoundariesAttr = "boundaries"

const (
	// minRangeSize is the floor on a range this layer chooses for itself, and the
	// grid steps by doubling from it so a larger artifact's ranges nest inside a
	// smaller one's rather than crossing them. Two owners that picked different
	// sizes then still agree about where a boundary is.
	minRangeSize = 16 << 20
	// maxRanges bounds the request count on a very large artifact.
	maxRanges = 512
	// minParallel is where several connections start being worth more than the
	// second pass over the file they cost. Below two ranges there is nothing to
	// run in parallel anyway.
	minParallel = 2 * minRangeSize
)

// DefaultConnections is how many ranges of one artifact an owner fetches at once.
//
// Eight, and the number is a guess: Ollama uses sixteen, aria2 defaults to five,
// and the one thing measured here is eight against one
// (research/transfer/2026-09-05-parallel-fetcher.txt). Nobody has measured where
// it stops helping, or where a host starts refusing, so it is a field on Runner
// rather than a constant a caller cannot argue with.
const DefaultConnections = 8

// plan is where an artifact may be cut. Cuts are interior offsets, ascending.
type plan struct {
	size int64
	cuts []int64
}

// rangeFloor is the smallest range this layer will choose for an artifact of this
// size: minRangeSize, doubled until the whole artifact fits in maxRanges of them.
func rangeFloor(size int64) int64 {
	floor := int64(minRangeSize)
	for size/floor > maxRanges {
		floor <<= 1
	}
	return floor
}

func gridPlan(size int64) plan {
	step := rangeFloor(size)
	p := plan{size: size}
	for at := step; at < size; at += step {
		p.cuts = append(p.cuts, at)
	}
	return p
}

// publishedPlan reads the boundaries a source declared, refusing the whole
// attribute if any part of it is not ascending and inside the artifact, or if
// there are more of them than this layer would ever cut for itself. A
// half-understood partition is worse than none: it would put requests across
// boundaries while claiming to respect them.
//
// The count is a bound on a foreign plan, and it is checked while parsing so a
// hostile attribute is refused before it is allocated. Every cut is a request,
// an fsync and a record rewrite, so a source declaring a million of them for a
// 64 MiB artifact is a denial of service with our own scheduler as the weapon —
// and a resolver that copies Xet reconstruction boundaries into Attrs is
// copying network data into this field. A plan from a source is input, and gets
// the same suspicion as a digest from one.
func publishedPlan(src Source, size int64) (plan, bool) {
	raw := strings.TrimSpace(src.Attrs[BoundariesAttr])
	if raw == "" {
		return plan{}, false
	}
	p := plan{size: size}
	prev := int64(0)
	for raw != "" {
		field, rest, _ := strings.Cut(raw, ",")
		raw = rest
		n, err := strconv.ParseInt(strings.TrimSpace(field), 10, 64)
		if err != nil || n <= prev || n >= size || len(p.cuts) >= maxRanges-1 {
			return plan{}, false
		}
		p.cuts = append(p.cuts, n)
		prev = n
	}
	return p, len(p.cuts) > 0
}

// coarsened is the plan with every range shorter than the floor merged into its
// neighbour, on boundaries the plan already declared.
//
// A published partition can be finer than anything worth fetching separately —
// 512 ranges of 128 KiB is 512 requests, 512 fsyncs and 512 record rewrites for
// an artifact one request would carry. Merging keeps every cut it keeps, so no
// request crosses a boundary the source named; it only declines to stop at all
// of them.
func (p plan) coarsened() plan {
	floor := rangeFloor(p.size)
	out := plan{size: p.size}
	prev := int64(0)
	for _, c := range p.cuts {
		if c-prev < floor || p.size-c < floor {
			continue
		}
		out.cuts = append(out.cuts, c)
		prev = c
	}
	return out
}

// rangesOf cuts one gap at the plan's boundaries, so no request straddles one.
func (p plan) rangesOf(g job.Range) job.Ranges {
	var out job.Ranges
	at := g.Start
	for _, c := range p.cuts {
		if c <= at {
			continue
		}
		if c >= g.End {
			break
		}
		out = append(out, job.Range{Start: at, End: c})
		at = c
	}
	if at < g.End {
		out = append(out, job.Range{Start: at, End: g.End})
	}
	return out
}

// gaps is what is left to fetch: the artifact minus what is already proven.
//
// This is the whole of resuming from a range set rather than a prefix. A
// predecessor that proved [0,16M) and [32M,48M) leaves two gaps, and a
// successor fetches both — where a prefix-only reader would re-fetch the
// second range because it has no way to say it already has it.
func gaps(size int64, have job.Ranges) job.Ranges {
	// A set this layer cannot make sense of is read as nothing proven, which
	// re-fetches rather than leaving a hole in a file somebody will call
	// delivered.
	proven, err := job.CanonicalRanges(have)
	if err != nil {
		proven = nil
	}
	return proven.Missing(0, size)
}

func covered(rs job.Ranges) int64 {
	var n int64
	for _, r := range rs {
		n += r.End - r.Start
	}
	return n
}

func (r *Runner) connections() int {
	if r.Connections > 0 {
		return r.Connections
	}
	return 1
}

// rangeSource is a source that has been asked, and answered, that it serves
// bounded ranges of this artifact.
type rangeSource struct {
	src Source
	rf  RangeFetcher
}

// rangePlan decides whether this job is fetched as several ranges at once, and
// answers no in every case where one stream is the better answer: no artifact
// length to partition, an artifact small enough that the second hashing pass
// costs more than the concurrency saves, a tier that owns its own scheduling,
// or a source that will not serve a bounded request.
//
// Every source that will serve ranges is returned, in priority order, not only
// the first: one range refused by one source used to end the whole run, and the
// next sweep re-planned onto the same source, forever. The probe that qualifies
// each one is a single byte, and it is what makes the fallback safe — a source
// answering ranges of a DIFFERENT artifact would answer each request with a
// Content-Range naming exactly what was asked for.
//
// The plan comes from the first source that qualifies, because boundaries are a
// property of a source and mixing two sources' partitions would put requests
// across both.
func (r *Runner) rangePlan(ctx context.Context, rec *job.Record, spec Spec) ([]rangeSource, plan, bool) {
	if r.connections() < 2 || spec.Artifact.Size < minParallel {
		return nil, plan{}, false
	}
	var serving []rangeSource
	var p plan
	for _, src := range byPriority(spec.Sources) {
		f, ok := r.Fetchers.For(src, rec.Requires)
		if !ok {
			continue
		}
		rf, ok := f.(RangeFetcher)
		if !ok {
			continue
		}
		if r.Reach.check(HostOf(src.Locator)) != nil {
			continue
		}
		headers, err := headersFor(src, r.Credentials)
		if err != nil {
			continue
		}
		size, ranged, err := rf.Ranged(ctx, src, headers)
		if err != nil || !ranged {
			continue
		}
		// A source whose length disagrees with the spec cannot serve what was
		// asked for. Falling back to one stream makes that surface as the
		// length error it is, rather than as ranges that never meet.
		if size != spec.Artifact.Size {
			continue
		}
		if len(serving) == 0 {
			q, ok := publishedPlan(src, size)
			if !ok {
				q = gridPlan(size)
			}
			if p = q.coarsened(); len(p.cuts) == 0 {
				continue
			}
		}
		serving = append(serving, rangeSource{src: src, rf: rf})
	}
	return serving, p, len(serving) > 0
}

func byPriority(sources []Source) []Source {
	out := append([]Source(nil), sources...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Priority < out[j].Priority })
	return out
}

// resumable is what a predecessor proved and is still on disk.
//
// A range is written at its own offset, so the file's length bounds only the
// highest proven end — the middle of it may be a hole nobody has filled. That
// is why the single-stream rule ("the smaller of the checkpoint and the file")
// cannot be reused here, and why a range past the end of the file is dropped
// rather than the whole checkpoint being distrusted.
func resumable(rec *job.Record, partial string) (job.Ranges, error) {
	rs, err := rec.CheckpointRanges()
	if err != nil {
		return nil, err
	}
	onDisk := int64(0)
	if st, err := os.Stat(partial); err == nil {
		onDisk = st.Size()
	}
	var out job.Ranges
	for _, x := range rs {
		x.End = min(x.End, onDisk)
		if x.End > x.Start {
			out = append(out, x)
		}
	}
	return job.CanonicalRanges(out)
}

// parallel fetches every gap in the artifact with several connections at once,
// under the one lease this owner already holds.
//
// A parallel fetcher is one owner with several connections, not several owners.
// Nothing here claims, renews or releases on its own account: the ranges share
// this process's claim and its keeper, and a second owner is refused by the
// store exactly as it is today.
//
// The digest is order-dependent, so a gapped plan cannot hash as it writes.
// This returns the total proven and no digest; the caller hashes the finished
// file in a second pass. There is no way round it and it is not papered over —
// on a fresh download that is one extra read of the file at disk speed, and on
// a resume it is the same read the single-stream path already does to rebuild
// its rolling hash over the prefix it is keeping.
func (r *Runner) parallel(ctx context.Context, rec *job.Record, epoch int64, partial string, serving []rangeSource, p plan, have job.Ranges, onIntent func(job.Want)) (int64, error) {
	f, err := os.OpenFile(partial, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	// A tail past the end of the artifact is not part of the artifact: a
	// predecessor's overflow, a stray copy, whatever else was at this path. The
	// file is opened without truncation because ranges land in the middle of it,
	// so nothing else would ever remove those bytes — they survive every range,
	// the second pass hashes them with the rest, and a digest mismatch discards
	// every proven range for bytes nothing asked for.
	if st, serr := f.Stat(); serr == nil && st.Size() > p.size {
		if err := f.Truncate(p.size); err != nil {
			return 0, err
		}
	}
	cp, err := CheckpointOf(rec)
	if err != nil {
		return 0, err
	}

	// The cancel is taken BEFORE the keeper starts, so the intent callback can
	// be set once, at construction, and read without a lock. It used to be
	// installed afterwards under the progress mutex, which made observing intent
	// contend with recording a range: a keeper parked behind one slow checkpoint
	// renewed nothing, the lease lapsed under an owner that was alive and about
	// to honour the pause, and the record was left running under a dead lease.
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	w := &progress{
		store: r.Store, id: rec.ID, epoch: epoch,
		file: f, size: p.size,
		have:       append(job.Ranges(nil), have...),
		validators: cp.Validators,
		onIntent:   func(want job.Want) { onIntent(want); stop() },
	}
	// The keeper holds the lease here, and reads intent too. Renewal cannot ride
	// the data callback across eight goroutines, and a range can be minutes on a
	// thin link — so an owner that only looked at the record when a range landed
	// would have a pause button that takes minutes to work. The renewals read the
	// record three times a TTL anyway; both questions are answered there.
	ctx, keep := r.keepWatching(ctx, rec.ID, epoch, func(latest *job.Record) {
		w.observe(latest.Wants())
	})
	defer keep.stop()
	w.keep = keep

	var todo job.Ranges
	for _, g := range gaps(p.size, have) {
		todo = append(todo, p.rangesOf(g)...)
	}

	work := make(chan job.Range)
	fail := make(chan error, r.connections())
	var wg sync.WaitGroup
	for i := 0; i < r.connections(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rng := range work {
				err := r.fetchRange(ctx, serving, RangeRequest{
					Range: rng, Out: f, Validators: w.validators, Beat: keep.beat,
				})
				if err == nil {
					err = w.proved(rng)
				}
				if err != nil {
					select {
					case fail <- err:
					default:
					}
					stop()
					return
				}
			}
		}()
	}
feed:
	for _, rng := range todo {
		select {
		case work <- rng:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()

	// Read the context BEFORE stopping the keeper: stopping cancels the context
	// this work ran under, and asking afterwards would report every finished
	// transfer as cancelled.
	cancelled := ctx.Err()
	fence := keep.stop()
	var failed error
	select {
	case failed = <-fail:
	default:
	}
	done := covered(w.snapshot())
	switch {
	case fence != nil:
		return done, fence
	case failed != nil:
		return done, failed
	case cancelled != nil:
		return done, cancelled
	}
	return done, f.Sync()
}

// fetchRange gets one range, from the first source that will serve it.
//
// A range refused by one source used to end the run, which left the job to be
// re-planned onto the same source by the next sweep and refused again. The
// preference between failures is the one the single-stream path makes: a source
// that says no does not speak for a mirror that merely dropped the connection.
func (r *Runner) fetchRange(ctx context.Context, serving []rangeSource, req RangeRequest) error {
	var last error
	for _, s := range serving {
		req.Source = s.src
		// Credentials are resolved per range, not once for the run. A read token
		// from a content-addressed store is good for minutes and a 40 GB
		// artifact is 132 of these over hours; headers taken once would start
		// returning 401 two thirds of the way through, at the point where the
		// cost of being wrong is highest.
		headers, err := headersFor(s.src, r.Credentials)
		if err == nil {
			req.Headers = headers
			req.Reach = r.Reach
			err = s.rf.FetchRange(ctx, req)
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if last == nil || Permanent(last) || !Permanent(err) {
			last = err
		}
	}
	return last
}

// progress is the proven set, and the only thing that writes it down.
type progress struct {
	store job.Store
	id    string
	epoch int64
	file  *os.File
	keep  *keeper
	size  int64

	// onIntent and validators are set once, before any goroutine exists, and
	// read without the lock. onIntent especially: the keeper calls it, and a
	// keeper that has to wait for a checkpoint stops renewing the lease it is
	// there to hold.
	onIntent   func(job.Want)
	validators Validators

	mu   sync.Mutex
	have job.Ranges
}

// proved records one landed range. Durability comes before the claim: the file
// is synced first, so a record saying those bytes are proven is never ahead of
// the bytes themselves.
func (p *progress) proved(rng job.Range) error {
	// Outside the lock. Sync flushes the whole file rather than this range, so
	// holding the proven set across it only stops the other connections recording
	// what they landed.
	if err := p.file.Sync(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	next, err := p.have.Add(rng.Start, rng.End)
	if err != nil {
		return err
	}
	p.have = next
	done := covered(p.have)
	updated, err := p.store.Update(p.id, p.epoch, func(rr *job.Record) error {
		rr.Progress.Done = done
		if rr.Progress.Total == 0 {
			rr.Progress.Total = p.size
		}
		rr.Progress.UpdatedAt = job.At(time.Now())
		// The validators go down with the proven bytes. A checkpoint saying how
		// far it got but not WHICH version it got that far through is what lets
		// a successor splice two artifacts into one file, and writing the ranges
		// without them erased what a stream predecessor had recorded.
		return setCheckpoint(rr, Checkpoint{Verified: p.have, Validators: p.validators})
	})
	if err != nil {
		p.keep.refused(err)
		return err
	}
	if want := updated.Wants(); want != job.WantRun {
		p.onIntent(want)
	}
	return nil
}

// observe acts on what somebody asked for, noticed anywhere.
//
// It takes no lock, and that is the whole of it: the keeper calls this on every
// renewal, and a keeper waiting on the mutex that records a range is a keeper
// that is not renewing the lease.
func (p *progress) observe(want job.Want) {
	if want != job.WantRun {
		p.onIntent(want)
	}
}

func (p *progress) snapshot() job.Ranges {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append(job.Ranges(nil), p.have...)
}
