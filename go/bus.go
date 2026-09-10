package download

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	job "github.com/openabstractions/abstraction-job/go"
)

// The bus is the one connection a process has to a supervisor, on the transport
// the identity layer binds, so every request arrives with the program that made
// it and a caller the kernel cannot name is refused. The store stays the truth;
// the bus carries "look" and "who" and nothing that would be a second copy of it.

const (
	busLine = 64 << 10
	// A supervisor that has not answered a one-line request in this long is not
	// one a submit waits for: the sweep is still coming.
	busWait = 2 * time.Second
)

var (
	ErrNoSupervisor  = errors.New("download: no supervisor answers for this store")
	ErrNoBus         = errors.New("download: the supervisor is reachable through the store only")
	ErrCallerRefused = errors.New("download: the supervisor refused this caller")
	ErrNoName        = errors.New("download: this bus has no name to listen at")
)

// Scope is the population one supervisor serves, and the endpoint is named
// after it.
//
// The name used to be invented per run and published in the heartbeat, which
// made the heartbeat the only way to reach a supervisor. Nothing else can
// address a name that changes: not a Windows service trigger, not a launchd
// MachServices key, not a systemd socket unit, not a security descriptor
// written by an installer. Fixed names, one per scope, are what let a
// supervisor be started by the platform rather than found by a file
// (research/guar198/SPEC.md § 3.1).
//
// One holder per name is the platform's answer, not ours: a second listener at
// a held name is refused with listen.ErrTaken.
type Scope string

const (
	// UserScope is one supervisor for one account. It is what this project
	// installs, and what a sink inside a person's own tree needs: a supervisor
	// running as somebody else shares the filesystem and not the rights.
	UserScope Scope = "user"
	// MachineScope is one supervisor for every account on the machine.
	MachineScope Scope = "machine"
)

// Endpoint is where a supervisor of this scope listens, spelled the way the
// identity layer spells an endpoint on this platform.
//
// Empty means the platform would not say which account this is. That is
// unknown rather than nobody, and ListenBus refuses it: every process that
// could not name its account would otherwise agree on one name.
func (s Scope) Endpoint() string { return endpointOf(s) }

// DefaultEndpoint is where a supervisor listens unless it was told otherwise,
// the way asks, rights and router each name theirs.
func DefaultEndpoint() string { return UserScope.Endpoint() }

type busRequest struct {
	Op string `json:"op"`
}

// Answer is what a supervisor says back, including who it saw asking.
type Answer struct {
	Owner  string      `json:"owner,omitempty"`
	Tier   string      `json:"tier,omitempty"`
	Caller listen.Seen `json:"caller"`
	Error  string      `json:"error,omitempty"`
}

type Bus struct {
	Endpoint string
	owner    string
	tier     func() string
	l        listen.Listener
	looks    chan struct{}
	mu       sync.Mutex
	closed   bool
	live     map[listen.Conn]struct{}
	wg       sync.WaitGroup
}

// ListenBus opens the supervisor's end at the given endpoint, which is
// DefaultEndpoint for a supervisor nobody told otherwise. The caller supplies
// it — as asks, rights and router each do — so that a test, or a second store
// on one account, has a name of its own rather than a random one.
//
// The name is never derived from the store path: two spellings of one path
// would be two names.
func ListenBus(endpoint, owner string, tier func() string) (*Bus, error) {
	if err := identity.CanEver(listen.Program); err != nil {
		return nil, fmt.Errorf("download: a supervisor names who calls it, and this machine cannot say which program is calling: %w", err)
	}
	if endpoint == "" {
		return nil, ErrNoName
	}
	l, err := listen.Listen(endpoint)
	if err != nil {
		return nil, err
	}
	b := &Bus{Endpoint: endpoint, owner: owner, tier: tier, l: l,
		looks: make(chan struct{}, 1), live: map[listen.Conn]struct{}{}}
	go b.loop()
	return b, nil
}

// C fires when an identified caller asked for a sweep. Ten asks pending at once
// are one, so nobody can force a sweep per message.
func (b *Bus) C() <-chan struct{} { return b.looks }

func (b *Bus) Close() error {
	err := b.l.Close()
	b.mu.Lock()
	b.closed = true
	for c := range b.live {
		c.Close()
	}
	b.mu.Unlock()
	b.wg.Wait()
	return err
}

func (b *Bus) loop() {
	for {
		c, err := b.l.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			continue
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			c.Close()
			continue
		}
		b.live[c] = struct{}{}
		b.wg.Add(1)
		b.mu.Unlock()
		go func() {
			defer b.wg.Done()
			b.serve(c)
			b.mu.Lock()
			delete(b.live, c)
			b.mu.Unlock()
		}()
	}
}

func (b *Bus) serve(c listen.Conn) {
	kill := time.AfterFunc(busWait, func() { c.Close() })
	k, err := listen.Receive(c, listen.Program, busLine)
	kill.Stop()
	defer k.Close()
	if err != nil {
		answer(k, Answer{Error: "jobd: refused, " + err.Error()})
		return
	}
	var req busRequest
	if err := json.Unmarshal(k.Frame, &req); err != nil {
		answer(k, Answer{Error: "jobd: not a request: " + err.Error()})
		return
	}
	switch req.Op {
	case "look":
		select {
		case b.looks <- struct{}{}:
		default:
		}
	case "who":
	default:
		answer(k, Answer{Error: "jobd: unknown op " + req.Op})
		return
	}
	answer(k, Answer{Owner: b.owner, Tier: b.tier(), Caller: k.Caller})
}

func answer(w io.Writer, a Answer) error {
	raw, err := json.Marshal(a)
	if err != nil {
		return err
	}
	_, err = w.Write(append(raw, '\n'))
	return err
}

// Nudge asks the supervisor watching this store to sweep now. Nil means an
// identified caller was heard; ErrCallerRefused means one was not; ErrNoSupervisor
// means the announced supervisor does not answer; ErrNoBus means it can only be
// reached through the store, which the sweep already is.
func Nudge(store job.Store) error {
	_, err := ask(store, "look")
	return err
}

// Who asks the supervisor who it is and who it takes this process for.
func Who(store job.Store) (Answer, error) { return ask(store, "who") }

func ask(store job.Store, op string) (Answer, error) {
	sup, live := SupervisorOf(store)
	if !live {
		return Answer{}, fmt.Errorf("%w: nothing is announced", ErrNoSupervisor)
	}
	if sup.Endpoint == "" || !announcedHere(sup) {
		return Answer{}, ErrNoBus
	}
	return exchange(sup.Endpoint, op)
}

func announcedHere(sup Supervisor) bool {
	host, err := os.Hostname()
	return err == nil && sup.Host != "" && strings.EqualFold(sup.Host, host)
}

func exchange(endpoint, op string) (Answer, error) {
	nc, err := listen.Dial(endpoint)
	if err != nil {
		return Answer{}, fmt.Errorf("%w: nobody at %s (%v)", ErrNoSupervisor, endpoint, err)
	}
	defer nc.Close()
	// The dialled end of a pipe takes no read deadline on Windows; closing it is
	// what bounds the wait, and a Close does release a blocked reader
	// (research/bus145/RESULTS.md).
	hangup := time.AfterFunc(busWait, func() { nc.Close() })
	defer hangup.Stop()
	raw, err := json.Marshal(busRequest{Op: op})
	if err != nil {
		return Answer{}, err
	}
	if _, err := nc.Write(append(raw, '\n')); err != nil {
		return Answer{}, fmt.Errorf("%w: %s took no request (%v)", ErrNoSupervisor, endpoint, err)
	}
	sc := bufio.NewScanner(nc)
	sc.Buffer(make([]byte, busLine), busLine)
	if !sc.Scan() {
		return Answer{}, fmt.Errorf("%w: %s hung up without answering", ErrNoSupervisor, endpoint)
	}
	var a Answer
	if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
		return Answer{}, fmt.Errorf("%w: %s answered with something that is not an answer (%v)", ErrNoSupervisor, endpoint, err)
	}
	if a.Error != "" {
		return a, fmt.Errorf("%w: %s", ErrCallerRefused, a.Error)
	}
	return a, nil
}
