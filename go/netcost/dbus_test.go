package netcost

import (
	"bufio"
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDBusMessageRoundTrip(t *testing.T) {
	in := &dbusMessage{Type: dbusSignal, Serial: 7, Path: nmPath, Interface: dbusProps, Member: "PropertiesChanged", Sender: ":1.4",
		Body: []any{nmName, map[string]dbusVariant{"Metered": {Sig: "u", Value: uint32(nmMeteredGuessYes)}}, []string{"Other"}}}
	raw, err := encodeMessage(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw)%8 != 0 && raw[4] == 0 {
		t.Fatalf("unaligned: %d", len(raw))
	}
	out, err := readMessage(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if out.Signature != "sa{sv}as" || out.Path != in.Path || out.Member != in.Member || out.Sender != ":1.4" || out.Serial != 7 {
		t.Fatalf("header: %+v", out)
	}
	want := []any{nmName, map[any]any{"Metered": dbusVariant{Sig: "u", Value: uint32(3)}}, []any{"Other"}}
	if !reflect.DeepEqual(out.Body, want) {
		t.Fatalf("body: %#v", out.Body)
	}
	if v, ok := meteredChange(out); !ok || classifyNM(v) != Metered {
		t.Fatalf("metered change %v %v", v, ok)
	}
	for size := 0; size < len(raw); size++ {
		if _, err := readMessage(strings.NewReader(string(raw[:size]))); err == nil {
			t.Fatalf("truncated message of %d bytes accepted", size)
		}
	}
}

// fakeBus answers as dbus-daemon and NetworkManager do, over one stream.
type fakeBus struct {
	t       *testing.T
	conn    net.Conn
	metered uint32
	noNM    bool
	serial  uint32
	mu      sync.Mutex
	owner   string
	matches []string
}

func (b *fakeBus) send(m *dbusMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.serial++
	m.Serial = b.serial
	raw, err := encodeMessage(m)
	if err != nil {
		b.t.Error(err)
		return
	}
	b.conn.Write(raw)
}

func (b *fakeBus) currentOwner() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.owner
}

func (b *fakeBus) ownerChange(old, next string, metered uint32) {
	b.mu.Lock()
	b.owner, b.metered = next, metered
	b.mu.Unlock()
	b.send(&dbusMessage{Type: dbusSignal, Path: dbusPath, Interface: dbusName,
		Member: "NameOwnerChanged", Sender: dbusName, Body: []any{nmName, old, next}})
}

func (b *fakeBus) changeFrom(sender string, metered uint32) {
	b.send(&dbusMessage{Type: dbusSignal, Path: nmPath, Interface: dbusProps, Member: "PropertiesChanged", Sender: sender,
		Body: []any{nmName, map[string]dbusVariant{"Metered": {Sig: "u", Value: metered}}, []string{}}})
}

func (b *fakeBus) serve(ready chan<- struct{}) {
	b.mu.Lock()
	if !b.noNM && b.owner == "" {
		b.owner = ":1.2"
	}
	b.mu.Unlock()
	r := bufio.NewReader(b.conn)
	line, _ := r.ReadString('\n')
	if !strings.HasPrefix(line, "\x00AUTH EXTERNAL ") {
		b.t.Errorf("auth line %q", line)
		return
	}
	b.conn.Write([]byte("OK 0123456789abcdef\r\n"))
	if line, _ = r.ReadString('\n'); line != "BEGIN\r\n" {
		b.t.Errorf("begin line %q", line)
		return
	}
	for {
		m, err := readMessage(r)
		if err != nil {
			return
		}
		reply := &dbusMessage{Type: dbusMethodReturn, ReplySerial: m.Serial, Destination: ":1.9", Sender: dbusName}
		switch m.Member {
		case "Hello":
			reply.Body = []any{":1.9"}
		case "AddMatch":
			b.mu.Lock()
			if len(m.Body) == 1 {
				if rule, ok := m.Body[0].(string); ok {
					b.matches = append(b.matches, rule)
				}
			}
			b.mu.Unlock()
		case "GetNameOwner":
			if b.noNM {
				reply = &dbusMessage{Type: dbusError, ReplySerial: m.Serial, ErrorName: "org.freedesktop.DBus.Error.ServiceUnknown", Body: []any{"The name is not activatable"}}
				break
			}
			reply.Body = []any{b.currentOwner()}
		case "Get":
			if b.noNM {
				reply = &dbusMessage{Type: dbusError, ReplySerial: m.Serial, ErrorName: "org.freedesktop.DBus.Error.ServiceUnknown", Body: []any{"The name is not activatable"}}
				break
			}
			if !reflect.DeepEqual(m.Body, []any{nmName, "Metered"}) || m.Destination != nmName {
				b.t.Errorf("Get %+v", m)
			}
			b.mu.Lock()
			reply.Body = []any{dbusVariant{Sig: "u", Value: b.metered}}
			b.mu.Unlock()
			reply.Sender = b.currentOwner()
		}
		b.send(reply)
		if m.Member == "Get" && ready != nil {
			close(ready)
			ready = nil
		}
	}
}

func (b *fakeBus) change(metered uint32) {
	b.changeFrom(b.currentOwner(), metered)
}

func busBarrier(t *testing.T, source Source) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := source.(*networkManager).conn.call(ctx, dbusName, dbusPath, dbusName, "Hello"); err != nil {
		t.Fatal(err)
	}
}

func TestNetworkManagerOwnerRestartInvalidatesCost(t *testing.T) {
	client, server := net.Pipe()
	bus := &fakeBus{t: t, conn: server, metered: nmMeteredNo}
	go bus.serve(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := openNetworkManager(ctx, client, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := s.Watch()
	defer w.Close()
	if got := next(t, w); got != Unmetered {
		t.Fatalf("initial %q", got)
	}
	bus.mu.Lock()
	matchedOwner := false
	for _, rule := range bus.matches {
		if strings.Contains(rule, "member='NameOwnerChanged'") && strings.Contains(rule, "arg0='"+nmName+"'") {
			matchedOwner = true
		}
	}
	bus.mu.Unlock()
	if !matchedOwner {
		t.Fatal("source did not subscribe to NetworkManager owner changes")
	}
	bus.send(&dbusMessage{Type: dbusSignal, Path: dbusPath, Interface: dbusName,
		Member: "NameOwnerChanged", Sender: ":1.2", Body: []any{nmName, ":1.2", ""}})
	busBarrier(t, s)
	if got := s.Cost(); got != Unmetered {
		t.Fatalf("untrusted owner notice changed cost to %q", got)
	}
	bus.ownerChange(":1.2", "", nmMeteredNo)
	if got := next(t, w); got != CostUnavailable {
		t.Fatalf("after owner loss %q", got)
	}
	bus.changeFrom(":1.2", nmMeteredNo)
	busBarrier(t, s)
	if got := s.Cost(); got != CostUnavailable {
		t.Fatalf("stale old owner changed cost to %q", got)
	}
	bus.ownerChange("", ":1.3", nmMeteredYes)
	if got := next(t, w); got != Metered {
		t.Fatalf("replacement owner's queried cost %q", got)
	}
	bus.changeFrom(":1.2", nmMeteredNo)
	busBarrier(t, s)
	if got := s.Cost(); got != Metered {
		t.Fatalf("old owner changed replacement cost to %q", got)
	}
	bus.changeFrom(":1.3", nmMeteredNo)
	if got := next(t, w); got != Unmetered {
		t.Fatalf("replacement owner signal %q", got)
	}
}

func TestNetworkManagerBusDisconnectInvalidatesCost(t *testing.T) {
	client, server := net.Pipe()
	bus := &fakeBus{t: t, conn: server, metered: nmMeteredNo}
	go bus.serve(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := openNetworkManager(ctx, client, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := s.Watch()
	defer w.Close()
	if got := next(t, w); got != Unmetered {
		t.Fatalf("initial %q", got)
	}
	server.Close()
	if got := next(t, w); got != CostUnavailable || s.Cost() != CostUnavailable {
		t.Fatalf("after bus disconnect %q, cached %q", got, s.Cost())
	}
}

func TestNetworkManagerSourceFollowsSignals(t *testing.T) {
	client, server := net.Pipe()
	bus := &fakeBus{t: t, conn: server, metered: nmMeteredNo}
	ready := make(chan struct{})
	go bus.serve(ready)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := openNetworkManager(ctx, client, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	<-ready
	w := s.Watch()
	defer w.Close()
	if c := next(t, w); c != Unmetered {
		t.Fatalf("initial %q", c)
	}
	bus.change(nmMeteredYes)
	if c := next(t, w); c != Metered {
		t.Fatalf("after metered signal %q", c)
	}
	bus.change(nmMeteredGuessNo)
	if c := next(t, w); c != Unmetered {
		t.Fatalf("after unmetered signal %q", c)
	}
}

func TestNetworkManagerAbsentIsUnavailable(t *testing.T) {
	client, server := net.Pipe()
	bus := &fakeBus{t: t, conn: server, noNM: true}
	go bus.serve(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := openNetworkManager(ctx, client, 1000)
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "ServiceUnknown") {
		t.Fatalf("no NetworkManager: %v", err)
	}
}
