package netcost

// A minimal D-Bus client: SASL EXTERNAL, method calls, replies and signals over
// one stream, which is what reading NetworkManager's `Metered` property and
// its change signal needs. It is not a general binding. The codec follows the
// D-Bus Specification 0.43, "Message Protocol".

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

const (
	dbusMethodCall   = 1
	dbusMethodReturn = 2
	dbusError        = 3
	dbusSignal       = 4

	dbusFieldPath        = 1
	dbusFieldInterface   = 2
	dbusFieldMember      = 3
	dbusFieldErrorName   = 4
	dbusFieldReplySerial = 5
	dbusFieldDestination = 6
	dbusFieldSender      = 7
	dbusFieldSignature   = 8

	dbusMaxMessage = 1 << 24
)

type dbusObjectPath string
type dbusSignature string

// dbusVariant is a value with its own signature.
type dbusVariant struct {
	Sig   string
	Value any
}

type dbusMessage struct {
	Type        byte
	Serial      uint32
	Path        string
	Interface   string
	Member      string
	ErrorName   string
	ReplySerial uint32
	Destination string
	Sender      string
	Signature   string
	Body        []any
}

// dbusEncoder writes values aligned relative to the start of its buffer.
type dbusEncoder struct {
	b     []byte
	order binary.AppendByteOrder
}

func (e *dbusEncoder) align(n int) {
	for len(e.b)%n != 0 {
		e.b = append(e.b, 0)
	}
}

func (e *dbusEncoder) u32(v uint32) {
	e.align(4)
	e.b = e.order.AppendUint32(e.b, v)
}

func (e *dbusEncoder) str(v string) {
	e.u32(uint32(len(v)))
	e.b = append(append(e.b, v...), 0)
}

func (e *dbusEncoder) sig(v string) {
	e.b = append(append(append(e.b, byte(len(v))), v...), 0)
}

// signatureOf spells the D-Bus type of a value this encoder can write.
func signatureOf(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return "s", nil
	case dbusObjectPath:
		return "o", nil
	case dbusSignature:
		return "g", nil
	case uint32:
		return "u", nil
	case dbusVariant:
		return "v", nil
	case []string:
		return "as", nil
	case map[string]dbusVariant:
		return "a{sv}", nil
	default:
		return "", fmt.Errorf("dbus: cannot encode %T", x)
	}
}

func (e *dbusEncoder) value(v any) error {
	switch x := v.(type) {
	case string:
		e.str(x)
	case dbusObjectPath:
		e.str(string(x))
	case dbusSignature:
		e.sig(string(x))
	case uint32:
		e.u32(x)
	case dbusVariant:
		e.sig(x.Sig)
		return e.value(x.Value)
	case []string:
		e.u32(0)
		at := len(e.b) - 4
		start := len(e.b)
		for _, s := range x {
			e.str(s)
		}
		binary.LittleEndian.PutUint32(e.b[at:], uint32(len(e.b)-start))
	case map[string]dbusVariant:
		e.u32(0)
		at := len(e.b) - 4
		e.align(8)
		start := len(e.b)
		for k, item := range x {
			e.align(8)
			e.str(k)
			if err := e.value(item); err != nil {
				return err
			}
		}
		binary.LittleEndian.PutUint32(e.b[at:], uint32(len(e.b)-start))
	default:
		_, err := signatureOf(v)
		return err
	}
	return nil
}

func encodeMessage(m *dbusMessage) ([]byte, error) {
	body := &dbusEncoder{order: binary.LittleEndian}
	signature := ""
	for _, v := range m.Body {
		s, err := signatureOf(v)
		if err != nil {
			return nil, err
		}
		signature += s
		if err := body.value(v); err != nil {
			return nil, err
		}
	}
	e := &dbusEncoder{b: []byte{'l', m.Type, 0, 1}, order: binary.LittleEndian}
	e.u32(uint32(len(body.b)))
	e.u32(m.Serial)
	e.u32(0)
	at := len(e.b) - 4
	e.align(8)
	start := len(e.b)
	field := func(code byte, v any) {
		s, _ := signatureOf(v)
		e.align(8)
		e.b = append(e.b, code)
		e.sig(s)
		e.value(v)
	}
	if m.Path != "" {
		field(dbusFieldPath, dbusObjectPath(m.Path))
	}
	if m.Interface != "" {
		field(dbusFieldInterface, m.Interface)
	}
	if m.Member != "" {
		field(dbusFieldMember, m.Member)
	}
	if m.ErrorName != "" {
		field(dbusFieldErrorName, m.ErrorName)
	}
	if m.ReplySerial != 0 {
		field(dbusFieldReplySerial, m.ReplySerial)
	}
	if m.Destination != "" {
		field(dbusFieldDestination, m.Destination)
	}
	if m.Sender != "" {
		field(dbusFieldSender, m.Sender)
	}
	if signature != "" {
		field(dbusFieldSignature, dbusSignature(signature))
	}
	binary.LittleEndian.PutUint32(e.b[at:], uint32(len(e.b)-start))
	e.align(8)
	return append(e.b, body.b...), nil
}

var errDBusShort = errors.New("dbus: message truncated")

type dbusDecoder struct {
	b     []byte
	off   int
	order binary.ByteOrder
	depth int
}

func (d *dbusDecoder) align(n int) error {
	for d.off%n != 0 {
		if d.off >= len(d.b) {
			return errDBusShort
		}
		d.off++
	}
	return nil
}

func (d *dbusDecoder) take(n int) ([]byte, error) {
	if n < 0 || d.off+n > len(d.b) {
		return nil, errDBusShort
	}
	p := d.b[d.off : d.off+n]
	d.off += n
	return p, nil
}

func (d *dbusDecoder) fixed(size int) (uint64, error) {
	if err := d.align(size); err != nil {
		return 0, err
	}
	p, err := d.take(size)
	if err != nil {
		return 0, err
	}
	switch size {
	case 1:
		return uint64(p[0]), nil
	case 2:
		return uint64(d.order.Uint16(p)), nil
	case 4:
		return uint64(d.order.Uint32(p)), nil
	}
	return d.order.Uint64(p), nil
}

// completeType returns the length of the first complete type in sig.
func completeType(sig string) (int, error) {
	if sig == "" {
		return 0, errors.New("dbus: empty signature")
	}
	switch sig[0] {
	case 'a':
		n, err := completeType(sig[1:])
		return n + 1, err
	case '(', '{':
		closer := byte(')')
		if sig[0] == '{' {
			closer = '}'
		}
		i := 1
		for i < len(sig) && sig[i] != closer {
			n, err := completeType(sig[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
		if i >= len(sig) {
			return 0, errors.New("dbus: unbalanced signature")
		}
		return i + 1, nil
	}
	return 1, nil
}

func alignment(c byte) int {
	switch c {
	case 'n', 'q':
		return 2
	case 'b', 'i', 'u', 'h', 's', 'o', 'a':
		return 4
	case 'x', 't', 'd', '(', '{':
		return 8
	}
	return 1
}

// value decodes one complete type from the front of sig.
func (d *dbusDecoder) value(sig string) (any, error) {
	if d.depth > 64 {
		return nil, errors.New("dbus: nesting too deep")
	}
	d.depth++
	defer func() { d.depth-- }()
	switch c := sig[0]; c {
	case 'y':
		v, err := d.fixed(1)
		return byte(v), err
	case 'b':
		v, err := d.fixed(4)
		return v != 0, err
	case 'n':
		v, err := d.fixed(2)
		return int16(v), err
	case 'q':
		v, err := d.fixed(2)
		return uint16(v), err
	case 'i':
		v, err := d.fixed(4)
		return int32(v), err
	case 'u', 'h':
		v, err := d.fixed(4)
		return uint32(v), err
	case 'x':
		v, err := d.fixed(8)
		return int64(v), err
	case 't', 'd':
		return d.fixed(8)
	case 's', 'o':
		n, err := d.fixed(4)
		if err != nil {
			return nil, err
		}
		p, err := d.take(int(n) + 1)
		if err != nil {
			return nil, err
		}
		return string(p[:n]), nil
	case 'g':
		n, err := d.fixed(1)
		if err != nil {
			return nil, err
		}
		p, err := d.take(int(n) + 1)
		if err != nil {
			return nil, err
		}
		return dbusSignature(p[:n]), nil
	case 'v':
		s, err := d.value("g")
		if err != nil {
			return nil, err
		}
		inner := string(s.(dbusSignature))
		if n, err := completeType(inner); err != nil || n != len(inner) {
			return nil, errors.New("dbus: bad variant signature")
		}
		v, err := d.value(inner)
		return dbusVariant{Sig: inner, Value: v}, err
	case 'a':
		n, err := d.fixed(4)
		if err != nil {
			return nil, err
		}
		size, err := completeType(sig[1:])
		if err != nil {
			return nil, err
		}
		element := sig[1 : 1+size]
		if err := d.align(alignment(element[0])); err != nil {
			return nil, err
		}
		end := d.off + int(n)
		if end > len(d.b) {
			return nil, errDBusShort
		}
		if element[0] == '{' {
			m := map[any]any{}
			for d.off < end {
				entry, err := d.value(element)
				if err != nil {
					return nil, err
				}
				pair := entry.([]any)
				m[pair[0]] = pair[1]
			}
			return m, nil
		}
		var list []any
		for d.off < end {
			v, err := d.value(element)
			if err != nil {
				return nil, err
			}
			list = append(list, v)
		}
		return list, nil
	case '(', '{':
		if err := d.align(8); err != nil {
			return nil, err
		}
		size, err := completeType(sig)
		if err != nil {
			return nil, err
		}
		inner := sig[1 : size-1]
		var fields []any
		for inner != "" {
			n, err := completeType(inner)
			if err != nil {
				return nil, err
			}
			v, err := d.value(inner[:n])
			if err != nil {
				return nil, err
			}
			fields = append(fields, v)
			inner = inner[n:]
		}
		return fields, nil
	default:
		return nil, fmt.Errorf("dbus: unsupported type %q", c)
	}
}

func readMessage(r io.Reader) (*dbusMessage, error) {
	head := make([]byte, 16)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, err
	}
	var order binary.ByteOrder
	switch head[0] {
	case 'l':
		order = binary.LittleEndian
	case 'B':
		order = binary.BigEndian
	default:
		return nil, errors.New("dbus: bad byte order")
	}
	bodyLen, fieldsLen := order.Uint32(head[4:]), order.Uint32(head[12:])
	headerLen := 16 + int(fieldsLen)
	headerLen += (8 - headerLen%8) % 8
	total := headerLen + int(bodyLen)
	if fieldsLen > dbusMaxMessage || bodyLen > dbusMaxMessage || total > dbusMaxMessage {
		return nil, errors.New("dbus: message too large")
	}
	all := make([]byte, total)
	copy(all, head)
	if _, err := io.ReadFull(r, all[16:]); err != nil {
		return nil, err
	}
	m := &dbusMessage{Type: head[1], Serial: order.Uint32(head[8:])}
	d := &dbusDecoder{b: all[:16+int(fieldsLen)], off: 12, order: order}
	fields, err := d.value("a(yv)")
	if err != nil {
		return nil, err
	}
	for _, f := range fields.([]any) {
		pair := f.([]any)
		v, _ := pair[1].(dbusVariant)
		text := fmt.Sprint(v.Value)
		switch pair[0].(byte) {
		case dbusFieldPath:
			m.Path = text
		case dbusFieldInterface:
			m.Interface = text
		case dbusFieldMember:
			m.Member = text
		case dbusFieldErrorName:
			m.ErrorName = text
		case dbusFieldReplySerial:
			serial, _ := v.Value.(uint32)
			m.ReplySerial = serial
		case dbusFieldDestination:
			m.Destination = text
		case dbusFieldSender:
			m.Sender = text
		case dbusFieldSignature:
			m.Signature = text
		}
	}
	body := &dbusDecoder{b: all[headerLen:], order: order}
	for sig := m.Signature; sig != ""; {
		n, err := completeType(sig)
		if err != nil {
			return nil, err
		}
		v, err := body.value(sig[:n])
		if err != nil {
			return nil, err
		}
		m.Body = append(m.Body, v)
		sig = sig[n:]
	}
	return m, nil
}

// dbusConn is one authenticated connection with a reader goroutine.
type dbusConn struct {
	conn    net.Conn
	writeMu sync.Mutex
	mu      sync.Mutex
	serial  uint32
	pending map[uint32]dbusPending
	err     error
	signal  func(*dbusMessage)
	done    chan struct{}
}

// dbusAuthenticate runs SASL EXTERNAL for uid and returns the reader positioned
// at the first message.
func dbusAuthenticate(conn net.Conn, uid int) (*bufio.Reader, error) {
	identity := hex.EncodeToString([]byte(strconv.Itoa(uid)))
	if _, err := io.WriteString(conn, "\x00AUTH EXTERNAL "+identity+"\r\n"); err != nil {
		return nil, err
	}
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "OK ") {
		return nil, fmt.Errorf("dbus: authentication refused: %s", strings.TrimSpace(line))
	}
	if _, err := io.WriteString(conn, "BEGIN\r\n"); err != nil {
		return nil, err
	}
	return r, nil
}

func newDBusConn(conn net.Conn, uid int, signal func(*dbusMessage)) (*dbusConn, error) {
	r, err := dbusAuthenticate(conn, uid)
	if err != nil {
		return nil, err
	}
	c := &dbusConn{conn: conn, pending: map[uint32]dbusPending{}, signal: signal, done: make(chan struct{})}
	go c.read(r)
	return c, nil
}

func (c *dbusConn) read(r io.Reader) {
	defer close(c.done)
	for {
		m, err := readMessage(r)
		if err != nil {
			c.mu.Lock()
			c.err = err
			for serial, p := range c.pending {
				close(p.reply)
				delete(c.pending, serial)
			}
			c.mu.Unlock()
			return
		}
		switch m.Type {
		case dbusMethodReturn, dbusError:
			c.mu.Lock()
			p, ok := c.pending[m.ReplySerial]
			delete(c.pending, m.ReplySerial)
			c.mu.Unlock()
			if ok {
				if p.apply != nil && m.Type == dbusMethodReturn {
					p.apply(m)
				}
				p.reply <- m
			}
		case dbusSignal:
			if c.signal != nil {
				c.signal(m)
			}
		}
	}
}

// call sends a method call and waits for its reply.
func (c *dbusConn) call(ctx context.Context, destination, path, iface, member string, body ...any) (*dbusMessage, error) {
	return c.callApplying(ctx, nil, destination, path, iface, member, body...)
}

func (c *dbusConn) callApplying(ctx context.Context, apply func(*dbusMessage), destination, path, iface, member string, body ...any) (*dbusMessage, error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return nil, c.err
	}
	c.serial++
	serial := c.serial
	reply := make(chan *dbusMessage, 1)
	c.pending[serial] = dbusPending{reply: reply, apply: apply}
	c.mu.Unlock()
	raw, err := encodeMessage(&dbusMessage{Type: dbusMethodCall, Serial: serial, Destination: destination, Path: path, Interface: iface, Member: member, Body: body})
	if err == nil {
		c.writeMu.Lock()
		_, err = c.conn.Write(raw)
		c.writeMu.Unlock()
	}
	if err != nil {
		c.mu.Lock()
		delete(c.pending, serial)
		c.mu.Unlock()
		return nil, err
	}
	select {
	case m, ok := <-reply:
		if !ok {
			return nil, fmt.Errorf("dbus: connection closed: %w", c.err)
		}
		if m.Type == dbusError {
			detail := ""
			if len(m.Body) > 0 {
				detail = fmt.Sprint(m.Body[0])
			}
			return m, &dbusCallError{Name: m.ErrorName, Detail: detail}
		}
		return m, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, serial)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (c *dbusConn) Close() error {
	err := c.conn.Close()
	<-c.done
	return err
}

// dbusPending is a call awaiting its reply. apply, when set, runs on the
// reader goroutine before the next message is read, so a reply and the signals
// after it are applied in the order the bus sent them.
type dbusPending struct {
	reply chan *dbusMessage
	apply func(*dbusMessage)
}

type dbusCallError struct{ Name, Detail string }

func (e *dbusCallError) Error() string { return "dbus: " + e.Name + ": " + e.Detail }

// NetworkManager's NMMetered values.
const (
	nmMeteredUnknown  = 0
	nmMeteredYes      = 1
	nmMeteredNo       = 2
	nmMeteredGuessYes = 3
	nmMeteredGuessNo  = 4
)

func classifyNM(v uint32) Cost {
	switch v {
	case nmMeteredYes, nmMeteredGuessYes:
		return Metered
	case nmMeteredNo, nmMeteredGuessNo:
		return Unmetered
	}
	return Unknown
}

const (
	nmName      = "org.freedesktop.NetworkManager"
	nmPath      = "/org/freedesktop/NetworkManager"
	dbusName    = "org.freedesktop.DBus"
	dbusPath    = "/org/freedesktop/DBus"
	dbusProps   = "org.freedesktop.DBus.Properties"
	nmOpenLimit = 5 // seconds a bus has to answer while opening
)

// networkManager is the Linux source: NetworkManager's global Metered
// property, updated by its PropertiesChanged signals.
type networkManager struct {
	*hub
	conn *dbusConn
}

// meteredChange returns the Metered value a PropertiesChanged signal carries.
func meteredChange(m *dbusMessage) (uint32, bool) {
	if m.Path != nmPath {
		return 0, false
	}
	var changed any
	switch {
	case m.Interface == dbusProps && m.Member == "PropertiesChanged" && len(m.Body) >= 2 && m.Body[0] == nmName:
		changed = m.Body[1]
	case m.Interface == nmName && m.Member == "PropertiesChanged" && len(m.Body) >= 1:
		changed = m.Body[0] // NetworkManager before 1.2 sent its own signal
	default:
		return 0, false
	}
	values, _ := changed.(map[any]any)
	v, ok := values["Metered"].(dbusVariant)
	if !ok {
		return 0, false
	}
	metered, ok := v.Value.(uint32)
	return metered, ok
}

// openNetworkManager authenticates on conn, subscribes to NetworkManager's
// property changes and reads Metered. A bus with no NetworkManager is
// ErrUnavailable.
func openNetworkManager(ctx context.Context, conn net.Conn, uid int) (Source, error) {
	h := newHub(Unknown)
	c, err := newDBusConn(conn, uid, func(m *dbusMessage) {
		if v, ok := meteredChange(m); ok {
			h.set(classifyNM(v))
		}
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	fail := func(err error) (Source, error) {
		c.Close()
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if _, err := c.call(ctx, dbusName, dbusPath, dbusName, "Hello"); err != nil {
		return fail(err)
	}
	for _, rule := range []string{
		"type='signal',sender='" + nmName + "',path='" + nmPath + "',interface='" + dbusProps + "',member='PropertiesChanged'",
		"type='signal',sender='" + nmName + "',path='" + nmPath + "',interface='" + nmName + "',member='PropertiesChanged'",
	} {
		if _, err := c.call(ctx, dbusName, dbusPath, dbusName, "AddMatch", rule); err != nil {
			return fail(err)
		}
	}
	read := false
	_, err = c.callApplying(ctx, func(reply *dbusMessage) {
		if len(reply.Body) == 1 {
			if v, ok := reply.Body[0].(dbusVariant); ok {
				if metered, ok := v.Value.(uint32); ok {
					h.set(classifyNM(metered))
					read = true
				}
			}
		}
	}, nmName, nmPath, dbusProps, "Get", nmName, "Metered")
	if err != nil {
		return fail(err)
	}
	if !read {
		return fail(errors.New("NetworkManager Metered is not a uint32"))
	}
	return &networkManager{hub: h, conn: c}, nil
}

func (n *networkManager) Close() error {
	n.hub.close()
	return n.conn.Close()
}
