//go:build windows

package download

import (
	"bufio"
	"os"
	"strings"
	"syscall"
	"testing"
)

const (
	securitySQOSPresent = 0x00100000
	securityAnonymous   = 0
)

// B4 on a real pipe: a client that opens it anonymously hands the kernel a
// token that names nobody. What the refusal says is the identity layer's
// answer; that it refuses, and wakes nothing, is this layer's.
func TestAnAnonymousCallerIsRefusedOnARealPipe(t *testing.T) {
	_, store, _ := newRunner(t)
	b := announce(t, store, "jobd@test:1", "here")
	name, err := syscall.UTF16PtrFromString(b.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	h, err := syscall.CreateFile(name, syscall.GENERIC_READ|syscall.GENERIC_WRITE, 0, nil,
		syscall.OPEN_EXISTING, securitySQOSPresent|securityAnonymous, 0)
	if err != nil {
		t.Fatal(err)
	}
	f := os.NewFile(uintptr(h), "pipe")
	defer f.Close()
	if _, err := f.Write([]byte(`{"op":"look"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(f).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, "jobd: refused, ") || !strings.Contains(line, `"bound":false`) {
		t.Fatalf("an anonymous caller was answered: %s", line)
	}
	select {
	case <-b.C():
		t.Fatal("an anonymous look woke the supervisor")
	default:
	}
}
