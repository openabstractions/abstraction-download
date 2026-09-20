//go:build windows

package netcost

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

const (
	windowsWorkstationProductType      = 1 // VER_NT_WORKSTATION
	windowsDomainControllerProductType = 2 // VER_NT_DOMAIN_CONTROLLER
	windowsServerProductType           = 3 // VER_NT_SERVER
)

func expectedPlatformUnavailable(err error) bool {
	return windowsServerNLMUnavailable(windows.RtlGetVersion().ProductType, err)
}

// INetworkCostManager has no supported Windows Server version. Restrict the
// exception to the exact COM activation result for its absent class so a broken
// NLM installation or a later call failure remains visible.
func windowsServerNLMUnavailable(productType byte, err error) bool {
	isServer := productType == windowsDomainControllerProductType || productType == windowsServerProductType
	return isServer &&
		errors.Is(err, ErrUnavailable) &&
		strings.Contains(err.Error(), "Network List Manager: HRESULT 0x80040154")
}

func TestWindowsServerNLMUnavailable(t *testing.T) {
	classMissing := errors.Join(ErrUnavailable, errors.New("Network List Manager: HRESULT 0x80040154"))
	for _, productType := range []byte{windowsDomainControllerProductType, windowsServerProductType} {
		if !windowsServerNLMUnavailable(productType, classMissing) {
			t.Fatalf("Windows Server product type %d class-not-registered result was not recognized", productType)
		}
	}
	if windowsServerNLMUnavailable(windowsWorkstationProductType, classMissing) {
		t.Fatal("a workstation must retain the real NLM requirement")
	}
	for _, productType := range []byte{0, 255} {
		if windowsServerNLMUnavailable(productType, classMissing) {
			t.Fatalf("unknown product type %d was accepted as Windows Server", productType)
		}
	}
	if windowsServerNLMUnavailable(3, errors.Join(ErrUnavailable, errors.New("Network List Manager: HRESULT 0x80004005"))) {
		t.Fatal("an unrelated COM failure was accepted")
	}
	if windowsServerNLMUnavailable(3, errors.New("Network List Manager: HRESULT 0x80040154")) {
		t.Fatal("class-not-registered without ErrUnavailable was accepted")
	}
}
