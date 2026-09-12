//go:build !windows

package runtimehost

import (
	"context"
	"strings"
	"testing"
)

func TestContainmentUnavailable(t *testing.T) {
	h, err := Start(context.Background(), Options{Executable: "/unavailable/openabstractions"})
	if h != nil || err == nil || !strings.Contains(err.Error(), "containment is not implemented") {
		t.Fatal(h, err)
	}
}
