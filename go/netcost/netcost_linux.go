//go:build linux

package netcost

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// systemBus returns the system bus's socket address: DBUS_SYSTEM_BUS_ADDRESS
// when it names a unix path, else the well-known path.
func systemBus() (network, address string) {
	for _, part := range strings.Split(os.Getenv("DBUS_SYSTEM_BUS_ADDRESS"), ";") {
		part = strings.TrimPrefix(part, "unix:")
		for _, kv := range strings.Split(part, ",") {
			switch {
			case strings.HasPrefix(kv, "path="):
				return "unix", strings.TrimPrefix(kv, "path=")
			case strings.HasPrefix(kv, "abstract="):
				return "unix", "@" + strings.TrimPrefix(kv, "abstract=")
			}
		}
	}
	return "unix", "/run/dbus/system_bus_socket"
}

func open() (Source, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nmOpenLimit*time.Second)
	defer cancel()
	network, address := systemBus()
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("%w: system bus: %v", ErrUnavailable, err)
	}
	return openNetworkManager(ctx, conn, os.Getuid())
}
