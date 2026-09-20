//go:build darwin && !cgo

package netcost

// NWPathMonitor needs cgo; a build without it has no cost source.
func open() (Source, error) { return nil, ErrUnavailable }
