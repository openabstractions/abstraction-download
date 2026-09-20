//go:build !windows && !linux && !darwin

package netcost

func open() (Source, error) { return nil, ErrUnavailable }
