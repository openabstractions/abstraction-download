//go:build !windows

package netcost

func expectedPlatformUnavailable(error) bool { return false }
