//go:build !windows && !linux && (!darwin || !cgo)

package archive

import "context"

func retireHeldArchiveMember(context.Context, string, string, string, func() error) error {
	return errArchiveOccurrence
}
