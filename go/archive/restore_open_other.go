//go:build !windows

package archive

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/openabstractions/abstraction-identity/fileid"
)

func openRestoreFile(ctx context.Context, folder, relative string, read bool) (selected *restoreFile, failure error) {
	root, err := fileid.Open(ctx, fileid.Self, folder, fileid.Options{})
	if err != nil {
		return nil, err
	}
	ancestors := []*os.File{root.File()}
	defer func() {
		if selected == nil {
			failure = errors.Join(failure, closeRestoreAncestors(ancestors))
		}
	}()
	current := root
	parts := strings.Split(relative, "/")
	for index, part := range parts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fileid.SafeName(part); err != nil {
			return nil, err
		}
		final := index == len(parts)-1
		child, err := current.Beneath(part, fileid.Options{Read: final && read})
		if err != nil {
			return nil, err
		}
		if final {
			return &restoreFile{File: child.File(), ancestors: ancestors}, nil
		}
		ancestors = append(ancestors, child.File())
		current = child
	}
	return nil, errLink
}
