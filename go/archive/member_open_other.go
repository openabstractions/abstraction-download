//go:build !linux && !darwin

package archive

import "context"

func openDownloadMember(ctx context.Context, folder, relative string, read bool) (*restoreFile, error) {
	return openRestoreFile(ctx, folder, relative, read)
}
