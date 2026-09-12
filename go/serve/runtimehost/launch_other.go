//go:build !windows

package runtimehost

import (
	"context"
	"errors"
	"os"
)

const executableName = "openabstractions"

func launch(context.Context, Options) (process, *os.File, *os.File, error) {
	return nil, nil, nil, errors.New("runtimehost: atomic process-tree containment is not implemented on this platform")
}
