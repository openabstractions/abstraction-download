//go:build !windows

package nas

import (
	"context"
	"errors"
)

// The reason is returned rather than folded into an empty list: a host serving
// ten shares and a platform that cannot ask both produced nil before, and the
// caller had nowhere to read the difference. Naming the share by hand is the
// way through this, and it is a thing a person does, not an answer this
// function has.
func shares(context.Context, string) ([]string, error) {
	return nil, errors.New("listing what a host serves needs an SMB client and there is none in the standard library on this platform; name the share instead")
}
