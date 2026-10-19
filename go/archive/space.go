package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	cas "github.com/openabstractions/abstraction-cas/go"
)

// The archive's caps (architect ruling of 2026-11-11, DL-R44): no per-app
// quota and no eviction, and a free-space reserve on the archive's volume.

// reserveCap is the most free space a copy leaves on the archive's volume;
// a smaller volume keeps a tenth of its size free.
const reserveCap int64 = 50 << 30

// errNoRoom marks a copy the free-space reserve skips.
var errNoRoom = errors.New("the archive's volume would fall below its free-space reserve")

// volumeSpace reports the free bytes the caller may use and the total bytes
// of the volume holding folder; tests replace it.
var volumeSpace = diskSpace

// CheckWriteSpace refuses a write of size bytes that would leave the archive's volume
// with less than the smaller of 10% of its size and 50 GB free. A volume
// that does not answer within the probe bound is unreachable; one whose
// space this platform cannot read takes the copy, and a full volume fails
// the write itself.
func CheckWriteSpace(folder string, size int64) error {
	type answer struct {
		free, total int64
		err         error
	}
	done := make(chan answer, 1)
	go func() {
		free, total, err := volumeSpace(folder)
		done <- answer{free, total, err}
	}()
	var got answer
	timer := time.NewTimer(probeTimeout)
	defer timer.Stop()
	select {
	case got = <-done:
	case <-timer.C:
		return errSlow
	}
	if got.err != nil {
		return nil
	}
	reserve := min(got.total/10, reserveCap)
	if got.free-size < reserve {
		return fmt.Errorf("%w (%d bytes free, %d kept free)", errNoRoom, got.free, reserve)
	}
	return nil
}

// writtenName is the state file that counts the bytes the write-back put in
// the archive for each app.
const writtenName = "download-archive-written.json"

func writtenPath() (string, error) {
	dir, err := stateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, writtenName), nil
}

// recordWritten adds bytes to app's count.
func recordWritten(app string, bytes int64) {
	path, err := writtenPath()
	if err != nil {
		return
	}
	err = cas.ChangeLimitContext(context.Background(), path, 1<<20, func(raw []byte) ([]byte, error) {
		counts := map[string]int64{}
		if len(raw) > 0 && json.Unmarshal(raw, &counts) != nil {
			counts = map[string]int64{}
		}
		counts[app] += bytes
		return json.Marshal(counts)
	})
	if err != nil {
		logf("the bytes written for %s were not counted: %v", app, err)
	}
}

// Usage is the archive's size from the stored listing and the bytes the
// write-back put there for each app. Reading it touches no folder; an
// unreadable count reads as none.
func Usage() (listed int64, written map[string]int64) {
	if index, ok := ReadListing(); ok {
		for _, file := range index.Files {
			listed += file.Size
		}
	}
	written = map[string]int64{}
	path, err := writtenPath()
	if err != nil {
		return listed, written
	}
	raw, err := cas.ReadLimit(path, 1<<20)
	if err != nil || len(raw) == 0 || json.Unmarshal(raw, &written) != nil {
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			logf("the bytes written were not read: %v", err)
		}
		return listed, map[string]int64{}
	}
	return listed, written
}
