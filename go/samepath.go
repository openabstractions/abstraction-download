package download

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
)

// SamePath reports whether a and b name one file.
//
// The question belongs to the filesystem holding them, never to their bytes:
// NTFS folds `café` against `CAFÉ` and keeps U+212A apart, APFS folds both,
// ext4 folds neither, and one volume can be mounted differently from the next.
// os.SameFile is the standard library's answer and it compares volume serial
// and file id, which is where the answer actually lives — Python spells it
// os.path.samefile, C++17 spells it std::filesystem::equivalent, Java spells it
// Files.isSameFile. See ../CONTRACT.md, "Two paths, one file".
//
// A destination usually does not exist yet, which those four cannot answer. Its
// PARENT does, so the parent is settled the same way and the final component is
// settled by asking the parent directory to demonstrate its own rule.
func SamePath(a, b string) bool {
	if a == "" || b == "" {
		return a == b
	}
	if abs, err := filepath.Abs(a); err == nil {
		a = abs
	}
	if abs, err := filepath.Abs(b); err == nil {
		b = abs
	}
	a, b = filepath.Clean(a), filepath.Clean(b)
	if a == b {
		return true
	}

	fa, ea := os.Stat(a)
	fb, eb := os.Stat(b)
	if ea == nil && eb == nil {
		return os.SameFile(fa, fb)
	}
	// One spelling resolves and the other does not, so the volume holding them
	// does not treat them as one name. This is an answer, not a failure.
	if ea == nil || eb == nil {
		return false
	}
	if !os.IsNotExist(ea) || !os.IsNotExist(eb) {
		return false
	}

	da, na := filepath.Split(a)
	db, nb := filepath.Split(b)
	if na == "" || nb == "" {
		return false
	}
	if !SamePath(filepath.Clean(da), filepath.Clean(db)) {
		return false
	}
	if na == nb {
		return true
	}
	if !mayFold(na, nb) {
		return false
	}
	return foldsTogether(nearestDir(filepath.Clean(da)), na, nb)
}

// mayFold is the filter that keeps the probe off the common path. Two names
// holding no byte above 0x7F and still differing after A–Z is folded cannot be
// one file anywhere: case mapping and Unicode normalisation are both the
// identity on ASCII apart from the letters. So `llama.gguf` against
// `mistral.gguf` costs nothing, and only a real candidate reaches the volume.
func mayFold(x, y string) bool {
	if !isASCII(x) || !isASCII(y) {
		return true
	}
	if len(x) != len(y) {
		return false
	}
	for i := 0; i < len(x); i++ {
		if asciiLower(x[i]) != asciiLower(y[i]) {
			return false
		}
	}
	return true
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

func asciiLower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + 32
	}
	return c
}

// nearestDir is the closest ancestor that exists. On Windows a directory's
// case sensitivity is a per-directory flag inherited from its parent at
// creation, so the nearest existing ancestor is the one that will decide for
// the directories still to be made under it.
func nearestDir(dir string) string {
	for {
		if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
			return dir
		}
		up := filepath.Dir(dir)
		if up == dir {
			return ""
		}
		dir = up
	}
}

var folded sync.Map

// foldsTogether asks dir whether it keeps x and y as two names, by making one
// and looking for the other. Both are carried on a name of our own so that
// neither destination is created as a side effect of being asked about.
func foldsTogether(dir, x, y string) bool {
	if dir == "" {
		return false
	}
	if x > y {
		x, y = y, x
	}
	key := dir + "\x00" + x + "\x00" + y
	if v, ok := folded.Load(key); ok {
		return v.(bool)
	}
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return false
	}
	tag := ".abstraction-samepath-" + hex.EncodeToString(seed[:]) + "-"
	made := filepath.Join(dir, tag+x)
	f, err := os.OpenFile(made, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return false
	}
	f.Close()
	defer os.Remove(made)
	mine, err := os.Stat(made)
	if err != nil {
		return false
	}
	other, err := os.Stat(filepath.Join(dir, tag+y))
	same := err == nil && os.SameFile(mine, other)
	folded.Store(key, same)
	return same
}
