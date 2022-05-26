//go:build !darwin && (dragonfly || freebsd || netbsd || openbsd)

package tape

import (
	"os"
)

func fallocate(f *os.File, n int64) error {
	return f.Truncate(n)
}

func fsync(f *os.File) error {
	return f.Sync()
}
