package tape

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func fallocate(f *os.File, n int64) error {
	return f.Truncate(n)
}

func fsync(f *os.File, dataOnly bool) error {
	// Use F_BARRIERFSYNC instead of f.Sync because f.Sync calls
	// fcntl(F_FULLFSYNC), which is dreadfully slow on macOS.
	// This is technically less safe (because it only writes to
	// the drive), but is recommended by Apple.
	// See https://developer.apple.com/documentation/xcode/reducing-disk-writes
	err := ignoringEINTR(func() error {
		_, err := unix.FcntlInt(f.Fd(), unix.F_BARRIERFSYNC, 0)
		return err
	})
	runtime.KeepAlive(f)
	return err
}
