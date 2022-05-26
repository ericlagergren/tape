package tape

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func fallocate(f *os.File, n int64) error {
	err := ignoringEINTR(func() error {
		return unix.Fallocate(int(f.Fd()), 0, 0, n)
	})
	if err == unix.ENOSYS {
		err = f.Truncate(n)
	}
	runtime.KeepAlive(f)
	return err
}

func fsync(f *os.File, dataOnly bool) error {
	if dataOnly {
		err := ignoringEINTR(func() error {
			return unix.Fdatasync(int(f.Fd()))
		})
		runtime.KeepAlive(f)
		return err
	}
	return f.Sync()
}
