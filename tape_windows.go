package tape

import (
	"os"
)

func fallocate(f *os.File, n int64) error {
	return f.Truncate(n)
}

func fsync(f *os.File, _ bool) error {
	return f.Sync()
}
