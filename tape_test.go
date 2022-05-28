package tape

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/bcmills/more/morebytes"
	"github.com/dsnet/try"
)

func (q *Queue) GoString() string {
	var b strings.Builder
	b.WriteString("{\n")
	fmt.Fprintf(&b, "\tfile: %q,\n", q)
	fmt.Fprintf(&b, "\tsize: %d,\n", q.size)
	fmt.Fprintf(&b, "\tcount: %d,\n", q.count)
	fmt.Fprintf(&b, "\thead: %#v,\n", q.head)
	fmt.Fprintf(&b, "\ttail: %#v,\n", q.tail)
	fmt.Fprintf(&b, "\tgen: %d,\n", q.gen)
	fmt.Fprintf(&b, "\tgrows: %d,\n", q.grows)
	fmt.Fprintf(&b, "\twraps: %d,\n", q.wraps)
	fmt.Fprintf(&b, "\tnoSync: %t,\n", q.noSync)
	b.WriteString("}")
	return b.String()
}

func (q *Queue) String() string {
	r := io.NewSectionReader(q.f, 0, q.size)
	buf, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%#x", buf)
}

// randbuf returns n random bytes.
func randbuf(n int) []byte {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return buf
}

// byteRepeat is bytes.Repeat but with a single character.
func byteRepeat(c byte, n int) []byte {
	return bytes.Repeat([]byte{c}, n)
}

// mktemp creates a temporary file.
func mktemp(tb testing.TB) *os.File {
	f, err := os.CreateTemp(tb.TempDir(), "")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		f.Close()
	})
	return f
}

// clone returns a copy of the queue's underlying file.
//
// The returned File might not have the same underlying type as
// the queue's file.
func clone(t *testing.T, q *Queue) File {
	var f File
	switch v := q.f.(type) {
	case *os.File:
		f = v
	default:
		f = new(morebytes.File)
	}
	r := io.NewSectionReader(q.f, 0, q.size)
	buf, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	if _, err := f.WriteAt(buf, 0); err != nil {
		panic(err)
	}
	return f
}

// runTests runs fn with different File types.
func runTests(t *testing.T, fn func(t *testing.T, fn func() File)) {
	t.Run("memory", func(t *testing.T) {
		fn(t, func() File {
			return new(morebytes.File)
		})
	})
	t.Run("disk", func(t *testing.T) {
		fn(t, func() File {
			return mktemp(t)
		})
	})
}

// TestExisting tests calling New with an existing file.
func TestExisting(t *testing.T) {
	runTests(t, func(t *testing.T, fn func() File) {
		testExisting(t, fn())
	})
}

func testExisting(t *testing.T, f File) {
	q1, err := New(f,
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		if err := q1.Add([]byte{byte(i)}); err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
	}

	q2, err := New(clone(t, q1),
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 256; i++ {
		want, err := q1.Peek()
		if err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
		got, err := q2.Peek()
		if err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("expected %x, got %x", want, got)
		}
		if err := q1.Remove(1); err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
		if err := q2.Remove(1); err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
	}
}

// TestAddZero tests adding zero-sized elements.
func TestAddZero(t *testing.T) {
	runTests(t, func(t *testing.T, fn func() File) {
		testAddZero(t, fn())
	})
}

func testAddZero(t *testing.T, f File) {
	const (
		size = 256
		N    = (256 - fileHeaderSize) / elemHeaderSize
	)
	q, err := New(f,
		WithSize(size),
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		if err := q.Add(nil); err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
	}
	err = q.Range(func(got []byte) bool {
		if len(got) != 0 {
			t.Fatalf("expected 0, got %d", len(got))
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestAddWrap tests the behavior of Add when the addition wraps
// around the end of the file.
func TestAddWrap(t *testing.T) {
	runTests(t, func(t *testing.T, fn func() File) {
		testAddWrap(t, fn())
	})
}

func testAddWrap(t *testing.T, f File) {
	defer try.F(t.Fatal)

	// Create a queue with almost enough space for 4 3-byte
	// elements.
	const (
		elemSize = elemHeaderSize + 3
		fileSize = fileHeaderSize + (4 * elemSize) - 1
	)
	q, err := New(f,
		WithSize(fileSize),
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}

	elems := make([][]byte, 5)
	for i := range elems {
		elems[i] = byteRepeat(byte(i+1), 3)
	}

	try.E(q.Add(elems[0]))
	try.E(q.Add(elems[1]))
	try.E(q.Add(elems[2]))
	if q.grows != 0 {
		t.Fatalf("expected %d, got %d", 0, q.grows)
	}
	if q.wraps != 0 {
		t.Fatalf("expected %d, got %d", 1, q.wraps)
	}

	try.E(q.Remove(1))
	try.E(q.Add(elems[3]))
	if q.grows != 0 {
		t.Fatalf("expected %d, got %d", 0, q.grows)
	}
	if q.wraps != 1 {
		t.Fatalf("expected %d, got %d", 1, q.wraps)
	}

	i := 1
	err = q.Range(func(got []byte) bool {
		want := elems[i]
		if !bytes.Equal(want, got) {
			t.Fatalf("#%d: expected %x, got %x", i, want, got)
		}
		i++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	try.E(q.Add(elems[4]))

	for i := 1; i < len(elems); i++ {
		got, err := q.Peek()
		if err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
		try.E(q.Remove(1))

		want := elems[i]
		if !bytes.Equal(want, got) {
			t.Fatalf("#%d: expected %x, got %x", i, want, got)
		}
	}
}

// TestAddExpand tests the case where we have to move the wrapped
// chunk from the beginning of the file to the end of the file.
func TestAddExpand(t *testing.T) {
	runTests(t, func(t *testing.T, fn func() File) {
		testAddExpand(t, fn())
	})
}

func testAddExpand(t *testing.T, f File) {
	defer try.F(t.Fatal)

	const (
		fileSize = 256
	)
	q, err := New(f,
		WithSize(fileSize),
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}

	elems := make([][]byte, 6)
	for i := range elems {
		const (
			size = (fileSize-fileHeaderSize)/4 - elemHeaderSize
		)
		elems[i] = byteRepeat(byte(i+1), size)
	}

	// The queue should be full.
	try.E(q.Add(elems[0]))
	try.E(q.Add(elems[1]))
	try.E(q.Add(elems[2]))
	try.E(q.Add(elems[3]))

	// Remove the first block, allowing the next element to be
	// added in the first slot.
	try.E(q.Remove(1))
	try.E(q.Add(elems[4]))
	if q.grows != 0 {
		t.Fatalf("expected %d, got %d", 0, q.grows)
	}
	if q.wraps != 0 {
		t.Fatalf("expected %d, got %d", 0, q.wraps)
	}

	// This block will cause the queue to grow, making the
	// previous element contiguous.
	try.E(q.Add(elems[5]))
	if q.grows != 1 {
		t.Fatalf("expected %d, got %d", 1, q.grows)
	}

	for i := 1; i < len(elems); i++ {
		got, err := q.Peek()
		if err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
		want := elems[i]
		if !bytes.Equal(want, got) {
			t.Fatalf("#%d: expected %x, got %x", i, want, got)
		}
		try.E(q.Remove(1))
	}
}

// TestRemove tests the Remove method.
func TestRemove(t *testing.T) {
	runTests(t, func(t *testing.T, fn func() File) {
		testRemove(t, fn())
	})
}

func testRemove(t *testing.T, f File) {
	q, err := New(f,
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}

	const (
		N = 4096
	)
	elems := make([][]byte, N)
	for i := range elems {
		want := randbuf(i)
		if err := q.Add(want); err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
		elems[i] = want
	}

	for i, want := range elems {
		got, err := q.Peek()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("#%d: expected %q, got %q", i, want, got)
		}
		if err := q.Remove(1); err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
		if i+1 < len(elems) {
			want = elems[i+1]
			got, err := q.Peek()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("#%d: expected %q, got %q", i, want, got)
			}
		}
	}

	// The queue should now be empty.
	//
	// Check that calling Remove on an empty queue does not
	// return an error.
	for i := 0; i < 2; i++ {
		_, err = q.Peek()
		if !errors.Is(err, ErrEmpty) {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := q.Remove(1); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRemoveN tests the internal remove method.
func TestRemoveN(t *testing.T) {
	runTests(t, func(t *testing.T, fn func() File) {
		testRemoveN(t, fn(), fn())
	})
}

func testRemoveN(t *testing.T, fa, fb File) {
	a, err := New(fa,
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(fb,
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}

	const (
		N = 8
	)
	elems := make([][]byte, N)
	for i := range elems {
		elems[i] = randbuf(i)
	}

	// Check that remove(n) is equivalent to calling Remove
	// n times.
	for n := 0; n < N; n++ {
		if err := a.Reset(); err != nil {
			t.Fatalf("#%d: %v", n, err)
		}
		if err := b.Reset(); err != nil {
			t.Fatalf("#%d: %v", n, err)
		}
		for i, e := range elems {
			if err := a.Add(e); err != nil {
				t.Fatalf("#%d: %v", i, err)
			}
			if err := b.Add(e); err != nil {
				t.Fatalf("#%d: %v", i, err)
			}
		}

		for i := 0; i < n; i++ {
			if err := a.Remove(1); err != nil {
				t.Fatalf("#%d: %v", i, err)
			}
		}
		if err := b.Remove(n); err != nil {
			t.Fatalf("#%d: %v", n, err)
		}

		check := func(v *Queue) {
			t.Helper()

			i := n
			err := v.Range(func(got []byte) bool {
				if i >= len(elems) {
					t.Fatalf("out of range: %d", i)
				}
				want := elems[i]
				if !bytes.Equal(got, want) {
					t.Fatalf("#%d: expected %q, got %q", i, want, got)
				}
				i++
				return true
			})
			if err != nil {
				t.Fatalf("#%d: %v", n, err)
			}
		}
		check(a)
		check(b)
	}
}

// TestEmptyRange tests that iterating over an empty queue does
// not invoke the callback.
func TestEmptyRange(t *testing.T) {
	runTests(t, func(t *testing.T, fn func() File) {
		testEmptyRange(t, fn())
	})
}

func testEmptyRange(t *testing.T, f File) {
	q, err := New(f,
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = q.Range(func(got []byte) bool {
		t.Fatal("should not be called")
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestRange test iterating over a queue.
func TestRange(t *testing.T) {
	runTests(t, func(t *testing.T, fn func() File) {
		testRange(t, fn())
	})
}

func testRange(t *testing.T, f File) {
	q, err := New(f,
		WithNoSync(),
	)
	if err != nil {
		t.Fatal(err)
	}

	const (
		N = 4096
	)
	elems := make([][]byte, N)
	for i := range elems {
		want := randbuf(i)
		if err := q.Add(want); err != nil {
			t.Fatalf("#%d: %v", i, err)
		}
		elems[i] = want
	}

	i := 0
	err = q.Range(func(got []byte) bool {
		want := elems[i]
		if len(got) != len(want) {
			t.Fatalf("#%d: expected %d, got %d", i, len(want), len(got))
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("#%d: expected %x, got %x", i, want, got)
		}
		i++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkAdd(b *testing.B) {
	b.Run("memory", func(b *testing.B) {
		benchAdd(b, new(morebytes.File))
	})
	b.Run("disk", func(b *testing.B) {
		benchAdd(b, mktemp(b))
	})
}

func benchAdd(b *testing.B, f File) {
	q, err := New(f,
		WithNoSync(),
	)
	if err != nil {
		b.Fatal(err)
	}
	text := []byte("hello, world!")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := q.Add(text); err != nil {
			b.Fatal(err)
		}
	}
}
