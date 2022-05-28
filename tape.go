// Copyright (C) 2010 Square, Inc.
// Copyright (C) 2022 Eric Lagergren
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package tape implements a file-based FIFO queue.
//
// The typical use case is to use an os.File as backing storage
// and has special handling for this use case. However, any type
// that implements io.ReaderAt and io.WriterAt can be used. The
// documentation is written with the assumption that the backing
// storage is an os.File.
//
// Operations
//
// The queue does not support the "pop" operation. This prevents
// callers from accidentally removing data from the queue before
// the operation that uses the data is successful.
//
// The queue does not shrink as elements are removed. To shrink
// the queue, wait until its size drops to zero call Reset once
// or just it and create a new one.
//
// Durability
//
// The queue is designed to be resilient against corruption from
// power loss and program crashes. For example, if an addition to
// the queue fails partway through, the queue will remain
// uncorrupted so long as the 32-byte queue header is updated
// atomically (or not at all). Queue operations are flushed to
// disk after each operation.
//
// On macOS and iOS, operations are flushed to disk using
// fcntl(F_BARRIERFSYNC) instead of calling Sync. This is
// because Sync uses fcntl(F_FULLFSYNC) which (objectively) has
// horrendous performance and, according to Apple, usually isn't
// necessary. For more information, see Apple's documentation[1].
//
// Example usage (error handing is omitted for brevity):
//
//    q, _:= New(f)
//    q.Add([]byte("hello!"))
//    got, _ := q.Peek()
//    if string(got) == "hello!" {
//        // The data is correct, so pop it from the queue.
//        q.Remove()
//    }
//
// It is heavily based off Square's Tape library[0].
//
// [0]: https://github.com/square/tape
// [1]: https://developer.apple.com/documentation/xcode/reducing-disk-writes
package tape

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

// ErrEmpty is returned when the queue is empty.
var ErrEmpty = errors.New("tape: queue is empty")

// CorruptedError is returned when the queue has been
// corrupted.
type CorruptedError struct {
	err error
}

var _ error = (*CorruptedError)(nil)

func (e *CorruptedError) Error() string {
	return fmt.Sprintf("tape: %v", e.err)
}

func (e *CorruptedError) Unwrap() error {
	return e.err
}

func corruptedErr(err error) error {
	return &CorruptedError{
		err: err,
	}
}

// Queue is a file-based FIFO queue.
type Queue struct {
	// f is the underlying file.
	f File
	// size is the total size of the queue.
	size int64
	// count is the number of elements in the queue.
	count int
	// head is the head element in the queue.
	head elem
	// tail is the final element in the queue.
	tail elem
	// buf is a scratch buffer for writing the file header.
	buf [fileHeaderSize]byte
	// gen is the current generation of the queue.
	//
	// It's incremented each time the queue is modified and used
	// to detect queue modifications while iterating.
	gen int

	// grows is the number of calls to grow that increased the
	// size of the queue.
	grows int
	// wraps is the number of additions that wrapped.
	wraps int

	// noSync disables syncing data to disk.
	noSync bool
}

// elem is an element in the queue.
type elem struct {
	// off is the offset of the element in the file.
	//
	// The element's data starts at off+elemHeaderSize.
	off int64
	// len is the length of the element's data.
	len int64
}

// File is the underlying file.
type File interface {
	io.ReaderAt
	io.WriterAt
}

var _ File = (*os.File)(nil)

// Truncater is an optional interface used to grow or shrink the
// size of the File as necessary.
type Truncater interface {
	Truncate(int64) error
}

// Syncer is an optional interface used to flush the File to
// permanent storage.
type Syncer interface {
	Sync() error
}

// Option configures a queue.
type Option func(*qopts)

type qopts struct {
	size   int64 // pre-allocated size
	noSync bool  // disable syncing
}

// WithNoSync disables syncing the queue to permanent storge.
//
// This should only be set in tests to make the disk-based tests
// quicker.
func WithNoSync() Option {
	return func(o *qopts) { o.noSync = true }
}

// WithSize configures the default allocation size for new files.
func WithSize(n int64) Option {
	return func(o *qopts) { o.size = n }
}

// New creates a queue from the File.
func New(f File, opts ...Option) (*Queue, error) {
	o := qopts{
		size: 4096,
	}
	for _, fn := range opts {
		fn(&o)
	}
	if o.size < 0 {
		return nil, fmt.Errorf("tape: size must be >= 0: %d", o.size)
	}
	if o.size < fileHeaderSize {
		o.size = fileHeaderSize
	}

	buf := make([]byte, fileHeaderSize)
	n, err := f.ReadAt(buf, 0)
	if err != nil && err != io.EOF && n > 0 {
		return nil, corruptedErr(fmt.Errorf("unable to read header: %w", err))
	}

	// Empty file.
	if n == 0 {
		q := &Queue{
			f:      f,
			size:   o.size,
			noSync: o.noSync,
		}
		if err := q.init(); err != nil {
			return nil, err
		}
		return q, nil
	}

	var h header
	if err := h.unmarshal(buf); err != nil {
		return nil, corruptedErr(fmt.Errorf("invalid header: %v", err))
	}

	// If the file is smaller than the size listed in the header,
	// the file must be corrupt.
	if f, ok := f.(*os.File); ok {
		info, err := f.Stat()
		if err != nil {
			return nil, err
		}
		if info.Size() < h.size {
			return nil, corruptedErr(fmt.Errorf(
				"file size is shorter than header: %d < %d",
				info.Size(), h.size))
		}
	}

	var head, tail elem
	if h.count > 0 {
		head, err = readElem(f, h.head, h.size)
		if err != nil {
			return nil, corruptedErr(err)
		}
		tail, err = readElem(f, h.tail, h.size)
		if err != nil && err != io.EOF {
			return nil, corruptedErr(err)
		}
	}
	return &Queue{
		f:      f,
		size:   h.size,
		count:  int(h.count),
		head:   head,
		tail:   tail,
		noSync: o.noSync,
	}, nil
}

func readElem(f File, off, size int64) (elem, error) {
	if off >= size {
		off = fileHeaderSize + off - size
	}
	buf := make([]byte, elemHeaderSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		return elem{}, err
	}
	len := binary.LittleEndian.Uint64(buf)
	if len > math.MaxInt64 {
		return elem{}, fmt.Errorf("element header is too large: %d", len)
	}
	return elem{off, int64(len)}, nil
}

// init initializes a new queue.
func (q *Queue) init() error {
	if err := truncate(q.f, q.size); err != nil {
		return err
	}
	h := header{
		version: 1,
		size:    q.size,
	}
	return q.writeHeader(h)
}

// wrap returns the offset wrapped to the size of the file.
func (q *Queue) wrap(off int64) int64 {
	if off < q.size {
		return off
	}
	return fileHeaderSize + off - q.size
}

// used reports the number of bytes currently in use.
func (q *Queue) used() int64 {
	switch {
	// Nothing added, so just the header.
	case q.count == 0:
		return fileHeaderSize
	// Normal case.
	case q.tail.off >= q.head.off:
		return fileHeaderSize +
			(q.tail.off - q.head.off) +
			elemHeaderSize + q.tail.len
	// The file wraps.
	default:
		return q.tail.off +
			elemHeaderSize + q.tail.len +
			q.size - q.head.off
	}
}

// grow increases the size of the queue large enough for
// a write an element of n bytes (excluding its header).
//
// The file is grown to the next power of two large enough to
// write the element.
func (q *Queue) grow(n int) error {
	if math.MaxInt64-int64(n) < elemHeaderSize {
		panic("tape: element too large")
	}
	// elemHeaderSize + n won't overflow

	need := elemHeaderSize + int64(n)
	if q.avail() >= need {
		// We have enough space.
		return nil
	}
	q.grows++

	newSize := q.size
	if newSize == 0 {
		newSize = fileHeaderSize
	}
	used := q.used()
	for newSize-used < need {
		newSize *= 2
		if newSize < 0 {
			panic("tape: file size overflow")
		}
	}
	if err := truncate(q.f, newSize); err != nil {
		return err
	}
	// TODO(eric): call sync here to flush the size of the queue
	// to disk?

	tailEnd := q.wrap(q.tail.off + elemHeaderSize + q.tail.len)
	if tailEnd <= q.head.off {
		// Copy the tail element to the end of the file.
		buf := make([]byte, tailEnd-fileHeaderSize)
		_, err := q.f.ReadAt(buf, fileHeaderSize)
		if err != nil {
			return err
		}
		_, err = q.f.WriteAt(buf, q.size)
		if err != nil {
			return err
		}
	}

	h := header{
		version: 1,
		size:    newSize,
		count:   uint32(q.count),
		head:    q.head.off,
		tail:    q.tail.off,
	}
	if q.tail.off < q.head.off {
		h.tail = q.size + q.tail.off - fileHeaderSize
	}
	if err := q.writeHeader(h); err != nil {
		return err
	}
	q.tail.off = h.tail
	q.size = newSize
	return nil
}

// sync flushes the underlying file to permanent storage, if
// possible.
func (q *Queue) sync(dataOnly bool) error {
	if q.noSync {
		return nil
	}
	if f, ok := q.f.(*os.File); ok {
		return fsync(f, dataOnly)
	}
	if sf, ok := q.f.(Syncer); ok {
		return sf.Sync()
	}
	return nil
}

// writeHeader writes the header to the beginning of the tape
// file.
func (q *Queue) writeHeader(h header) error {
	// Syncing before and after the header write ensures correct
	// ordering. The first sync flushes the previous writes (if
	// any) and the second sync flushes the header. Without the
	// first sync, the header could be flushed before the
	// previous writes, which leaves a gap for corruption to
	// occur.
	if err := q.sync(q.size == h.size); err != nil {
		return err
	}
	h.marshal(q.buf[:])
	if _, err := q.f.WriteAt(q.buf[:], 0); err != nil {
		return err
	}
	return q.sync(q.size == h.size)
}

// write writes len(p) bytes to the queue at off.
//
// It handles wrapping.
func (q *Queue) write(p []byte, off int64) error {
	pos := q.wrap(off)
	if pos+int64(len(p)) <= q.size {
		_, err := q.f.WriteAt(p, pos)
		return err
	}
	q.wraps++

	// Before EOF.
	n, err := q.f.WriteAt(p[:q.size-pos], pos)
	if err != nil {
		return err
	}
	// Start of the file (but after the header).
	_, err = q.f.WriteAt(p[n:], fileHeaderSize)
	return err
}

// read reads len(p) bytes from the queue starting at off.
//
// It handles wrapping.
func (q *Queue) read(p []byte, off int64) error {
	pos := q.wrap(off)
	if pos+int64(len(p)) <= q.size {
		_, err := q.f.ReadAt(p, pos)
		if err == io.EOF {
			err = nil
		}
		return err
	}
	// Before EOF.
	n, err := q.f.ReadAt(p[:q.size-pos], pos)
	if err != nil && err != io.EOF {
		return err
	}
	// Start of the file (but after the header).
	_, err = q.f.ReadAt(p[n:], fileHeaderSize)
	if err == io.EOF {
		err = nil
	}
	return err
}

// Add appends an element to the end of the queue.
func (q *Queue) Add(data []byte) error {
	if q.count+1 < 0 {
		return errors.New("tape: queue too large")
	}
	if err := q.grow(len(data)); err != nil {
		return err
	}

	off := int64(fileHeaderSize)
	empty := q.count == 0
	if !empty {
		off = q.wrap(q.tail.off + elemHeaderSize + q.tail.len)
	}

	buf := make([]byte, elemHeaderSize)
	binary.LittleEndian.PutUint64(buf, uint64(len(data)))
	err := q.write(buf, off)
	if err != nil {
		return err
	}
	err = q.write(data, elemHeaderSize+off)
	if err != nil {
		return err
	}

	head := q.head.off
	if empty {
		head = off
	}
	h := header{
		version: 1,
		size:    q.size,
		count:   uint32(q.count + 1),
		head:    head,
		tail:    off,
	}
	if err := q.writeHeader(h); err != nil {
		return err
	}
	q.tail = elem{off, int64(len(data))}
	q.count++
	if empty {
		q.head = q.tail
	}
	q.gen++
	return nil
}

// Available returns the number of bytes available for new queue
// elements before growing.
func (q *Queue) Available() int64 {
	if m := q.avail(); m > elemHeaderSize {
		return m
	}
	return 0
}

// Cap returns the total space allocated by the queue.
func (q *Queue) Cap() int64 {
	return q.size
}

// avail returns the number of bytes available.
func (q *Queue) avail() int64 {
	return q.size - q.used()
}

// Len returns the number of entries in the queue.
func (q *Queue) Len() int {
	return q.count
}

// Peek returns the first entry from the queue, but does not
// remove it.
func (q *Queue) Peek() ([]byte, error) {
	if q.count == 0 {
		return nil, ErrEmpty
	}
	off := q.head.off + elemHeaderSize
	buf := make([]byte, q.head.len)
	if err := q.read(buf, off); err != nil {
		return nil, err
	}
	return buf, nil
}

// Remove deletes the first n entries from the queue.
//
// If the queue is empty or n <= 0, RemoveN returns nil.
func (q *Queue) Remove(n int) error {
	if q.count <= 0 {
		return nil
	}
	if n >= q.count {
		q.Reset()
		return nil
	}

	newHead := q.head
	buf := make([]byte, elemHeaderSize)
	for i := 0; i < n; i++ {
		newHead.off = q.wrap(newHead.off + elemHeaderSize + newHead.len)
		if err := q.read(buf, newHead.off); err != nil {
			return err
		}
		len := binary.LittleEndian.Uint64(buf)
		if len > math.MaxInt64 {
			return corruptedErr(fmt.Errorf(
				"element header is too large: %d", len))
		}
		newHead.len = int64(len)
	}

	h := header{
		version: 1,
		size:    q.size,
		count:   uint32(q.count - n),
		head:    newHead.off,
		tail:    q.tail.off,
	}
	if err := q.writeHeader(h); err != nil {
		return err
	}
	q.head = newHead
	q.count -= n
	q.gen++
	return nil
}

// Range calls fn for each element element in the queue, or
// until fn returns false.
//
// The slice passed to the function is only valid until the
// function returns.
//
// It is undefined behavior to call Add or Remove while iterating
// over the elements.
func (q *Queue) Range(fn func(p []byte) bool) error {
	gen := q.gen
	buf := make([]byte, elemHeaderSize)
	off := q.head.off
	for i := 0; i < q.count; i++ {
		if q.gen != gen {
			panic("tape: modified while iteraeting")
		}
		err := q.read(buf[:elemHeaderSize], off)
		if err != nil {
			return err
		}
		n := binary.LittleEndian.Uint64(buf)
		if n > math.MaxInt64 {
			return corruptedErr(fmt.Errorf(
				"element header %d is too large: %d", i, n))
		}
		if c := uint64(cap(buf)); c < n {
			buf = append(buf[:cap(buf)], make([]byte, n-c)...)
		}
		err = q.read(buf[:n], off+elemHeaderSize)
		if err != nil {
			return err
		}
		if !fn(buf[:n:n]) {
			break
		}
		off += elemHeaderSize + int64(n)
	}
	return nil
}

// Reset clears the queue to its original size.
func (q *Queue) Reset() error {
	if err := truncate(q.f, fileHeaderSize); err != nil {
		return err
	}
	h := header{
		version: 1,
		size:    fileHeaderSize,
	}
	if err := q.writeHeader(h); err != nil {
		return err
	}
	q.size = fileHeaderSize
	q.count = 0
	q.head = elem{}
	q.tail = elem{}
	return nil
}

// Size reports the number of bytes being used by queue elements
// and metadata.
func (q *Queue) Size() int64 {
	return q.used()
}

const (
	// fileHeaderSize is the size of the queue header.
	//               v   s   c   h   t
	fileHeaderSize = 4 + 8 + 4 + 8 + 8
	// elemHeaderSize is the size of the element header.
	elemHeaderSize = 8
)

// header is the queue header.
type header struct {
	// version identifies the version of this queue.
	version uint32
	// size is the size in bytes of the queue.
	size int64
	// count is the number of elements in the queue.
	count uint32
	// head is the offset in bytes of the first element.
	head int64
	// tail is the offset in bytes of the last element.
	tail int64
}

func (h header) marshal(buf []byte) {
	_ = buf[fileHeaderSize-1]
	binary.LittleEndian.PutUint32(buf[0:4], h.version)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(h.size))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.count))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(h.head))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.tail))
}

func (h *header) unmarshal(data []byte) error {
	if len(data) != fileHeaderSize {
		return fmt.Errorf("header too short: want %d, got %d",
			fileHeaderSize, len(data))
	}
	h.version = binary.LittleEndian.Uint32(data[0:4])
	if h.version != 1 {
		return fmt.Errorf("invalid version: expected %d, got %d",
			1, h.version)
	}
	h.size = int64(binary.LittleEndian.Uint64(data[4:12]))
	h.count = binary.LittleEndian.Uint32(data[12:16])
	h.head = int64(binary.LittleEndian.Uint64(data[16:24]))
	h.tail = int64(binary.LittleEndian.Uint64(data[24:32]))
	return nil
}

func truncate(f File, n int64) error {
	if f, ok := f.(*os.File); ok {
		return fallocate(f, n)
	}
	if tf, ok := f.(Truncater); ok {
		return tf.Truncate(n)
	}
	return nil
}
