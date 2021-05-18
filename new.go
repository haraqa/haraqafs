package haraqafs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func New(name string, opts ...FileOption) (*File, error) {
	// new with defaults
	f := &File{
		flags: os.O_RDWR,
		lock:  make(chan struct{}, 1),
	}
	f.lock <- struct{}{}

	// apply options
	for _, opt := range opts {
		if err := opt(f); err != nil {
			return nil, err
		}
	}

	// check if no volumes spec'd: open single file
	if len(f.volumes) == 0 {
		name = filepath.Clean(name)
		f.volumes = []string{name}
		f.paths = []string{name}
		tmp, err := os.OpenFile(f.paths[0], f.flags, f.perms)
		if err != nil {
			return nil, err
		}
		f.multi = []*os.File{tmp}
		return f, nil
	}
	if f.quorum == 0 {
		f.quorum = 1 + len(f.volumes)/2
	}

	// open files
	var errs []error

	for i := range f.volumes {
		f.paths = append(f.paths, filepath.Join(f.volumes[i], name))
		var err error
		tmp, err := os.OpenFile(f.paths[i], f.flags, f.perms)
		if err != nil {
			errs = append(errs, err)
		}
		f.multi = append(f.multi, tmp)
	}
	if len(f.volumes)-len(errs) < f.quorum {
		// best effort close any open files
		_ = f.Close()
		return nil, aggErrors(errs)
	}

	err := f.consensus()
	if err != nil {
		// best effort close any open files
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func (f *File) consensus() error {
	// quick 1 file check
	if len(f.multi) == 1 && f.multi[0] != nil {
		return nil
	}

	var (
		foundDir, foundFile bool
		sourceIndex               = -1
		sourceSize          int64 = -1
		sourceMod           time.Time
	)
	hashes := hashPool.Get().([][]byte)
	if cap(hashes) < len(f.multi) {
		hashes = append(hashes, make([][]byte, len(f.multi))...)
	}
	hashes = hashes[:len(f.multi)]
	defer func() { hashPool.Put(hashes) }()
	sizes := sizePool.Get().([]int64)[:0]
	defer func() { sizePool.Put(sizes) }()
	for i := len(f.multi) - 1; i >= 0; i-- {
		var err error
		if f.multi[i] == nil {
			continue
		}

		info, err := f.multi[i].Stat()
		if err != nil {
			continue
		}

		if info.IsDir() {
			foundDir = true
		} else {
			foundFile = true
		}
		if foundDir && foundFile {
			return fmt.Errorf("mismatched file types: %w", os.ErrInvalid)
		}

		if isSourceOfTruth(info, f.quorumFail, i, sourceIndex, sourceSize, sourceMod) {
			sourceSize = info.Size()
			sourceMod = info.ModTime()
			sourceIndex = i
		}
		sizes = append(sizes, info.Size())
		if info.Size() == 0 {
			continue
		}
		if f.hashing == nil {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(info.Size()))
			hashes[i] = b[:]
		} else {
			f.hashing.Reset()
			_, e := io.Copy(f.hashing, f.multi[i])
			if e == nil {
				hashes[i] = f.hashing.Sum(nil)
			}
		}
	}

	// TODO: handle directories

	allEqual := true
	for i := range hashes[:len(hashes)-1] {
		if !bytes.Equal(hashes[i], hashes[i+1]) {
			allEqual = false
			break
		}
	}

	// cool, we're already at consensus, moving on
	if allEqual {
		if f.appendOnly {
			f.offset = sizes[0]
		}
		return nil
	}

	// try to find a quorum
	hashMatches := make(map[string]int, len(hashes))
	for i := len(hashes) - 1; i >= 0; i-- {
		if hashes[i] == nil {
			continue
		}
		hashMatches[string(hashes[i])]++
		if hashMatches[string(hashes[i])] >= f.quorum {
			// we've reached a quorum, using the first file in the quorum list as the source of truth
			return f.source(foundDir, i, hashes, sizes)
		}
	}

	// unable to reach quorum
	if sourceSize == -1 {
		return fmt.Errorf("unable to reach quorum in source")
	}
	return f.source(foundDir, sourceIndex, hashes, sizes)
}

func (f *File) source(isDir bool, index int, hashes [][]byte, sizes []int64) error {
	if f.appendOnly {
		f.offset = sizes[index]
	}
	var buf []byte
	if !isDir {
		buf = make([]byte, 1e6)
	}

	for i := range f.multi {
		// check if already equal
		if i == index || bytes.Equal(hashes[i], hashes[index]) {
			continue
		}
		if isDir {
			if err := os.MkdirAll(f.paths[i], f.perms); err != nil {
				return fmt.Errorf("mkdir failed for %s: %w", f.paths[i], err)
			}
			continue
		}
		if f.multi[i] == nil {
			var err error
			f.multi[i], err = os.Create(f.paths[i])
			if err != nil {
				return fmt.Errorf("create failed for %s: %w", f.paths[i], err)
			}
			var n int64
			n, err = io.Copy(f.multi[i], f.multi[index])
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("copy failed for new file %s: %w", f.paths[i], err)
			}
			if n != sizes[index] {
				return fmt.Errorf("copy failed for new file %s: %w", f.paths[i], io.ErrShortWrite)
			}
			continue
		}
		if !f.appendOnly {
			sizes[i] = 0
			if err := f.multi[i].Truncate(0); err != nil {
				return fmt.Errorf("trunc failed for existing file %s: %w", f.paths[i], err)
			}
		}
		if sizes[i] > sizes[index] {
			if err := f.multi[i].Truncate(sizes[index]); err != nil {
				return fmt.Errorf("trunc failed for existing file %s: %w", f.paths[i], err)
			}
			continue
		}
		for sizes[i] < sizes[index] {
			n, err := f.multi[index].ReadAt(buf, sizes[i])
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("read failed for existing file %s: %w", f.paths[index], err)
			}

			// TODO: this could be more efficient if we read once and write to many
			p, err := f.multi[i].Write(buf[:n])
			if err != nil {
				return fmt.Errorf("write failed for existing file %s: %w", f.paths[i], err)
			}
			if p != n {
				return fmt.Errorf("write failed for existing file %s: %w", f.paths[i], io.ErrShortWrite)
			}
			sizes[i] += int64(n)
		}
	}
	return nil
}

type fileAgg struct {
	qf    quorumFailEnum
	index int
	size  int64
	time  time.Time
}

func isSourceOfTruth(info os.FileInfo, qf quorumFailEnum, i, index int, size int64, t time.Time) bool {
	switch qf {
	case QFError:
	case QFWriteFirst:
		return info.ModTime().Before(t) || t.IsZero()
	case QFWriteLast:
		return info.ModTime().After(t)
	case QFOrderFirst:
		return index < 0 || i < index
	case QFOrderLast:
		return i > index
	case QFLongest:
		return info.Size() > size
	case QFShortest:
		return size < 0 || info.Size() < size
	case QFShortestNonZero:
		return info.Size() > 0 && info.Size() < size
	}
	return false
}

type quorumFailEnum int

const (
	QFError = iota
	QFWriteFirst
	QFWriteLast
	QFOrderFirst
	QFOrderLast
	QFLongest
	QFShortest
	QFShortestNonZero
)
