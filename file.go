package haraqafs

import (
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
)

type File struct {
	quorum     int
	volumes    []string
	flags      int
	perms      os.FileMode
	hashing    hash.Hash
	appendOnly bool
	quorumFail quorumFailEnum
	forceSync  bool

	paths  []string
	multi  []*os.File
	offset int64
	lock   chan struct{}
}

func (f *File) Chdir() error {
	return fmt.Errorf("haraqafs files are not directories...yet: %w", os.ErrInvalid)
}

func (f *File) Chmod(mode os.FileMode) error {
	if f == nil || len(f.multi) == 0 || f.lock == nil {
		return os.ErrInvalid
	}
	if _, ok := <-f.lock; !ok {
		return os.ErrClosed
	}
	defer func() { f.lock <- struct{}{} }()

	for i := range f.multi {
		err := f.multi[i].Chmod(mode)
		if err != nil {
			if i > 0 {
				// best effort, try to undo what we've set so far
				if info, e := f.multi[len(f.multi)-1].Stat(); e == nil {
					m := info.Mode()
					for j := range f.multi[:i] {
						_ = f.multi[j].Chmod(m)
					}
				}
			}
			return fmt.Errorf("unable to chmod file at path %s: %w", f.paths[i], err)
		}
	}
	return nil
}

func (f *File) Chown(uid int, gid int) error {
	if f == nil || len(f.multi) == 0 || f.lock == nil {
		return os.ErrInvalid
	}
	if _, ok := <-f.lock; !ok {
		return os.ErrClosed
	}
	defer func() { f.lock <- struct{}{} }()

	for i := range f.multi {
		err := f.multi[i].Chown(uid, gid)
		if err != nil {
			//TODO: best effort, try to undo what we've set so far
			//if i > 0 {
			//	if info, e := f.multi[len(f.multi)-1].Stat(); e == nil {
			//	  	m := info.Mode()
			//		for j := range f.multi[:i] {
			//			_ = f.multi[j].Chown(...)
			//		}
			//	}
			//}
			return fmt.Errorf("unable to chown file at path %s: %w", f.paths[i], err)
		}
	}
	return nil
}

func (f *File) Close() error {
	if f == nil || len(f.multi) == 0 || f.lock == nil {
		return os.ErrInvalid
	}
	if _, ok := <-f.lock; !ok {
		return os.ErrClosed
	}

	var errs []error
	var closedErrs int
	for i := range f.multi {
		if f.multi[i] == nil {
			continue
		}

		err := f.multi[i].Close()
		if err != nil {
			if errors.Is(err, os.ErrClosed) {
				closedErrs++
			}
			err = fmt.Errorf("close %s: %w", f.paths[i], err)
			errs = append(errs, err)
		}
	}

	// if the only errors we got are closed, then we started in a partial close state but succeeded this time
	if len(errs) == 0 || len(errs) == closedErrs {
		close(f.lock)
		pathPool.Put(f.paths[:0])
		filePool.Put(f.multi[:0])
		if len(f.multi) == closedErrs {
			return os.ErrClosed
		}
		return nil
	}

	// we failed to close, unlock and maybe try again
	defer func() { f.lock <- struct{}{} }()

	// aggregate errors
	return aggErrors(errs)
}

func (f *File) Name() string {
	if f == nil || len(f.multi) == 0 {
		return ""
	}

	return f.multi[0].Name()
}

func (f *File) Read(b []byte) (int, error) {
	return f.ReadAt(b, f.offset)
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	if f == nil || len(f.multi) == 0 || f.lock == nil {
		return 0, os.ErrInvalid
	}
	if _, ok := <-f.lock; !ok {
		return 0, os.ErrClosed
	}
	defer func() { f.lock <- struct{}{} }()

	var n int
	var err error
	for i := len(f.multi) - 1; i >= 0; i-- {
		n, err = f.multi[i].ReadAt(b, off)
		if err == nil || n > 0 {
			break
		}
	}
	f.offset += int64(n)

	return n, err
}

type DirEntry = fs.DirEntry

func (f *File) ReadDir(n int) ([]DirEntry, error) {
	if f == nil || len(f.multi) == 0 || f.lock == nil {
		return nil, os.ErrInvalid
	}
	if _, ok := <-f.lock; !ok {
		return nil, os.ErrClosed
	}
	defer func() { f.lock <- struct{}{} }()

	// TODO: full parsing & support
	dirs, err := f.multi[0].ReadDir(n)
	if err != nil {
		return []DirEntry{}, err
	}
	return dirs, nil
}

func (f *File) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		//TODO: seek from end
	}
	return f.offset, nil
}

func (f *File) Truncate(size int64) error {
	if f == nil || len(f.multi) == 0 || f.lock == nil {
		return os.ErrInvalid
	}
	if _, ok := <-f.lock; !ok {
		return os.ErrClosed
	}
	defer func() { f.lock <- struct{}{} }()

	for i := range f.multi {
		if err := f.multi[i].Truncate(size); err != nil {
			return err
		}
	}
	return nil
}

func (f *File) Write(b []byte) (int, error) {
	return f.WriteAt(b, f.offset)
}

func (f *File) WriteString(s string) (int, error) {
	return f.WriteAt([]byte(s), f.offset)
}

func (f *File) WriteAt(b []byte, offset int64) (int, error) {
	if f == nil || len(f.multi) == 0 || f.lock == nil {
		return 0, os.ErrInvalid
	}
	if _, ok := <-f.lock; !ok {
		return 0, os.ErrClosed
	}
	defer func() { f.lock <- struct{}{} }()

	for i := range f.multi {
		n, err := f.multi[i].WriteAt(b, offset)
		if err != nil {
			return 0, fmt.Errorf("write failed on file %s: %w", f.paths[i], err)
		}
		if n != len(b) {
			return 0, fmt.Errorf("write failed on file %s: %w", f.paths[i], io.ErrShortWrite)
		}
		if f.forceSync {
			if err = f.multi[i].Sync(); err != nil {
				return 0, fmt.Errorf("sync failed on file %s: %w", f.paths[i], err)
			}
		}
	}
	f.offset += int64(len(b))
	return len(b), nil
}
