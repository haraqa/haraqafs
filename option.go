package haraqafs

import (
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type FileOption func(f *File) error

func WithQuorum(n int) FileOption {
	return func(f *File) error {
		if n <= 0 {
			return fmt.Errorf("quorum must be greater than 0: %w", os.ErrInvalid)
		}
		f.quorum = n
		return nil
	}
}

func WithFlags(flags int) FileOption {
	return func(f *File) error {
		// we need to be able to read & write files to open & sync
		if flags&os.O_WRONLY != 0 {
			flags = flags ^ os.O_WRONLY
		}
		if flags&os.O_RDWR == 0 {
			flags = flags | os.O_RDWR
		}

		f.flags = flags
		return nil
	}
}

func WithCreate() FileOption {
	return func(f *File) error {
		f.flags = os.O_RDWR | os.O_CREATE | os.O_TRUNC
		f.perms = 0666
		return nil
	}
}

func WithCreateIfNotExist() FileOption {
	return func(f *File) error {
		f.flags = os.O_RDWR | os.O_CREATE
		f.perms = 0666
		return nil
	}
}

func WithVolumes(volumes ...string) FileOption {
	for i := range volumes {
		volumes[i] = filepath.Clean(volumes[i])
	}
	if int64(len(volumes)) > volumeMax {
		atomic.SwapInt64(&volumeMax, int64(len(volumes)))
	}
	return func(f *File) error {
		if len(volumes) == 0 {
			return fmt.Errorf("missing volumes: %w", os.ErrInvalid)
		}
		f.volumes = volumes
		f.paths = pathPool.Get().([]string)[:0]
		f.multi = filePool.Get().([]*os.File)[:0]
		return nil
	}
}

var (
	volumeMax int64 = 1

	pathPool = sync.Pool{New: func() interface{} {
		return make([]string, 0, atomic.LoadInt64(&volumeMax))
	}}
	filePool = sync.Pool{New: func() interface{} {
		return make([]*os.File, 0, atomic.LoadInt64(&volumeMax))
	}}
	hashPool = sync.Pool{New: func() interface{} {
		return make([][]byte, 0, atomic.LoadInt64(&volumeMax))
	}}
	sizePool = sync.Pool{New: func() interface{} {
		return make([]int64, 0, atomic.LoadInt64(&volumeMax))
	}}
)

func WithHashing(h hash.Hash) FileOption {
	return func(f *File) error {
		f.hashing = h
		return nil
	}
}

func WithAppendOnly(appendOnly bool) FileOption {
	return func(f *File) error {
		f.appendOnly = appendOnly
		return nil
	}
}

func WithForceSync(sync bool) FileOption {
	return func(f *File) error {
		f.forceSync = sync
		return nil
	}
}
