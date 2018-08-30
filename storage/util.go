package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Reader wraps an io.Reader with one that checks ctx.Done() on each Read call.
//
// If ctx has a deadline and if r has a `SetReadDeadline(time.Time) error` method,
// then it is called with the deadline.
//
// Imported from https://gist.github.com/dchapes/6c992bf3e943934462509338cd213e99
func ContextReader(ctx context.Context, r io.Reader) io.Reader {
	if deadline, ok := ctx.Deadline(); ok {
		type deadliner interface {
			SetReadDeadline(time.Time) error
		}
		if d, ok := r.(deadliner); ok {
			d.SetReadDeadline(deadline)
		}
	}
	return reader{ctx, r}
}

type reader struct {
	ctx context.Context
	r   io.Reader
}

func (r reader) Read(p []byte) (n int, err error) {
	if err = r.ctx.Err(); err != nil {
		return
	}
	if n, err = r.r.Read(p); err != nil {
		return
	}
	err = r.ctx.Err()
	return
}

// FileSize returns the file size in bytes, or return 0 if there's an error calling os.Stat().
func FileSize(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

// exists returns whether the given file or directory exists or not
func exists(p string) (bool, error) {
	_, err := os.Stat(p)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// EnsureDir ensures a directory exists.
func EnsureDir(p string) error {
	e, err := exists(p)
	if err != nil {
		return err
	}
	if !e {
		// TODO configurable mode?
		err := os.MkdirAll(p, 0775)
		if err != nil {
			return err
		}
	}
	return nil
}

// EnsurePath ensures a directory exists, given a file path. This calls path.Dir(p)
func EnsurePath(p string) error {
	dir := filepath.Dir(p)
	return EnsureDir(dir)
}
