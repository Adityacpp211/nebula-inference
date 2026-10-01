package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Dir is a directory-backed store: <root>/sha256/<hex>. For air-gapped installs and
// tests. It cannot presign, so versions stored here are uploaded out of band.
type Dir struct {
	root string
}

// NewDir returns a directory store, creating the root.
func NewDir(root string) (*Dir, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(abs, "sha256"), 0o755); err != nil {
		return nil, fmt.Errorf("artifact dir: %w", err)
	}
	return &Dir{root: abs}, nil
}

// Scheme implements Store.
func (d *Dir) Scheme() string { return "file" }

// URI implements Store.
func (d *Dir) URI(key string) string { return "file://" + filepath.ToSlash(filepath.Join(d.root, key)) }

// Path is where a key lives on disk.
func (d *Dir) Path(key string) string { return filepath.Join(d.root, filepath.FromSlash(key)) }

// PresignPut implements Store.
func (d *Dir) PresignPut(context.Context, string, time.Duration) (*Upload, error) {
	return nil, ErrPresignUnsupported
}

// Stat implements Store.
func (d *Dir) Stat(_ context.Context, key string) (Info, error) {
	if !ValidKey(key) {
		return Info{}, fmt.Errorf("refusing non-content-addressed key %q", key)
	}
	st, err := os.Stat(d.Path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return Info{}, ErrNotFound
	}
	if err != nil {
		return Info{}, err
	}
	return Info{Size: st.Size()}, nil
}

// Open implements Store.
func (d *Dir) Open(_ context.Context, key string) (io.ReadCloser, error) {
	if !ValidKey(key) {
		return nil, fmt.Errorf("refusing non-content-addressed key %q", key)
	}
	f, err := os.Open(d.Path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}

// Put writes bytes atomically: to a temporary file, then renamed into place, so a
// reader never sees half an artifact.
func (d *Dir) Put(_ context.Context, key string, r io.Reader) error {
	if !ValidKey(key) {
		return fmt.Errorf("refusing non-content-addressed key %q", key)
	}
	return writeAtomic(d.Path(key), r)
}

func writeAtomic(path string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".partial-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if _, err := io.Copy(tmp, r); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
