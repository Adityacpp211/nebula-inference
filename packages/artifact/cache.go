package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FetchResult is what the artifact puller reports, as a file the worker and the
// controller can read, so a checksum failure is a named condition rather than an
// opaque CrashLoopBackOff (docs/deployment-architecture.md §2.2).
type FetchResult struct {
	Status     string `json:"status"` // cache_hit, downloaded, failed
	SHA256     string `json:"sha256"`
	Size       int64  `json:"size_bytes"`
	Path       string `json:"path,omitempty"`
	DurationMS int64  `json:"duration_ms"`
	ErrorCode  string `json:"error_code,omitempty"` // checksum_mismatch, not_found, too_large, io
	Error      string `json:"error,omitempty"`
}

// Fetch statuses.
const (
	FetchCacheHit   = "cache_hit"
	FetchDownloaded = "downloaded"
	FetchFailed     = "failed"
)

// marker records that a cached file was verified, and against what, so a pod
// restart does not re-hash gigabytes. It is keyed on size and modification time:
// anything that rewrites the file invalidates it.
type marker struct {
	SHA256     string    `json:"sha256"`
	Size       int64     `json:"size"`
	ModTime    time.Time `json:"mtime"`
	VerifiedAt time.Time `json:"verified_at"`
}

// Fetch makes the artifact with the given checksum available under
// <cacheDir>/sha256/<hex>, downloading and verifying it when it is not already
// there and verified.
//
// Concurrency: two pods on one node pulling the same artifact both download to
// their own temporary files and rename into place. Both copies are verified before
// the rename, so whichever wins, the bytes are right; the cost is one redundant
// download in a rare race, which is cheaper than a lock that can be left behind by
// a killed pod.
func Fetch(ctx context.Context, open func(context.Context) (io.ReadCloser, error),
	cacheDir, sha256Hex string, size, maxBytes int64,
) FetchResult {
	started := time.Now()
	sha256Hex = strings.ToLower(sha256Hex)
	res := FetchResult{SHA256: sha256Hex, Size: size}
	finish := func(status, code string, err error) FetchResult {
		res.Status = status
		res.DurationMS = time.Since(started).Milliseconds()
		if err != nil {
			res.ErrorCode, res.Error = code, err.Error()
		}
		return res
	}

	key := KeyFor(sha256Hex)
	if !ValidKey(key) {
		return finish(FetchFailed, "invalid_checksum", fmt.Errorf("%q is not a SHA-256 hex digest", sha256Hex))
	}
	path := filepath.Join(cacheDir, filepath.FromSlash(key))
	res.Path = path

	if ok, err := cachedAndVerified(path, sha256Hex, maxBytes); err == nil && ok {
		return finish(FetchCacheHit, "", nil)
	}

	body, err := open(ctx)
	if err != nil {
		code := "io"
		if errors.Is(err, ErrNotFound) {
			code = "not_found"
		}
		return finish(FetchFailed, code, err)
	}
	defer body.Close()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return finish(FetchFailed, "io", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".partial-"+sha256Hex[:12]+"-*")
	if err != nil {
		return finish(FetchFailed, "io", err)
	}
	defer os.Remove(tmp.Name())

	v, err := Hash(io.TeeReader(body, tmp), maxBytes)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		code := "io"
		if errors.Is(err, ErrTooLarge) {
			code = "too_large"
		}
		return finish(FetchFailed, code, err)
	}
	if err := v.Check(sha256Hex, size); err != nil {
		// The bad bytes are discarded (the deferred Remove), never renamed into the
		// cache where the next pod would trust them.
		code := "checksum_mismatch"
		if errors.Is(err, ErrSizeMismatch) {
			code = "size_mismatch"
		}
		return finish(FetchFailed, code, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return finish(FetchFailed, "io", err)
	}
	res.Size = v.Size
	_ = writeMarker(path, sha256Hex)
	return finish(FetchDownloaded, "", nil)
}

// cachedAndVerified reports whether the cached file exists and is known good,
// re-hashing it when its marker does not match.
func cachedAndVerified(path, sha256Hex string, maxBytes int64) (bool, error) {
	st, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if b, err := os.ReadFile(path + ".verified"); err == nil {
		var m marker
		if json.Unmarshal(b, &m) == nil && m.SHA256 == sha256Hex && m.Size == st.Size() && m.ModTime.Equal(st.ModTime()) {
			return true, nil
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(f, maxBytes+1)); err != nil {
		return false, err
	}
	if hex.EncodeToString(h.Sum(nil)) != sha256Hex {
		// A corrupt cache entry: remove it so the download path replaces it.
		_ = os.Remove(path)
		_ = os.Remove(path + ".verified")
		return false, nil
	}
	return true, writeMarker(path, sha256Hex)
}

func writeMarker(path, sha256Hex string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(marker{SHA256: sha256Hex, Size: st.Size(), ModTime: st.ModTime(), VerifiedAt: time.Now().UTC()})
	return os.WriteFile(path+".verified", b, 0o644)
}

// WriteResult writes a FetchResult as JSON, atomically.
func WriteResult(path string, r FetchResult) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(path, strings.NewReader(string(b)+"\n"))
}
