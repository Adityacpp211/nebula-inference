// Package artifact is NEBULA's content-addressed model artifact store.
//
// Weights are large, immutable and identified by their SHA-256, so they live in an
// object store under sha256/<hex> and are verified by reading the bytes that were
// actually received — never by trusting a declared checksum
// (docs/security-boundaries.md §2, B7). The key IS the checksum: a cache hit is
// provably the right bytes, two deployments of one version download once, and
// rollback never re-downloads.
//
// Two implementations of one interface: S3 (MinIO in development, any S3-compatible
// store in production) and a local directory, which exists for air-gapped installs
// and tests rather than as a shortcut. The interface is deliberately small — the
// registry needs presign, stat and read, and the node cache needs read.
package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

// Store is an artifact store.
type Store interface {
	// PresignPut returns a target the client uploads to directly, so multi-gigabyte
	// weights never pass through the JSON API. Nil with ErrPresignUnsupported for a
	// store that cannot presign.
	PresignPut(ctx context.Context, key string, ttl time.Duration) (*Upload, error)
	// Stat reports an object's size; ErrNotFound when absent.
	Stat(ctx context.Context, key string) (Info, error)
	// Open streams an object.
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	// URI is the artifact_uri the registry records for a key.
	URI(key string) string
	// Scheme is the URI scheme this store answers for ("s3", "file").
	Scheme() string
}

// Upload is a presigned upload target (docs/api.md §4, the two-phase upload).
type Upload struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expires_at"`
	MaxBytes  int64             `json:"max_bytes"`
}

// Info describes a stored object.
type Info struct {
	Size int64
}

// Errors.
var (
	ErrNotFound           = errors.New("artifact not found")
	ErrPresignUnsupported = errors.New("this artifact store cannot issue presigned uploads")
	ErrTooLarge           = errors.New("artifact exceeds the size limit")
	ErrChecksumMismatch   = errors.New("artifact checksum does not match")
	ErrSizeMismatch       = errors.New("artifact size does not match")
)

// KeyFor is the content address of a checksum.
func KeyFor(sha256Hex string) string { return "sha256/" + strings.ToLower(sha256Hex) }

var keyPattern = regexp.MustCompile(`^sha256/[0-9a-f]{64}$`)

// ValidKey reports whether a key is a content address. Anything else is refused
// before it reaches a store, so a key can never become a path traversal.
func ValidKey(key string) bool { return keyPattern.MatchString(key) }

// Verified is the result of reading an artifact end to end.
type Verified struct {
	SHA256 string
	Size   int64
}

// Hash reads r to the end, computing SHA-256 and counting bytes, and refuses to
// read past maxBytes. It is the one function that turns bytes into a checksum, used
// by the registry's finalize and by the node cache, so the two can never disagree
// about what "verified" means.
func Hash(r io.Reader, maxBytes int64) (Verified, error) {
	h := sha256.New()
	limit := io.LimitReader(r, maxBytes+1)
	n, err := io.Copy(h, limit)
	if err != nil {
		return Verified{}, fmt.Errorf("reading artifact: %w", err)
	}
	if n > maxBytes {
		return Verified{}, fmt.Errorf("%w: more than %d bytes", ErrTooLarge, maxBytes)
	}
	return Verified{SHA256: hex.EncodeToString(h.Sum(nil)), Size: n}, nil
}

// Check compares what was read with what was declared.
func (v Verified) Check(sha256Hex string, size int64) error {
	if !strings.EqualFold(v.SHA256, sha256Hex) {
		return fmt.Errorf("%w: declared %s, computed %s", ErrChecksumMismatch, strings.ToLower(sha256Hex), v.SHA256)
	}
	if size > 0 && v.Size != size {
		return fmt.Errorf("%w: declared %d bytes, read %d", ErrSizeMismatch, size, v.Size)
	}
	return nil
}

// ParseURI splits an artifact_uri into scheme and key: "s3://bucket/sha256/<hex>"
// gives ("s3", "sha256/<hex>").
func ParseURI(uri string) (scheme, bucket, key string, err error) {
	scheme, rest, ok := strings.Cut(uri, "://")
	if !ok {
		return "", "", "", fmt.Errorf("artifact uri %q has no scheme", uri)
	}
	switch scheme {
	case "s3":
		bucket, key, ok = strings.Cut(rest, "/")
		if !ok || bucket == "" {
			return "", "", "", fmt.Errorf("artifact uri %q has no bucket", uri)
		}
	case "file":
		// A file URI names an absolute directory followed by sha256/<hex>; the key
		// is those last two segments.
		i := strings.LastIndex(rest, "/sha256/")
		if i < 0 {
			return "", "", "", fmt.Errorf("artifact uri %q is not content-addressed", uri)
		}
		bucket, key = rest[:i], rest[i+1:]
	default:
		return "", "", "", fmt.Errorf("artifact uri scheme %q is not supported", scheme)
	}
	if !ValidKey(key) {
		return "", "", "", fmt.Errorf("artifact uri %q is not content-addressed (sha256/<hex>)", uri)
	}
	return scheme, bucket, key, nil
}
