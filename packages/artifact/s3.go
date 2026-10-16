package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config configures an S3-compatible store.
type S3Config struct {
	// Endpoint is host:port of the store as NEBULA's services reach it.
	Endpoint string
	// PublicEndpoint is host:port as clients reach it, used only in presigned URLs.
	// In kind the control plane talks to minio.nebula-data:9000 while a laptop
	// uploads through a NodePort; the signature covers the host, so presigning must
	// use the host the client will actually call. Empty means Endpoint.
	PublicEndpoint string
	Bucket         string
	AccessKey      string
	SecretKey      string
	Region         string
	UseTLS         bool
	// MaxBytes is announced in upload targets and enforced at verification.
	MaxBytes int64
}

// S3 is the S3 implementation.
type S3 struct {
	cfg     S3Config
	client  *minio.Client
	presign *minio.Client
}

// NewS3 connects to an S3-compatible store. It does not create the bucket: that is
// an install-time decision (the Helm chart's job), and a service that silently
// creates buckets is one that silently writes to the wrong account.
func NewS3(cfg S3Config) (*S3, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, errors.New("artifact store: endpoint and bucket are required")
	}
	mk := func(endpoint string) (*minio.Client, error) {
		return minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
			Secure: cfg.UseTLS,
			Region: cfg.Region,
		})
	}
	c, err := mk(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("artifact store: %w", err)
	}
	p := c
	if cfg.PublicEndpoint != "" && cfg.PublicEndpoint != cfg.Endpoint {
		if p, err = mk(cfg.PublicEndpoint); err != nil {
			return nil, fmt.Errorf("artifact store public endpoint: %w", err)
		}
	}
	return &S3{cfg: cfg, client: c, presign: p}, nil
}

// Scheme implements Store.
func (s *S3) Scheme() string { return "s3" }

// URI implements Store.
func (s *S3) URI(key string) string { return "s3://" + s.cfg.Bucket + "/" + key }

// PresignPut implements Store.
func (s *S3) PresignPut(ctx context.Context, key string, ttl time.Duration) (*Upload, error) {
	if !ValidKey(key) {
		return nil, fmt.Errorf("refusing non-content-addressed key %q", key)
	}
	// The region must be known to presign without a network call from a client
	// whose endpoint may not even be reachable from here.
	u, err := s.presign.PresignedPutObject(ctx, s.cfg.Bucket, key, ttl)
	if err != nil {
		return nil, fmt.Errorf("presigning upload: %w", err)
	}
	return &Upload{
		Method:    "PUT",
		URL:       u.String(),
		Headers:   map[string]string{},
		ExpiresAt: time.Now().Add(ttl).UTC(),
		MaxBytes:  s.cfg.MaxBytes,
	}, nil
}

// Stat implements Store.
func (s *S3) Stat(ctx context.Context, key string) (Info, error) {
	if !ValidKey(key) {
		return Info{}, fmt.Errorf("refusing non-content-addressed key %q", key)
	}
	info, err := s.client.StatObject(ctx, s.cfg.Bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return Info{}, ErrNotFound
		}
		return Info{}, fmt.Errorf("stat %s: %w", key, err)
	}
	return Info{Size: info.Size}, nil
}

// Open implements Store.
func (s *S3) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if !ValidKey(key) {
		return nil, fmt.Errorf("refusing non-content-addressed key %q", key)
	}
	obj, err := s.client.GetObject(ctx, s.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", key, err)
	}
	// GetObject is lazy; Stat forces the request so a missing object is reported
	// here rather than on the first Read.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("opening %s: %w", key, err)
	}
	return obj, nil
}

// Ping checks the bucket exists, for /healthz.
func (s *S3) Ping(ctx context.Context) error {
	ok, err := s.client.BucketExists(ctx, s.cfg.Bucket)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("bucket %q does not exist", s.cfg.Bucket)
	}
	return nil
}

// Put uploads bytes directly. Used by tests and by the development tooling that
// seeds an artifact; clients upload through presigned URLs.
func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if !ValidKey(key) {
		return fmt.Errorf("refusing non-content-addressed key %q", key)
	}
	_, err := s.client.PutObject(ctx, s.cfg.Bucket, key, r, size, minio.PutObjectOptions{})
	return err
}

// EnsureBucket creates the bucket when missing. Development tooling only; see
// NewS3 for why services do not call it.
func (s *S3) EnsureBucket(ctx context.Context) error {
	ok, err := s.client.BucketExists(ctx, s.cfg.Bucket)
	if err != nil || ok {
		return err
	}
	return s.client.MakeBucket(ctx, s.cfg.Bucket, minio.MakeBucketOptions{Region: s.cfg.Region})
}
