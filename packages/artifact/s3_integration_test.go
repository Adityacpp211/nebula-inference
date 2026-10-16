package artifact_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/artifact"
)

// S3 integration tests run against a real S3-compatible server when
// NEBULA_TEST_S3_ENDPOINT is set (host:port), with NEBULA_TEST_S3_ACCESS_KEY and
// NEBULA_TEST_S3_SECRET_KEY. Absent, they skip, like the database tests.
func s3Store(t *testing.T) *artifact.S3 {
	t.Helper()
	ep := os.Getenv("NEBULA_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("NEBULA_TEST_S3_ENDPOINT is not set; skipping S3 integration test")
	}
	s, err := artifact.NewS3(artifact.S3Config{
		Endpoint: ep, Bucket: "nebula-test", Region: "us-east-1", MaxBytes: 1 << 20,
		AccessKey: os.Getenv("NEBULA_TEST_S3_ACCESS_KEY"), SecretKey: os.Getenv("NEBULA_TEST_S3_SECRET_KEY"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatalf("creating the test bucket: %v", err)
	}
	return s
}

// The two-phase upload end to end: presign, PUT bytes over plain HTTP as a client
// would, then read them back and verify.
func TestS3PresignedUploadAndVerify(t *testing.T) {
	s := s3Store(t)
	ctx := context.Background()
	data := []byte("weights uploaded through a presigned URL " + time.Now().String())
	key := artifact.KeyFor(sum(data))

	if _, err := s.Stat(ctx, key); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("before upload: %v", err)
	}
	up, err := s.PresignPut(ctx, key, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if up.Method != "PUT" || up.MaxBytes != 1<<20 || time.Until(up.ExpiresAt) < 4*time.Minute {
		t.Errorf("upload target: %+v", up)
	}
	req, _ := http.NewRequestWithContext(ctx, up.Method, up.URL, bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("presigned PUT: %d %s", resp.StatusCode, body)
	}

	info, err := s.Stat(ctx, key)
	if err != nil || info.Size != int64(len(data)) {
		t.Fatalf("stat: %+v %v", info, err)
	}
	rc, err := s.Open(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	v, err := artifact.Hash(rc, 1<<20)
	rc.Close()
	if err != nil || v.Check(sum(data), int64(len(data))) != nil {
		t.Errorf("verify: %+v %v", v, err)
	}

	// The same URL cannot be reused to write under another key: the signature
	// covers the path.
	tampered := bytes.Replace([]byte(up.URL), []byte(sum(data)[:8]), []byte("00000000"), 1)
	req, _ = http.NewRequestWithContext(ctx, "PUT", string(tampered), bytes.NewReader(data))
	resp, err = http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Error("a presigned URL with a modified key must be refused")
		}
	}
}

func TestS3OpenMissing(t *testing.T) {
	s := s3Store(t)
	if _, err := s.Open(context.Background(), artifact.KeyFor(sum([]byte("never uploaded")))); !errors.Is(err, artifact.ErrNotFound) {
		t.Errorf("missing object: %v", err)
	}
}

// The node cache over a real store.
func TestS3FetchIntoCache(t *testing.T) {
	s := s3Store(t)
	ctx := context.Background()
	data := []byte("cache me " + time.Now().String())
	key := artifact.KeyFor(sum(data))
	if err := s.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	open := func(ctx context.Context) (io.ReadCloser, error) { return s.Open(ctx, key) }
	if r := artifact.Fetch(ctx, open, dir, sum(data), int64(len(data)), 1<<20); r.Status != artifact.FetchDownloaded {
		t.Fatalf("%+v", r)
	}
	if r := artifact.Fetch(ctx, open, dir, sum(data), int64(len(data)), 1<<20); r.Status != artifact.FetchCacheHit {
		t.Fatalf("%+v", r)
	}
}
