// Command nebula-artifact-puller makes a model artifact available in the node's
// content-addressed cache before the worker starts. It runs as an init container.
//
// It reads the artifact's coordinates from its environment, and the store's
// credentials from a Secret the controller references but never reads:
//
//	NEBULA_ARTIFACT_URI          s3://bucket/sha256/<hex> or file:///dir/sha256/<hex>
//	NEBULA_ARTIFACT_SHA256       the checksum the registry verified
//	NEBULA_ARTIFACT_SIZE_BYTES   the size the registry verified
//	NEBULA_ARTIFACT_MAX_BYTES    refuse to read more than this
//	NEBULA_ARTIFACT_CACHE_DIR    the node cache mount
//	NEBULA_ARTIFACT_S3_ENDPOINT, _ACCESS_KEY, _SECRET_KEY, _REGION, _USE_TLS
//
// A cache hit whose bytes still verify costs no download. Anything else is
// downloaded to a temporary file, hashed while it streams, and renamed into the
// cache only if the checksum matches, so the cache never holds bytes nobody
// verified. The result — cache hit, downloaded, or failed with a code — is written
// as JSON to the termination message, where the controller reads it and turns a
// failure into a named deployment condition instead of an opaque crash loop.
//
// Exit codes: 0 available, 3 the artifact is wrong (checksum, size, too large,
// missing), 4 misconfiguration, 5 transient failures exhausted retries.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/adityasatwar321/nebula/packages/artifact"
)

func main() {
	resultPath := flag.String("result", "/dev/termination-log", "where to write the JSON result")
	retries := flag.Int("retries", 4, "attempts for transient failures")
	flag.Parse()

	os.Exit(pull(*resultPath, *retries))
}

// pull runs the fetch and reports it; separate from main so its deferred cleanup
// runs before the process exits.
func pull(resultPath string, retries int) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	res, code := run(ctx, os.Getenv, retries)
	b, _ := json.Marshal(res)
	fmt.Println(string(b))
	// The kubelet reads the termination message as root; owner-only is enough.
	if err := os.WriteFile(resultPath, b, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintln(os.Stderr, "writing result:", err)
	}
	return code
}

func run(ctx context.Context, getenv func(string) string, retries int) (result artifact.FetchResult, exitCode int) {
	fail := func(code int, errCode string, err error) (artifact.FetchResult, int) {
		return artifact.FetchResult{Status: artifact.FetchFailed, ErrorCode: errCode, Error: err.Error(),
			SHA256: getenv("NEBULA_ARTIFACT_SHA256")}, code
	}
	uri := getenv("NEBULA_ARTIFACT_URI")
	sum := getenv("NEBULA_ARTIFACT_SHA256")
	dir := getenv("NEBULA_ARTIFACT_CACHE_DIR")
	size, err1 := strconv.ParseInt(getenv("NEBULA_ARTIFACT_SIZE_BYTES"), 10, 64)
	maxBytes, err2 := strconv.ParseInt(getenv("NEBULA_ARTIFACT_MAX_BYTES"), 10, 64)
	if uri == "" || sum == "" || dir == "" || err1 != nil || err2 != nil {
		return fail(4, "misconfigured", errors.New("NEBULA_ARTIFACT_URI, _SHA256, _SIZE_BYTES, _MAX_BYTES and _CACHE_DIR are required"))
	}

	scheme, bucket, key, err := artifact.ParseURI(uri)
	if err != nil {
		return fail(4, "misconfigured", err)
	}
	if key != artifact.KeyFor(sum) {
		// The URI and the checksum must name the same content; a mismatch means the
		// controller was handed an inconsistent version row.
		return fail(4, "misconfigured", fmt.Errorf("artifact uri %s does not address checksum %s", uri, sum))
	}

	var store artifact.Store
	switch scheme {
	case "s3":
		tls, _ := strconv.ParseBool(getenv("NEBULA_ARTIFACT_S3_USE_TLS"))
		store, err = artifact.NewS3(artifact.S3Config{
			Endpoint: getenv("NEBULA_ARTIFACT_S3_ENDPOINT"), Bucket: bucket,
			AccessKey: getenv("NEBULA_ARTIFACT_S3_ACCESS_KEY"), SecretKey: getenv("NEBULA_ARTIFACT_S3_SECRET_KEY"),
			Region: or(getenv("NEBULA_ARTIFACT_S3_REGION"), "us-east-1"), UseTLS: tls, MaxBytes: maxBytes,
		})
	case "file":
		store, err = artifact.NewDir(filepath.FromSlash(bucket))
	}
	if err != nil {
		return fail(4, "misconfigured", err)
	}

	open := func(ctx context.Context) (io.ReadCloser, error) { return store.Open(ctx, key) }
	var res artifact.FetchResult
	backoff := time.Second
	for attempt := 1; attempt <= retries; attempt++ {
		res = artifact.Fetch(ctx, open, dir, sum, size, maxBytes)
		switch {
		case res.Status != artifact.FetchFailed:
			return res, 0
		case res.ErrorCode != "io":
			// The artifact itself is wrong: retrying downloads the same wrong bytes.
			return res, 3
		}
		if attempt < retries {
			select {
			case <-ctx.Done():
				return res, 5
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}
	return res, 5
}

func or(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
