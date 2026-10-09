package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/adityasatwar321/nebula/packages/artifact"
)

func setup(t *testing.T, data []byte) (store *artifact.Dir, env map[string]string) {
	t.Helper()
	store, err := artifact.NewDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(data)
	sum := hex.EncodeToString(s[:])
	if err := store.Put(context.Background(), artifact.KeyFor(sum), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	return store, map[string]string{
		"NEBULA_ARTIFACT_URI":        store.URI(artifact.KeyFor(sum)),
		"NEBULA_ARTIFACT_SHA256":     sum,
		"NEBULA_ARTIFACT_SIZE_BYTES": "0",
		"NEBULA_ARTIFACT_MAX_BYTES":  "1048576",
		"NEBULA_ARTIFACT_CACHE_DIR":  t.TempDir(),
	}
}

func TestPullThenHit(t *testing.T) {
	t.Parallel()
	_, env := setup(t, []byte("weights"))
	res, code := run(context.Background(), func(k string) string { return env[k] }, 2)
	if code != 0 || res.Status != artifact.FetchDownloaded {
		t.Fatalf("%d %+v", code, res)
	}
	res, code = run(context.Background(), func(k string) string { return env[k] }, 2)
	if code != 0 || res.Status != artifact.FetchCacheHit {
		t.Fatalf("second run must hit the cache: %d %+v", code, res)
	}
}

func TestWrongBytesExit3(t *testing.T) {
	t.Parallel()
	store, env := setup(t, []byte("weights"))
	// Overwrite the stored object with other bytes under the same key.
	_ = store.Put(context.Background(), artifact.KeyFor(env["NEBULA_ARTIFACT_SHA256"]), bytes.NewReader([]byte("tampered")))
	res, code := run(context.Background(), func(k string) string { return env[k] }, 3)
	if code != 3 || res.ErrorCode != "checksum_mismatch" {
		t.Fatalf("%d %+v", code, res)
	}
}

func TestMisconfiguration(t *testing.T) {
	t.Parallel()
	_, env := setup(t, []byte("weights"))
	env["NEBULA_ARTIFACT_SHA256"] = "00" + env["NEBULA_ARTIFACT_SHA256"][2:]
	if _, code := run(context.Background(), func(k string) string { return env[k] }, 1); code != 4 {
		t.Errorf("a uri that does not address the checksum must be a config error: %d", code)
	}
	if _, code := run(context.Background(), func(string) string { return "" }, 1); code != 4 {
		t.Errorf("missing env: %d", code)
	}
}
