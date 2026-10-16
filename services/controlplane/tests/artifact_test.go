package controlplane_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/artifact"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

// storeFixture is a control plane with a real S3-compatible artifact store and a
// running verifier. Skips unless NEBULA_TEST_S3_ENDPOINT is set (see
// packages/artifact/s3_integration_test.go).
func storeFixture(t *testing.T) *fixture {
	t.Helper()
	ep := os.Getenv("NEBULA_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("NEBULA_TEST_S3_ENDPOINT is not set; skipping artifact store integration test")
	}
	extra := map[string]string{
		"NEBULA_ARTIFACT_STORE":           "s3",
		"NEBULA_ARTIFACT_S3_ENDPOINT":     ep,
		"NEBULA_ARTIFACT_S3_BUCKET":       "nebula-it",
		"NEBULA_ARTIFACT_S3_ACCESS_KEY":   os.Getenv("NEBULA_TEST_S3_ACCESS_KEY"),
		"NEBULA_ARTIFACT_S3_SECRET_KEY":   os.Getenv("NEBULA_TEST_S3_SECRET_KEY"),
		"NEBULA_ARTIFACT_VERIFY_INTERVAL": "200ms",
	}
	f := newFixtureWith(t, dbtest.Migrated(t), extra)
	if s3, ok := f.API.Artifacts.(*artifact.S3); ok {
		if err := s3.EnsureBucket(dbtest.Context(t)); err != nil {
			t.Fatalf("creating the bucket: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.API.Verifier.Run(ctx)
	return f
}

type createdVersion struct {
	ID          uuid.UUID `json:"id"`
	ArtifactURI string    `json:"artifact_uri"`
	Status      string    `json:"status"`
	Upload      *struct {
		Method   string `json:"method"`
		URL      string `json:"url"`
		MaxBytes int64  `json:"max_bytes"`
	} `json:"upload"`
}

func (f *fixture) newStoreVersion(t *testing.T, format string, declared []byte, size int) createdVersion {
	t.Helper()
	var model struct {
		ID uuid.UUID `json:"id"`
	}
	if code := f.do(t, "POST", "/v1/models", map[string]any{"name": "m-" + uuid.NewString()[:8], "task": "chat"}, &model); code != 201 {
		t.Fatalf("create model: %d", code)
	}
	sum := sha256.Sum256(declared)
	var v createdVersion
	code := f.do(t, "POST", "/v1/models/"+model.ID.String()+"/versions", map[string]any{
		"version": "v1", "format": format, "runtime": map[string]string{"gguf": "llamacpp", "mock": "mock"}[format],
		"size_bytes": size, "checksum_sha256": hex.EncodeToString(sum[:]), "context_window": 512,
	}, &v)
	if code != 201 {
		t.Fatalf("create version: %d", code)
	}
	return v
}

func put(t *testing.T, url string, body []byte) {
	t.Helper()
	req, _ := http.NewRequestWithContext(dbtest.Context(t), "PUT", url, bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("upload: %d", resp.StatusCode)
	}
}

func (f *fixture) waitStatus(t *testing.T, id uuid.UUID, want string) (status string, reason *string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var v struct {
			Status        string  `json:"status"`
			FailureReason *string `json:"failure_reason"`
		}
		f.do(t, "GET", "/v1/model-versions/"+id.String(), nil, &v)
		if v.Status == want {
			return v.Status, v.FailureReason
		}
		if v.Status != "verifying" {
			return v.Status, v.FailureReason
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("version %s stayed verifying", id)
	return "", nil
}

func finalize(t *testing.T, f *fixture, v createdVersion, declared []byte) (int, apiError) {
	t.Helper()
	sum := sha256.Sum256(declared)
	var env apiError
	code := f.do(t, "POST", "/v1/model-versions/"+v.ID.String()+"/finalize",
		map[string]any{"checksum_sha256": hex.EncodeToString(sum[:])}, &env)
	return code, env
}

// The whole two-phase flow: the registry mints the location, the client uploads
// directly, finalize returns 202, and the version becomes ready only after the
// control plane has read the bytes.
func TestStoreBackedVersionIsVerifiedByReadingTheBytes(t *testing.T) {
	t.Parallel()
	f := storeFixture(t)
	data := []byte("mock weights " + uuid.NewString())
	v := f.newStoreVersion(t, "mock", data, len(data))

	sum := sha256.Sum256(data)
	if v.Upload == nil || v.Upload.Method != "PUT" || !strings.HasSuffix(v.ArtifactURI, "/sha256/"+hex.EncodeToString(sum[:])) {
		t.Fatalf("created version: %+v", v)
	}

	// Finalize before the bytes exist: refused, and the version stays uploading.
	if code, env := finalize(t, f, v, data); code != 409 || env.Error.Code != "artifact_not_uploaded" {
		t.Fatalf("finalize before upload: %d %+v", code, env)
	}

	put(t, v.Upload.URL, data)
	var resp struct {
		Status       string `json:"status"`
		Verification string `json:"verification"`
	}
	if code := f.do(t, "POST", "/v1/model-versions/"+v.ID.String()+"/finalize",
		map[string]any{"checksum_sha256": hex.EncodeToString(sum[:])}, &resp); code != 202 ||
		resp.Status != "verifying" || resp.Verification != "pending" {
		t.Fatalf("finalize: %d %+v", code, resp)
	}
	if status, reason := f.waitStatus(t, v.ID, "ready"); status != "ready" {
		t.Fatalf("status %s (%v)", status, reason)
	}

	var verification string
	if err := f.Pool.QueryRow(dbtest.Context(t), `
		SELECT after->>'verification' FROM audit_logs
		 WHERE resource_id = $1 AND action = 'model_version.verify'`, v.ID).Scan(&verification); err != nil {
		t.Fatal(err)
	}
	if verification != "computed" {
		t.Errorf("audit verification %q", verification)
	}
}

// The roadmap's Phase 5 test: bytes that hash to something other than the declared
// checksum are refused, the computed value is reported, and the version can never
// be deployed.
func TestTamperedUploadFailsVerification(t *testing.T) {
	t.Parallel()
	f := storeFixture(t)
	declared := []byte("the real weights, 32 bytes long!")
	tampered := []byte("SWAPPED weights, 32 bytes long!!")
	v := f.newStoreVersion(t, "mock", declared, len(declared))
	put(t, v.Upload.URL, tampered)

	if code, _ := finalize(t, f, v, declared); code != 202 {
		t.Fatalf("finalize: %d", code)
	}
	status, reason := f.waitStatus(t, v.ID, "failed")
	bad := sha256.Sum256(tampered)
	if status != "failed" || reason == nil || !strings.Contains(*reason, hex.EncodeToString(bad[:])) {
		t.Fatalf("status %s reason %v", status, reason)
	}

	var env apiError
	code := f.do(t, "POST", "/v1/deployments", map[string]any{
		"name": "never", "model_version_id": v.ID, "replicas": 1,
		"resources": map[string]int{"cpu_milli": 500, "memory_mib": 256},
	}, &env)
	if code < 400 || code >= 500 {
		t.Errorf("a failed version must not be deployable: %d %+v", code, env)
	}
}

func TestSizeMismatchFailsAtFinalize(t *testing.T) {
	t.Parallel()
	f := storeFixture(t)
	declared := []byte("twenty-one characters")
	v := f.newStoreVersion(t, "mock", declared, len(declared))
	put(t, v.Upload.URL, []byte("short"))
	code, env := finalize(t, f, v, declared)
	if code != 422 || env.Error.Code != "size_mismatch" {
		t.Fatalf("%d %+v", code, env)
	}
}

// A version registered as GGUF must be a GGUF: the header is parsed, not trusted.
func TestGGUFFormatIsChecked(t *testing.T) {
	t.Parallel()
	f := storeFixture(t)
	notGGUF := []byte("PK\x03\x04 this is a zip file, not a model")
	v := f.newStoreVersion(t, "gguf", notGGUF, len(notGGUF))
	put(t, v.Upload.URL, notGGUF)
	finalize(t, f, v, notGGUF)
	status, reason := f.waitStatus(t, v.ID, "failed")
	if status != "failed" || reason == nil || !strings.Contains(*reason, "not a readable GGUF") {
		t.Fatalf("%s %v", status, reason)
	}

	// A real GGUF header whose context length is smaller than declared.
	var hdr bytes.Buffer
	hdr.WriteString("GGUF")
	_ = binary.Write(&hdr, binary.LittleEndian, uint32(3))
	_ = binary.Write(&hdr, binary.LittleEndian, uint64(0))
	_ = binary.Write(&hdr, binary.LittleEndian, uint64(2))
	for _, kv := range []struct {
		k   string
		str string
		u32 uint32
	}{{"general.architecture", "llama", 0}, {"llama.context_length", "", 256}} {
		_ = binary.Write(&hdr, binary.LittleEndian, uint64(len(kv.k)))
		hdr.WriteString(kv.k)
		if kv.str != "" {
			_ = binary.Write(&hdr, binary.LittleEndian, uint32(8))
			_ = binary.Write(&hdr, binary.LittleEndian, uint64(len(kv.str)))
			hdr.WriteString(kv.str)
		} else {
			_ = binary.Write(&hdr, binary.LittleEndian, uint32(4))
			_ = binary.Write(&hdr, binary.LittleEndian, kv.u32)
		}
	}
	gguf := hdr.Bytes()
	v = f.newStoreVersion(t, "gguf", gguf, len(gguf)) // declares context_window 512
	put(t, v.Upload.URL, gguf)
	finalize(t, f, v, gguf)
	status, reason = f.waitStatus(t, v.ID, "failed")
	if status != "failed" || reason == nil || !strings.Contains(*reason, "exceeds the model's own context length 256") {
		t.Fatalf("%s %v", status, reason)
	}
}
