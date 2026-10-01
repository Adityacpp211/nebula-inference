package artifact_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/artifact"
)

func sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestHashAndCheck(t *testing.T) {
	t.Parallel()
	data := []byte("model weights")
	v, err := artifact.Hash(bytes.NewReader(data), 100)
	if err != nil || v.SHA256 != sum(data) || v.Size != int64(len(data)) {
		t.Fatalf("%+v %v", v, err)
	}
	if err := v.Check(strings.ToUpper(sum(data)), int64(len(data))); err != nil {
		t.Errorf("case-insensitive match: %v", err)
	}
	if err := v.Check(sum([]byte("other")), 0); !errors.Is(err, artifact.ErrChecksumMismatch) {
		t.Errorf("mismatch: %v", err)
	}
	if err := v.Check(sum(data), 99); !errors.Is(err, artifact.ErrSizeMismatch) {
		t.Errorf("size: %v", err)
	}
	if _, err := artifact.Hash(bytes.NewReader(data), 5); !errors.Is(err, artifact.ErrTooLarge) {
		t.Errorf("limit: %v", err)
	}
}

func TestKeysAndURIs(t *testing.T) {
	t.Parallel()
	h := sum([]byte("x"))
	if !artifact.ValidKey(artifact.KeyFor(strings.ToUpper(h))) {
		t.Error("KeyFor must lower-case")
	}
	for _, bad := range []string{"sha256/../../etc/passwd", "sha256/abc", "other/" + h, ""} {
		if artifact.ValidKey(bad) {
			t.Errorf("%q must not be a valid key", bad)
		}
	}
	scheme, bucket, key, err := artifact.ParseURI("s3://nebula-models/sha256/" + h)
	if err != nil || scheme != "s3" || bucket != "nebula-models" || key != "sha256/"+h {
		t.Errorf("%s %s %s %v", scheme, bucket, key, err)
	}
	if _, _, key, err := artifact.ParseURI("file:///var/models/sha256/" + h); err != nil || key != "sha256/"+h {
		t.Errorf("file uri: %s %v", key, err)
	}
	for _, bad := range []string{"s3://bucket/models/x.gguf", "https://example.com/x", "nope"} {
		if _, _, _, err := artifact.ParseURI(bad); err == nil {
			t.Errorf("%q must be refused", bad)
		}
	}
}

func TestDirStore(t *testing.T) {
	t.Parallel()
	d, err := artifact.NewDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("weights")
	key := artifact.KeyFor(sum(data))
	if _, err := d.Stat(context.Background(), key); !errors.Is(err, artifact.ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
	if err := d.Put(context.Background(), key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	info, err := d.Stat(context.Background(), key)
	if err != nil || info.Size != int64(len(data)) {
		t.Errorf("%+v %v", info, err)
	}
	rc, err := d.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "weights" {
		t.Errorf("read %q", got)
	}
	if _, err := d.PresignPut(context.Background(), key, 0); !errors.Is(err, artifact.ErrPresignUnsupported) {
		t.Errorf("presign: %v", err)
	}
	if err := d.Put(context.Background(), "../escape", bytes.NewReader(data)); err == nil {
		t.Error("a non-content-addressed key must be refused")
	}
}

func opener(data []byte, calls *int) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) {
		*calls++
		return io.NopCloser(bytes.NewReader(data)), nil
	}
}

func TestFetchDownloadsThenHits(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	data := []byte("the model")
	calls := 0
	r := artifact.Fetch(context.Background(), opener(data, &calls), dir, sum(data), int64(len(data)), 1<<20)
	if r.Status != artifact.FetchDownloaded || calls != 1 {
		t.Fatalf("first fetch: %+v calls=%d", r, calls)
	}
	got, _ := os.ReadFile(r.Path)
	if string(got) != "the model" {
		t.Errorf("cached bytes %q", got)
	}
	r = artifact.Fetch(context.Background(), opener(data, &calls), dir, sum(data), int64(len(data)), 1<<20)
	if r.Status != artifact.FetchCacheHit || calls != 1 {
		t.Errorf("second fetch must hit without downloading: %+v calls=%d", r, calls)
	}
}

// Bytes that do not hash to the declared checksum never enter the cache.
func TestFetchRefusesWrongBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	calls := 0
	want := sum([]byte("expected"))
	r := artifact.Fetch(context.Background(), opener([]byte("tampered"), &calls), dir, want, 0, 1<<20)
	if r.Status != artifact.FetchFailed || r.ErrorCode != "checksum_mismatch" {
		t.Fatalf("%+v", r)
	}
	if _, err := os.Stat(filepath.Join(dir, "sha256", want)); !os.IsNotExist(err) {
		t.Error("tampered bytes were left in the cache")
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "sha256"))
	if len(entries) != 0 {
		t.Errorf("partial files left behind: %v", entries)
	}
}

// A corrupted cache entry is detected and replaced, not trusted.
func TestFetchReplacesACorruptCacheEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	data := []byte("good bytes")
	calls := 0
	artifact.Fetch(context.Background(), opener(data, &calls), dir, sum(data), 0, 1<<20)
	path := filepath.Join(dir, "sha256", sum(data))
	if err := os.WriteFile(path, []byte("rotten bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := artifact.Fetch(context.Background(), opener(data, &calls), dir, sum(data), 0, 1<<20)
	if r.Status != artifact.FetchDownloaded || calls != 2 {
		t.Fatalf("a corrupt entry must be re-downloaded: %+v calls=%d", r, calls)
	}
	got, _ := os.ReadFile(path)
	if string(got) != "good bytes" {
		t.Errorf("cache holds %q", got)
	}
}

func TestFetchNotFound(t *testing.T) {
	t.Parallel()
	r := artifact.Fetch(context.Background(), func(context.Context) (io.ReadCloser, error) {
		return nil, artifact.ErrNotFound
	}, t.TempDir(), sum([]byte("x")), 0, 10)
	if r.Status != artifact.FetchFailed || r.ErrorCode != "not_found" {
		t.Errorf("%+v", r)
	}
}

// ---------------------------------------------------------------------------
// GGUF
// ---------------------------------------------------------------------------

// ggufBuilder writes a GGUF header the way the specification lays it out.
type ggufBuilder struct {
	buf bytes.Buffer
	kvs int
	kv  bytes.Buffer
}

func (g *ggufBuilder) str(w *bytes.Buffer, s string) {
	_ = binary.Write(w, binary.LittleEndian, uint64(len(s)))
	w.WriteString(s)
}

func (g *ggufBuilder) kvString(k, v string) {
	g.kvs++
	g.str(&g.kv, k)
	_ = binary.Write(&g.kv, binary.LittleEndian, uint32(8))
	g.str(&g.kv, v)
}

func (g *ggufBuilder) kvU32(k string, v uint32) {
	g.kvs++
	g.str(&g.kv, k)
	_ = binary.Write(&g.kv, binary.LittleEndian, uint32(4))
	_ = binary.Write(&g.kv, binary.LittleEndian, v)
}

func (g *ggufBuilder) kvStringArray(k string, vs []string) {
	g.kvs++
	g.str(&g.kv, k)
	_ = binary.Write(&g.kv, binary.LittleEndian, uint32(9))
	_ = binary.Write(&g.kv, binary.LittleEndian, uint32(8))
	_ = binary.Write(&g.kv, binary.LittleEndian, uint64(len(vs)))
	for _, v := range vs {
		g.str(&g.kv, v)
	}
}

func (g *ggufBuilder) bytes(tensors uint64) []byte {
	var out bytes.Buffer
	out.WriteString("GGUF")
	_ = binary.Write(&out, binary.LittleEndian, uint32(3))
	_ = binary.Write(&out, binary.LittleEndian, tensors)
	_ = binary.Write(&out, binary.LittleEndian, uint64(g.kvs))
	out.Write(g.kv.Bytes())
	out.WriteString("...tensor data is never read...")
	return out.Bytes()
}

func TestParseGGUF(t *testing.T) {
	t.Parallel()
	var b ggufBuilder
	b.kvString("general.architecture", "llama")
	b.kvString("general.name", "nebula-tiny")
	b.kvU32("general.file_type", 15)
	b.kvU32("llama.context_length", 4096)
	vocab := make([]string, 300)
	for i := range vocab {
		vocab[i] = "tok"
	}
	b.kvStringArray("tokenizer.ggml.tokens", vocab)
	b.kvString("tokenizer.chat_template", "{% for m in messages %}{{ m.content }}{% endfor %}")

	g, err := artifact.ParseGGUF(bytes.NewReader(b.bytes(20)))
	if err != nil {
		t.Fatal(err)
	}
	if g.Version != 3 || g.TensorCount != 20 || g.Architecture != "llama" || g.Name != "nebula-tiny" ||
		g.ContextLength != 4096 || g.Quantization != "Q4_K_M" || *g.FileType != 15 ||
		!strings.Contains(g.ChatTemplate, "messages") {
		t.Errorf("%+v", g)
	}
}

func TestParseGGUFRefusesGarbage(t *testing.T) {
	t.Parallel()
	if _, err := artifact.ParseGGUF(strings.NewReader("PK\x03\x04 a zip file")); !errors.Is(err, artifact.ErrNotGGUF) {
		t.Errorf("magic: %v", err)
	}
	var b ggufBuilder
	b.kvString("general.architecture", "llama")
	full := b.bytes(1)
	if _, err := artifact.ParseGGUF(bytes.NewReader(full[:30])); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Errorf("truncation: %v", err)
	}
	// A header claiming a 1 TiB string must be refused before any allocation.
	var hostile bytes.Buffer
	hostile.WriteString("GGUF")
	_ = binary.Write(&hostile, binary.LittleEndian, uint32(3))
	_ = binary.Write(&hostile, binary.LittleEndian, uint64(0))
	_ = binary.Write(&hostile, binary.LittleEndian, uint64(1))
	_ = binary.Write(&hostile, binary.LittleEndian, uint64(1<<40))
	if _, err := artifact.ParseGGUF(&hostile); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Errorf("hostile length: %v", err)
	}
}
