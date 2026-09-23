package auth_test

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/auth"
)

func newHasher(t *testing.T) *auth.Hasher {
	t.Helper()
	pepper, err := auth.NewPepper()
	if err != nil {
		t.Fatalf("generating pepper: %v", err)
	}
	h, err := auth.NewHasher(pepper)
	if err != nil {
		t.Fatalf("building hasher: %v", err)
	}
	return h
}

func TestGeneratedKeyShape(t *testing.T) {
	t.Parallel()
	h := newHasher(t)

	seenPrefix := map[string]bool{}
	seenKey := map[string]bool{}

	for i := 0; i < 200; i++ {
		k, err := h.Generate()
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		if !strings.HasPrefix(k.Plaintext, auth.KeyPrefix) {
			t.Fatalf("key %q does not start with %q", k.Plaintext, auth.KeyPrefix)
		}
		if len(k.Prefix) != auth.PrefixLen {
			t.Fatalf("prefix %q is %d characters, want %d", k.Prefix, len(k.Prefix), auth.PrefixLen)
		}
		if !strings.HasPrefix(k.Plaintext, k.Prefix) {
			t.Fatalf("prefix %q is not a prefix of the key", k.Prefix)
		}
		if len(k.Hash) != auth.HashLen {
			t.Fatalf("hash is %d bytes, want %d", len(k.Hash), auth.HashLen)
		}
		// The hash must not be derivable from the visible prefix alone, or the
		// indexed column would be enough to forge a key.
		if strings.Contains(string(k.Hash), k.Prefix) {
			t.Fatal("hash contains the plaintext prefix")
		}

		if seenPrefix[k.Prefix] {
			// A collision in 200 draws would mean far too little entropy in the
			// prefix, which is also a unique column: collisions become 500s.
			t.Fatalf("duplicate prefix %q within 200 generated keys", k.Prefix)
		}
		if seenKey[k.Plaintext] {
			t.Fatal("duplicate key generated")
		}
		seenPrefix[k.Prefix] = true
		seenKey[k.Plaintext] = true
	}
}

func TestVerifyAcceptsOnlyTheRightKey(t *testing.T) {
	t.Parallel()
	h := newHasher(t)

	k, err := h.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	other, err := h.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if !h.Verify(k.Plaintext, k.Hash) {
		t.Fatal("Verify rejected the key it generated")
	}
	if h.Verify(other.Plaintext, k.Hash) {
		t.Fatal("Verify accepted a different key against this hash")
	}
	// The visible prefix must not authenticate: this is the property that makes it
	// safe to log and to store unhashed.
	if h.Verify(k.Prefix, k.Hash) {
		t.Fatal("Verify accepted the prefix alone")
	}
	if h.Verify(k.Plaintext[:len(k.Plaintext)-1], k.Hash) {
		t.Fatal("Verify accepted a truncated key")
	}
	if h.Verify("", k.Hash) {
		t.Fatal("Verify accepted an empty key")
	}
	if h.Verify(k.Plaintext, nil) {
		t.Fatal("Verify accepted a nil stored hash")
	}
	if h.Verify(k.Plaintext, make([]byte, auth.HashLen)) {
		t.Fatal("Verify accepted a zeroed stored hash")
	}
}

// The pepper is the whole reason a database dump is not enough to verify keys, so a
// different pepper must not verify the same plaintext.
func TestPepperSeparatesHashers(t *testing.T) {
	t.Parallel()

	a := newHasher(t)
	b := newHasher(t)

	k, err := a.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if b.Verify(k.Plaintext, k.Hash) {
		t.Fatal("a hasher with a different pepper verified the key")
	}
}

func TestNewHasherRejectsWeakPepper(t *testing.T) {
	t.Parallel()

	cases := []string{"", "short", strings.Repeat("a", 31)}
	for _, pepper := range cases {
		if _, err := auth.NewHasher(pepper); err == nil {
			t.Errorf("NewHasher(%q) succeeded; a short pepper must be refused", pepper)
		}
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := auth.NewHasher(base64.StdEncoding.EncodeToString(raw)); err != nil {
		t.Errorf("NewHasher rejected a 32-byte pepper: %v", err)
	}
}

// ParsePrefix is the gate that keeps malformed tokens from costing a database
// round-trip, so it has to reject everything that cannot possibly be a key.
func TestParsePrefix(t *testing.T) {
	t.Parallel()
	h := newHasher(t)

	k, err := h.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	got, err := auth.ParsePrefix(k.Plaintext)
	if err != nil {
		t.Fatalf("ParsePrefix(valid key): %v", err)
	}
	if got != k.Prefix {
		t.Fatalf("ParsePrefix = %q, want %q", got, k.Prefix)
	}

	bad := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"prefix only", auth.KeyPrefix},
		{"wrong scheme", "sk-" + strings.Repeat("a", 50)},
		{"too short", auth.KeyPrefix + "abc"},
		{"too long", k.Plaintext + "extra"},
		{"non base62", auth.KeyPrefix + strings.Repeat("-", 50)},
		{"whitespace", auth.KeyPrefix + " " + strings.Repeat("a", 49)},
		{"leading space", " " + k.Plaintext},
	}
	for _, c := range bad {
		if _, err := auth.ParsePrefix(c.value); err == nil {
			t.Errorf("ParsePrefix accepted %s", c.name)
		}
	}
}
