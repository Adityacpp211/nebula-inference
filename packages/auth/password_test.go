package auth_test

import (
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/auth"
)

const goodPassword = "correct horse battery staple"

func TestHashPasswordRoundTrip(t *testing.T) {
	t.Parallel()

	hash, err := auth.HashPassword(goodPassword)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	// PHC format, so the parameters travel with the hash and can be raised later
	// without invalidating existing hashes.
	if !strings.HasPrefix(hash, "$argon2id$v=19$") {
		t.Fatalf("hash %q is not in argon2id PHC format", hash)
	}
	if strings.Contains(hash, goodPassword) {
		t.Fatal("hash contains the password")
	}

	ok, err := auth.VerifyPassword(goodPassword, hash)
	if err != nil {
		t.Fatalf("VerifyPassword: %v", err)
	}
	if !ok {
		t.Fatal("VerifyPassword rejected the correct password")
	}
}

// A wrong password is (false, nil), never an error: conflating "wrong password"
// with "corrupt hash" means a login handler either leaks the difference to the
// caller or cannot alert on real corruption.
func TestVerifyPasswordWrongIsNotAnError(t *testing.T) {
	t.Parallel()

	hash, err := auth.HashPassword(goodPassword)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	ok, err := auth.VerifyPassword("wrong password entirely", hash)
	if err != nil {
		t.Fatalf("VerifyPassword returned an error for a wrong password: %v", err)
	}
	if ok {
		t.Fatal("VerifyPassword accepted a wrong password")
	}

	// Case and trailing whitespace must matter.
	for _, variant := range []string{
		strings.ToUpper(goodPassword),
		goodPassword + " ",
		" " + goodPassword,
		goodPassword[:len(goodPassword)-1],
	} {
		ok, err := auth.VerifyPassword(variant, hash)
		if err != nil {
			t.Fatalf("VerifyPassword(%q): %v", variant, err)
		}
		if ok {
			t.Errorf("VerifyPassword accepted %q", variant)
		}
	}
}

func TestSaltIsPerPassword(t *testing.T) {
	t.Parallel()

	first, err := auth.HashPassword(goodPassword)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	second, err := auth.HashPassword(goodPassword)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if first == second {
		t.Fatal("the same password hashed to the same value twice: the salt is not random")
	}

	// Both must still verify, which is what proves the salt travels in the hash.
	for _, h := range []string{first, second} {
		ok, err := auth.VerifyPassword(goodPassword, h)
		if err != nil || !ok {
			t.Fatalf("VerifyPassword(%q) = %v, %v", h, ok, err)
		}
	}
}

func TestPasswordLengthBounds(t *testing.T) {
	t.Parallel()

	if _, err := auth.HashPassword(strings.Repeat("a", auth.MinPasswordLen-1)); err == nil {
		t.Error("HashPassword accepted a password below the minimum length")
	}
	if _, err := auth.HashPassword(strings.Repeat("a", auth.MinPasswordLen)); err != nil {
		t.Errorf("HashPassword rejected a password at the minimum length: %v", err)
	}
	// The maximum exists so an enormous body cannot turn a login into a
	// memory-hard denial of service.
	if _, err := auth.HashPassword(strings.Repeat("a", auth.MaxPasswordLen+1)); err == nil {
		t.Error("HashPassword accepted a password above the maximum length")
	}
}

// A malformed stored hash IS an error: it means the row is corrupt or was written
// by something else, and silently reporting "wrong password" would hide that.
func TestVerifyPasswordRejectsMalformedHash(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		hash string
	}{
		{"empty", ""},
		{"not phc", "plaintext"},
		{"wrong algorithm", "$argon2i$v=19$m=19456,t=2,p=1$c2FsdHNhbHRzYWx0$aGFzaA"},
		{"truncated", "$argon2id$v=19$m=19456,t=2,p=1"},
		{"bad base64", "$argon2id$v=19$m=19456,t=2,p=1$!!!!$!!!!"},
		{"non numeric cost", "$argon2id$v=19$m=abc,t=2,p=1$c2FsdHNhbHRzYWx0$aGFzaA"},
	}
	for _, c := range cases {
		if _, err := auth.VerifyPassword(goodPassword, c.hash); err == nil {
			t.Errorf("VerifyPassword accepted a %s hash without error", c.name)
		}
	}
}

// VerifyPassword takes (password, encoded). Two strings in either order compile,
// so the protection against swapping them is that a password is not a PHC string
// and is reported as a malformed hash rather than as a silent mismatch. A silent
// false would be a login endpoint that rejects every correct password.
func TestVerifyPasswordDetectsSwappedArguments(t *testing.T) {
	t.Parallel()

	hash, err := auth.HashPassword(goodPassword)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	ok, err := auth.VerifyPassword(hash, goodPassword)
	if err == nil {
		t.Fatal("swapped arguments returned no error; a caller would see a silent mismatch")
	}
	if ok {
		t.Fatal("swapped arguments verified")
	}
}

func TestNeedsRehash(t *testing.T) {
	t.Parallel()

	hash, err := auth.HashPassword(goodPassword)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if auth.NeedsRehash(hash) {
		t.Error("a freshly written hash must not need rehashing")
	}

	// A hash written with weaker parameters must be flagged, which is how cost is
	// raised over time without a migration that cannot read plaintexts.
	weak := "$argon2id$v=19$m=4096,t=1,p=1$c2FsdHNhbHRzYWx0c2E$" +
		strings.Repeat("A", 43)
	if !auth.NeedsRehash(weak) {
		t.Error("a hash with weaker parameters must need rehashing")
	}
	if !auth.NeedsRehash("not a hash") {
		t.Error("an unreadable hash must be treated as needing a rehash")
	}
}
