package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters.
//
// These follow the OWASP password-storage guidance: 19 MiB of memory, two
// iterations, parallelism matched to available cores. Unlike an API key, a
// password is low-entropy and attacker-guessable, so the cost here is the point —
// it is paid once per login, not once per request.
const (
	argonTime    uint32 = 2
	argonMemory  uint32 = 19 * 1024 // KiB
	argonKeyLen  uint32 = 32
	argonSaltLen        = 16
)

// Password policy. Deliberately a length floor rather than a character-class rule:
// composition rules push people towards "Passw0rd!" while adding little entropy.
const (
	// MinPasswordLen is the shortest accepted password.
	MinPasswordLen = 12
	// MaxPasswordLen bounds the input so a very long password cannot be used to
	// make the hash function a denial-of-service vector.
	MaxPasswordLen = 1024
)

// Password errors.
var (
	// ErrPasswordTooShort means the password is below MinPasswordLen.
	ErrPasswordTooShort = fmt.Errorf("password must be at least %d characters", MinPasswordLen)
	// ErrPasswordTooLong means the password is above MaxPasswordLen.
	ErrPasswordTooLong = fmt.Errorf("password must be at most %d characters", MaxPasswordLen)
	// ErrBadPasswordHash means the stored hash is not in the expected format.
	ErrBadPasswordHash = errors.New("stored password hash is malformed")
	// ErrUnsupportedHashVersion means the hash was produced by a newer format.
	ErrUnsupportedHashVersion = errors.New("stored password hash uses an unsupported argon2 version")
)

// HashPassword returns the PHC-format argon2id hash of a password.
//
// The encoded form carries its own parameters, so raising the cost later does not
// invalidate existing hashes: old ones keep verifying with the parameters they
// were created with, and can be upgraded on next login.
func HashPassword(password string) (string, error) {
	if len(password) < MinPasswordLen {
		return "", ErrPasswordTooShort
	}
	if len(password) > MaxPasswordLen {
		return "", ErrPasswordTooLong
	}

	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generating password salt: %w", err)
	}

	// Bounded to [1, 4] before the conversion, so narrowing to uint8 cannot wrap.
	// argon2's parallelism parameter is a uint8, and more than 4 lanes buys nothing
	// on the hardware this runs on.
	threads := uint8(max(1, min(runtime.NumCPU(), 4))) // #nosec G115 -- bounded above
	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, threads, argonKeyLen)

	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemory, argonTime, threads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key),
	), nil
}

// VerifyPassword reports whether the password matches the encoded hash.
//
// It returns an error only when the stored hash is unusable; a simple mismatch is
// (false, nil), so a caller cannot accidentally treat "wrong password" as a
// server error and return 500 instead of 401.
func VerifyPassword(password, encoded string) (bool, error) {
	parts := strings.Split(encoded, "$")
	// ["", "argon2id", "v=19", "m=...,t=...,p=...", salt, key]
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return false, ErrBadPasswordHash
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, ErrBadPasswordHash
	}
	if version != argon2.Version {
		return false, ErrUnsupportedHashVersion
	}

	var memory, time uint32
	var threads uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &time, &threads); err != nil {
		return false, ErrBadPasswordHash
	}
	if memory == 0 || time == 0 || threads == 0 {
		return false, ErrBadPasswordHash
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, ErrBadPasswordHash
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, ErrBadPasswordHash
	}
	if len(salt) == 0 || len(want) == 0 {
		return false, ErrBadPasswordHash
	}

	// The key length comes from the stored hash, which the checks above have already
	// established is non-empty; argon2 takes it as a uint32 and a stored hash is
	// bounded by the column, so the conversion cannot wrap.
	keyLen := uint32(len(want)) // #nosec G115 -- len of a decoded, length-checked hash
	got := argon2.IDKey([]byte(password), salt, time, memory, threads, keyLen)
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

// NeedsRehash reports whether an existing hash was made with weaker parameters
// than the current policy, so it can be upgraded transparently on next login.
func NeedsRehash(encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 {
		return true
	}
	var memory, time uint32
	var threads uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &time, &threads); err != nil {
		return true
	}
	return memory < argonMemory || time < argonTime
}
