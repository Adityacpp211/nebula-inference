// Package auth implements NEBULA's credential handling: API key generation and
// verification, password hashing, and the scope and role model.
//
// The design decisions here are recorded in ADR-0011. The one that most often
// surprises reviewers: API keys are hashed with HMAC-SHA256 and a server-side
// pepper rather than a slow KDF. A key is 256 bits of CSPRNG output, so brute
// force is not the threat model, and verification runs on every inference request.
// Argon2id is used for user passwords, where the secret is low-entropy and
// verification happens once per login.
package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// Key format constants.
const (
	// KeyPrefix marks a NEBULA API key. Chosen to be greppable in logs and in
	// secret scanners.
	KeyPrefix = "nbk_"
	// prefixRandomLen is how many random characters follow KeyPrefix in the
	// displayable prefix. Prefix + this is what the database indexes.
	prefixRandomLen = 7
	// PrefixLen is the total length of the displayable prefix, matching the
	// char(11) column and the ck_api_keys__prefix_format constraint.
	PrefixLen = len(KeyPrefix) + prefixRandomLen
	// secretRandomLen is how many characters of secret follow the prefix. 43
	// base62 characters carry ~256 bits.
	secretRandomLen = 43
	// HashLen is the length of the stored HMAC, matching ck_api_keys__hash_length.
	HashLen = sha256.Size
)

// alphabet is base62. Chosen over base64 so a key survives being pasted into a
// URL, a shell, a YAML file or an environment variable without escaping.
const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// Errors returned by this package.
var (
	// ErrMalformedKey means the presented string is not shaped like a NEBULA key.
	// Returned without touching the database, so a malformed credential costs
	// nothing.
	ErrMalformedKey = errors.New("malformed API key")
	// ErrPepperMissing means the server has no pepper configured. Verification
	// fails closed rather than silently degrading to an unpeppered hash.
	ErrPepperMissing = errors.New("API key pepper is not configured")
)

// GeneratedKey is a freshly minted credential.
//
// Plaintext exists only in this struct, is returned to the caller exactly once,
// and is never stored. Hash is what goes in the database.
type GeneratedKey struct {
	// Plaintext is the full key: nbk_ + 50 base62 characters.
	Plaintext string
	// Prefix is the first 11 characters. Safe to display, log, and audit; the
	// database indexes it so lookup is O(1).
	Prefix string
	// Hash is HMAC-SHA256(pepper, plaintext).
	Hash []byte
}

// Hasher verifies and generates keys under one pepper.
//
// The pepper lives only in the application's memory, mounted from a Secret. A
// database dump alone therefore does not yield verifiable hashes, which is the
// whole reason for peppering rather than plain hashing.
type Hasher struct {
	pepper []byte
}

// NewHasher returns a Hasher. It refuses an empty or obviously weak pepper: a
// default pepper is worse than none, because it looks like protection.
func NewHasher(pepper string) (*Hasher, error) {
	if pepper == "" {
		return nil, ErrPepperMissing
	}
	if len(pepper) < 32 {
		return nil, fmt.Errorf("API key pepper must be at least 32 bytes, got %d", len(pepper))
	}
	return &Hasher{pepper: []byte(pepper)}, nil
}

// Generate mints a new API key.
func (h *Hasher) Generate() (GeneratedKey, error) {
	suffix, err := randomString(prefixRandomLen)
	if err != nil {
		return GeneratedKey{}, err
	}
	secret, err := randomString(secretRandomLen)
	if err != nil {
		return GeneratedKey{}, err
	}

	prefix := KeyPrefix + suffix
	plaintext := prefix + secret

	return GeneratedKey{
		Plaintext: plaintext,
		Prefix:    prefix,
		Hash:      h.hash(plaintext),
	}, nil
}

// ParsePrefix extracts the lookup prefix from a presented key.
//
// It validates the shape completely before returning, so an attacker cannot use
// malformed input to probe for timing differences in the database lookup.
func ParsePrefix(presented string) (string, error) {
	if len(presented) != PrefixLen+secretRandomLen {
		return "", ErrMalformedKey
	}
	if !strings.HasPrefix(presented, KeyPrefix) {
		return "", ErrMalformedKey
	}
	for i := len(KeyPrefix); i < len(presented); i++ {
		if !strings.ContainsRune(alphabet, rune(presented[i])) {
			return "", ErrMalformedKey
		}
	}
	return presented[:PrefixLen], nil
}

// Verify reports whether the presented key matches the stored hash.
//
// The comparison is constant-time. A length mismatch short-circuits, which leaks
// only the length of a stored hash — a fixed 32 bytes for every key.
func (h *Hasher) Verify(presented string, storedHash []byte) bool {
	if len(storedHash) != HashLen {
		return false
	}
	computed := h.hash(presented)
	return subtle.ConstantTimeCompare(computed, storedHash) == 1
}

// hash computes the stored form of a key.
func (h *Hasher) hash(plaintext string) []byte {
	mac := hmac.New(sha256.New, h.pepper)
	mac.Write([]byte(plaintext))
	return mac.Sum(nil)
}

// randomString returns n characters drawn uniformly from the base62 alphabet.
//
// It uses rejection-free modular reduction over a uniformly random big.Int per
// character rather than byte % 62, which would bias the first four characters of
// the alphabet.
func randomString(n int) (string, error) {
	bound := big.NewInt(int64(len(alphabet)))
	b := make([]byte, n)
	for i := range n {
		idx, err := rand.Int(rand.Reader, bound)
		if err != nil {
			return "", fmt.Errorf("generating random key material: %w", err)
		}
		b[i] = alphabet[idx.Int64()]
	}
	return string(b), nil
}

// NewPepper generates a pepper suitable for NEBULA_AUTH_KEY_PEPPER. Used by the
// development seeder and by operators bootstrapping an installation.
func NewPepper() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating pepper: %w", err)
	}
	return base64.RawStdEncoding.EncodeToString(b), nil
}
