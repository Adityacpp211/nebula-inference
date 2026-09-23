package api

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db/models"
)

// A fake clock, because a TTL test that sleeps is a slow test that fails on a busy
// machine.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) advance(d time.Duration) { c.now = c.now.Add(d) }

func newEntry(prefix string) cacheEntry {
	id := uuid.New()
	return cacheEntry{
		identity: auth.Identity{OrgID: uuid.New(), ActorID: id, ActorLabel: prefix},
		hash:     []byte("a-stored-hash-for-" + prefix),
		key:      models.APIKey{ID: id, Prefix: prefix},
	}
}

func TestKeyCacheExpires(t *testing.T) {
	t.Parallel()

	c := &clock{now: time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)}
	cache := newKeyCache(16, c.Now)

	cache.put("nbk_aaaaaaa", newEntry("nbk_aaaaaaa"), 30*time.Second)

	if _, ok := cache.get("nbk_aaaaaaa"); !ok {
		t.Fatal("a freshly cached entry was not returned")
	}

	c.advance(29 * time.Second)
	if _, ok := cache.get("nbk_aaaaaaa"); !ok {
		t.Fatal("an entry expired before its TTL")
	}

	// Exactly at the TTL the entry must be gone: the TTL is the whole revocation
	// window until Phase 4 publishes invalidations, so it must not be generous.
	c.advance(time.Second)
	if _, ok := cache.get("nbk_aaaaaaa"); ok {
		t.Fatal("an entry survived its TTL")
	}
}

func TestKeyCacheIsBounded(t *testing.T) {
	t.Parallel()

	c := &clock{now: time.Now()}
	const capacity = 4
	cache := newKeyCache(capacity, c.Now)

	for i := 0; i < capacity*10; i++ {
		prefix := "nbk_" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + "aaaaa"
		cache.put(prefix, newEntry(prefix), time.Minute)
	}

	cache.mu.Lock()
	size := len(cache.entries)
	cache.mu.Unlock()

	// The bound is what stops a flood of distinct prefixes from growing the process
	// without limit, which is the cheapest denial of service against a key cache.
	if size > capacity {
		t.Errorf("cache holds %d entries, above its maximum of %d", size, capacity)
	}
	if size == 0 {
		t.Error("eviction emptied the cache entirely")
	}
}

// Eviction must prefer expired entries: throwing out a live one while a dead one
// sits there turns every request for the live key into a database round-trip.
func TestKeyCacheEvictsExpiredFirst(t *testing.T) {
	t.Parallel()

	c := &clock{now: time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)}
	cache := newKeyCache(2, c.Now)

	cache.put("nbk_expired", newEntry("nbk_expired"), time.Second)
	c.advance(2 * time.Second)
	cache.put("nbk_live001", newEntry("nbk_live001"), time.Minute)

	// The cache is now full (one dead, one live). Inserting must drop the dead one.
	cache.put("nbk_live002", newEntry("nbk_live002"), time.Minute)

	if _, ok := cache.get("nbk_live001"); !ok {
		t.Error("the live entry was evicted while an expired one was present")
	}
	if _, ok := cache.get("nbk_live002"); !ok {
		t.Error("the newly inserted entry is missing")
	}
	if _, ok := cache.get("nbk_expired"); ok {
		t.Error("the expired entry is still being served")
	}
}

func TestKeyCacheForgetIsImmediate(t *testing.T) {
	t.Parallel()

	c := &clock{now: time.Now()}
	cache := newKeyCache(8, c.Now)

	cache.put("nbk_revoked", newEntry("nbk_revoked"), time.Hour)
	cache.forget("nbk_revoked")

	// Revocation must take effect in this process at once rather than after the TTL:
	// an operator who revokes a leaked key expects it to stop working now.
	if _, ok := cache.get("nbk_revoked"); ok {
		t.Error("a forgotten entry is still cached")
	}
	// Forgetting something absent must be a no-op, so a revoke of an already-revoked
	// key cannot panic.
	cache.forget("nbk_absent0")
}

func TestKeyCacheZeroTTLDoesNotCache(t *testing.T) {
	t.Parallel()

	c := &clock{now: time.Now()}
	cache := newKeyCache(8, c.Now)

	cache.put("nbk_nottl00", newEntry("nbk_nottl00"), 0)
	if _, ok := cache.get("nbk_nottl00"); ok {
		t.Error("an entry with a zero TTL was cached; configuration that disables the cache must disable it")
	}
}

// The cache stores the hash so verification still runs on a hit. If it stored only
// the identity, a correct prefix with a wrong secret would authenticate for the
// lifetime of the entry — which is the one bug a key cache must not have.
func TestKeyCacheRetainsTheStoredHash(t *testing.T) {
	t.Parallel()

	c := &clock{now: time.Now()}
	cache := newKeyCache(8, c.Now)

	entry := newEntry("nbk_hashed0")
	cache.put("nbk_hashed0", entry, time.Minute)

	got, ok := cache.get("nbk_hashed0")
	if !ok {
		t.Fatal("entry missing")
	}
	if len(got.hash) == 0 {
		t.Fatal("the cached entry carries no hash, so a hit could not be verified")
	}
	if !bytes.Equal(got.hash, entry.hash) {
		t.Errorf("cached hash = %q, want %q", got.hash, entry.hash)
	}
	if got.key.ID != entry.key.ID {
		t.Error("the cached entry lost the key row needed to re-check revocation and expiry")
	}
}

// usable() decides revocation and expiry on every request, including cache hits, so
// a revoked key cannot keep working just because it was cached before revocation.
func TestUsableRejectsRevokedAndExpiredKeys(t *testing.T) {
	t.Parallel()

	now := time.Now()
	past := now.Add(-time.Minute)
	future := now.Add(time.Hour)

	if err := usable(models.APIKey{}); err != nil {
		t.Errorf("a plain key was rejected: %v", err)
	}
	if err := usable(models.APIKey{ExpiresAt: &future}); err != nil {
		t.Errorf("a key expiring in the future was rejected: %v", err)
	}
	if err := usable(models.APIKey{RevokedAt: &past}); err == nil {
		t.Error("a revoked key was accepted")
	}
	if err := usable(models.APIKey{ExpiresAt: &past}); err == nil {
		t.Error("an expired key was accepted")
	}
	// Revocation must win over a valid expiry, so the reported code is the real
	// reason rather than whichever check ran first.
	err := usable(models.APIKey{RevokedAt: &past, ExpiresAt: &future})
	if err == nil {
		t.Fatal("a revoked key with a future expiry was accepted")
	}
}
