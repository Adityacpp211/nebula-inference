// Package gwtest holds test helpers shared by the gateway's packages.
package gwtest

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// Miniredis starts an in-memory Redis, retrying the transient Windows bind
// failure described in packages/testsupport/netx.
func Miniredis(t testing.TB) *miniredis.Miniredis {
	t.Helper()
	var err error
	for range 20 {
		m := miniredis.NewMiniRedis()
		if err = m.Start(); err == nil {
			t.Cleanup(m.Close)
			return m
		}
	}
	t.Fatalf("starting miniredis: %v", err)
	return nil
}
