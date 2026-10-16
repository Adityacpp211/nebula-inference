// Package netx provides test listeners that survive a Windows quirk.
//
// On Windows hosts with Hyper-V or WSL, the OS reserves port ranges inside its own
// dynamic range, and binding port 0 occasionally draws one of them and fails with
// WSAEACCES ("an attempt was made to access a socket in a way forbidden by its
// access permissions"). The failure is transient — the next bind draws another
// port — so a test that binds port 0 retries rather than flaking.
package netx

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Listen binds a loopback TCP port, retrying transient bind failures.
func Listen(t testing.TB) net.Listener {
	t.Helper()
	var err error
	for range 20 {
		var ln net.Listener
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		if err == nil {
			return ln
		}
	}
	t.Fatalf("binding a loopback port: %v", err)
	return nil
}

// NewServer is httptest.NewServer with a retrying listener.
func NewServer(t testing.TB, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	_ = srv.Listener.Close()
	srv.Listener = Listen(t)
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}
