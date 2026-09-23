// Package api embeds the OpenAPI specification and serves it.
//
// The spec is embedded rather than read from disk so the binary and its contract
// cannot be separated: a container that serves /openapi.yaml is serving the spec it
// was built with, not whatever a volume mount happens to contain.
//
// ADR-0018 commits to generating the Go server interfaces, CLI client and dashboard
// types from this file. TODO(NEB-131) tracks the generator; until then a drift test
// asserts that every mounted route appears here and every documented path is
// mounted, in both directions.
package api

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

//go:embed openapi.yaml
var files embed.FS

// Spec is the OpenAPI document.
var Spec = func() []byte {
	b, err := files.ReadFile("openapi.yaml")
	if err != nil {
		// Unreachable: the file is embedded at build time, so a failure here means
		// the binary was built without it, which must not start.
		panic("packages/api: embedded openapi.yaml is missing: " + err.Error())
	}
	return b
}()

// SpecETag is the strong ETag of the document, computed once at startup.
var SpecETag = func() string {
	sum := sha256.Sum256(Spec)
	return `"` + hex.EncodeToString(sum[:16]) + `"`
}()

// SpecPath is where the document is served.
const SpecPath = "/openapi.yaml"

// Handler serves the specification.
//
// Unauthenticated deliberately: a client cannot construct a valid request without
// the contract, and the contract describes the shape of the API rather than any
// tenant's data. It is the same reasoning that makes the probe endpoints
// unauthenticated.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		w.Header().Set("ETag", SpecETag)
		// The spec changes only when the binary does, so a long cache is safe as long
		// as the ETag is honoured.
		w.Header().Set("Cache-Control", "public, max-age=300")

		if match := r.Header.Get("If-None-Match"); match != "" && etagMatches(match, SpecETag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		http.ServeContent(w, r, "openapi.yaml", buildTime(), strings.NewReader(string(Spec)))
	})
}

// etagMatches implements the If-None-Match comparison for the one tag served here,
// including the wildcard and weak-comparison forms.
func etagMatches(header, tag string) bool {
	header = strings.TrimSpace(header)
	if header == "*" {
		return true
	}
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		candidate = strings.TrimPrefix(candidate, "W/")
		if candidate == tag {
			return true
		}
	}
	return false
}

// buildTime is deliberately the zero time: the spec's identity is its ETag, and a
// fabricated modification time would let a caller draw conclusions about the build
// that are not true.
func buildTime() time.Time { return time.Time{} }
