// Package version exposes build information stamped in at link time and the
// API contract version the binary implements.
//
// Values are set with -ldflags at build time (see the Makefile). They default to
// "dev"/"unknown" so a `go run` build is obviously not a release artifact rather
// than silently claiming to be one.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
)

// Stamped at link time. Do not set these anywhere else.
var (
	// Version is the semantic version of the build, or "dev".
	Version = "dev"
	// Commit is the git SHA of the build, or "unknown".
	Commit = "unknown"
	// BuildTime is an RFC 3339 timestamp, or "unknown".
	BuildTime = "unknown"
)

// Contract is the major API contract version this binary speaks. It is a
// compile-time constant rather than a build flag: changing it is a code change
// with a matching OpenAPI change, never a build-time decision.
const Contract = "v1"

// Info is the full build identity of a running binary.
type Info struct {
	Service   string `json:"service"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
	Contract  string `json:"contract"`
	GoVersion string `json:"go_version"`
}

// Get returns the build identity for the named service.
func Get(service string) Info {
	commit := Commit
	if commit == "unknown" {
		// A `go build` without ldflags still records VCS data when available.
		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				if s.Key == "vcs.revision" && s.Value != "" {
					commit = s.Value
				}
			}
		}
	}
	return Info{
		Service:   service,
		Version:   Version,
		Commit:    commit,
		BuildTime: BuildTime,
		Contract:  Contract,
		GoVersion: runtime.Version(),
	}
}

// Short renders a one-line identity suitable for a startup log or a User-Agent.
func (i Info) Short() string {
	c := i.Commit
	if len(c) > 7 {
		c = c[:7]
	}
	return fmt.Sprintf("%s/%s (%s, contract %s)", i.Service, i.Version, c, i.Contract)
}

// IsRelease reports whether this looks like a stamped release build rather than
// a local one. Used to refuse development-only behaviour in production.
func (i Info) IsRelease() bool {
	return i.Version != "dev" && i.Version != "" && i.Commit != "unknown"
}

// ContractSkew compares a peer's advertised contract version with this binary's.
// It returns a human-readable warning and true when they differ.
//
// Used by the CLI against the server, and by services against each other, so a
// mismatched deploy is reported rather than producing confusing 400s.
func ContractSkew(peer string) (string, bool) {
	peer = strings.TrimSpace(peer)
	if peer == "" || peer == Contract {
		return "", false
	}
	return fmt.Sprintf("API contract mismatch: this build speaks %s, peer speaks %s", Contract, peer), true
}
