package version_test

import (
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/version"
)

func TestGetIncludesIdentity(t *testing.T) {
	t.Parallel()

	info := version.Get("nebula-controlplane")
	if info.Service != "nebula-controlplane" {
		t.Errorf("Service = %q, want nebula-controlplane", info.Service)
	}
	if info.Contract != version.Contract {
		t.Errorf("Contract = %q, want %q", info.Contract, version.Contract)
	}
	if info.GoVersion == "" {
		t.Error("GoVersion is empty")
	}
	if info.Version == "" {
		t.Error("Version is empty; it should default to dev rather than being blank")
	}
}

func TestShortIsReadable(t *testing.T) {
	t.Parallel()

	info := version.Info{
		Service: "nebula-gateway", Version: "1.4.2",
		Commit: "a1b2c3d4e5f6a7b8", Contract: "v1",
	}
	got := info.Short()
	for _, want := range []string{"nebula-gateway", "1.4.2", "a1b2c3d", "v1"} {
		if !strings.Contains(got, want) {
			t.Errorf("Short() = %q, want it to contain %q", got, want)
		}
	}
	// The commit is abbreviated, not printed in full.
	if strings.Contains(got, "a1b2c3d4e5f6a7b8") {
		t.Errorf("Short() = %q, want an abbreviated commit", got)
	}
}

func TestShortHandlesShortCommit(t *testing.T) {
	t.Parallel()

	info := version.Info{Service: "s", Version: "v", Commit: "abc", Contract: "v1"}
	if got := info.Short(); !strings.Contains(got, "abc") {
		t.Errorf("Short() = %q, want it to contain the short commit unchanged", got)
	}
}

// A development build must be distinguishable from a release, so production can
// refuse development-only behaviour.
func TestIsRelease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		info version.Info
		want bool
	}{
		{"stamped release", version.Info{Version: "1.0.0", Commit: "abc123"}, true},
		{"unstamped local build", version.Info{Version: "dev", Commit: "unknown"}, false},
		{"version without commit", version.Info{Version: "1.0.0", Commit: "unknown"}, false},
		{"commit without version", version.Info{Version: "dev", Commit: "abc123"}, false},
		{"empty", version.Info{}, false},
	}
	for _, tt := range tests {
		if got := tt.info.IsRelease(); got != tt.want {
			t.Errorf("%s: IsRelease() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// Contract skew must be reported rather than producing confusing 400s later.
func TestContractSkew(t *testing.T) {
	t.Parallel()

	if msg, skewed := version.ContractSkew(version.Contract); skewed {
		t.Errorf("matching contract reported as skew: %q", msg)
	}
	if msg, skewed := version.ContractSkew(""); skewed {
		t.Errorf("empty peer contract reported as skew: %q", msg)
	}
	msg, skewed := version.ContractSkew("v2")
	if !skewed {
		t.Fatal("v2 against v1 was not reported as skew")
	}
	if !strings.Contains(msg, "v2") || !strings.Contains(msg, version.Contract) {
		t.Errorf("skew message names neither version: %q", msg)
	}
	// Surrounding whitespace, as an HTTP header may carry, must not matter.
	if _, skewed := version.ContractSkew("  " + version.Contract + "  "); skewed {
		t.Error("whitespace around a matching contract was reported as skew")
	}
}
