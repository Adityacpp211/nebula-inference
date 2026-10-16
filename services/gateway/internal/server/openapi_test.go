package server_test

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	specpkg "github.com/adityasatwar321/nebula/packages/api"
	"github.com/adityasatwar321/nebula/services/gateway/internal/server"
)

// The gateway's half of the drift check: every operation the gateway serves is
// documented with x-nebula-served-by: gateway, and every operation documented
// that way is served. The control plane's test checks the rest of the file.
func TestSpecMatchesGatewayRoutes(t *testing.T) {
	t.Parallel()
	var s struct {
		Paths map[string]map[string]yaml.Node `yaml:"paths"`
	}
	if err := yaml.Unmarshal(specpkg.Spec, &s); err != nil {
		t.Fatal(err)
	}

	documented := map[string]bool{}
	for path, ops := range s.Paths {
		for method, node := range ops {
			var op struct {
				ServedBy string `yaml:"x-nebula-served-by"`
			}
			if err := node.Decode(&op); err != nil || op.ServedBy != "gateway" {
				continue
			}
			documented[strings.ToUpper(method)+" "+path] = true
		}
	}
	mounted := map[string]bool{}
	for _, r := range server.Routes() {
		mounted[r.Method+" "+r.Path] = true
	}
	for op := range mounted {
		if !documented[op] {
			t.Errorf("%s is served by the gateway but not documented with x-nebula-served-by: gateway", op)
		}
	}
	for op := range documented {
		if !mounted[op] {
			t.Errorf("%s is documented as gateway-served but the gateway does not serve it", op)
		}
	}
}
