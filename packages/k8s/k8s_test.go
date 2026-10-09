package k8s_test

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/adityasatwar321/nebula/packages/k8s"
)

func TestObjectName(t *testing.T) {
	t.Parallel()
	if got := k8s.ObjectName("acme", "qwen-prod"); got != "nebula-acme-qwen-prod" {
		t.Errorf("got %q", got)
	}
	long1 := k8s.ObjectName("a-very-long-organization-slug-for-testing", "a-very-long-deployment-name-one")
	long2 := k8s.ObjectName("a-very-long-organization-slug-for-testing", "a-very-long-deployment-name-two")
	if len(long1) > 63 || len(long2) > 63 || long1 == long2 {
		t.Errorf("long names must fit 63 chars and stay distinct: %q %q", long1, long2)
	}
	if strings.HasSuffix(k8s.RootName(long1), "--root") || len(k8s.RootName(long1)) > 63 {
		t.Errorf("root name %q", k8s.RootName(long1))
	}
	if got := k8s.ObjectName("Acme_Corp", "x"); got != "nebula-acme-corp-x" {
		t.Errorf("names must be DNS labels: %q", got)
	}
}

func TestQuantities(t *testing.T) {
	t.Parallel()
	if got := k8s.CPUMilli(resource.MustParse("2500m")); got != 2500 {
		t.Errorf("cpu %d", got)
	}
	if got := k8s.CPUMilli(resource.MustParse("4")); got != 4000 {
		t.Errorf("cpu %d", got)
	}
	if got := k8s.MemoryMiB(resource.MustParse("8Gi")); got != 8192 {
		t.Errorf("memory %d", got)
	}
	if got := k8s.MemoryMiB(resource.MustParse("1G")); got != 953 {
		t.Errorf("decimal memory must round down: %d", got)
	}
}
