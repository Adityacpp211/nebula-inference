// Package k8s holds NEBULA's Kubernetes conventions: object names, labels and
// annotations, client construction, and quantity normalization.
//
// It is the one place those conventions are written down in code, so the
// controller that creates objects, the inventory that reads them and a future
// gateway EndpointSlice informer cannot disagree about what "a NEBULA worker" looks
// like (docs/deployment-architecture.md §2.2).
package k8s

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Labels on every object the controller creates.
const (
	LabelName         = "app.kubernetes.io/name"
	LabelManagedBy    = "app.kubernetes.io/managed-by"
	LabelComponent    = "app.kubernetes.io/component"
	LabelOrg          = "nebula.dev/org"
	LabelOrgID        = "nebula.dev/org-id"
	LabelDeployment   = "nebula.dev/deployment"
	LabelDeploymentID = "nebula.dev/deployment-id"
	LabelVersionID    = "nebula.dev/model-version-id"
	LabelRuntime      = "nebula.dev/runtime"
	LabelRevision     = "nebula.dev/revision"

	AnnotationSpecHash   = "nebula.dev/spec-hash"
	AnnotationGeneration = "nebula.dev/generation"

	// ManagedBy is the value of LabelManagedBy. The controller's "is this mine?"
	// check is this label, not a naming convention.
	ManagedBy = "nebula"
	// WorkerName is the LabelName of worker pods.
	WorkerName = "nebula-worker"
	// ComponentRoot marks the per-deployment root ConfigMap that owns the rest.
	ComponentRoot = "root"
	// FieldManager names the controller in server-side apply.
	FieldManager = "nebula-controller"
)

// ManagedSelector selects every object the controller manages.
func ManagedSelector() string { return LabelManagedBy + "=" + ManagedBy }

var dns1123 = regexp.MustCompile(`[^a-z0-9-]+`)

// ObjectName is the name of a deployment's Kubernetes objects:
// nebula-<org-slug>-<deployment-name>, within the 63-character DNS label limit. A
// name that would be longer is truncated and suffixed with a hash of the full name,
// so two long names that share a prefix never collide.
func ObjectName(orgSlug, deploymentName string) string {
	full := dns1123.ReplaceAllString(strings.ToLower("nebula-"+orgSlug+"-"+deploymentName), "-")
	full = strings.Trim(full, "-")
	if len(full) <= 63 {
		return full
	}
	sum := sha256.Sum256([]byte(full))
	return strings.TrimRight(full[:54], "-") + "-" + hex.EncodeToString(sum[:])[:8]
}

// RootName is the root ConfigMap's name.
func RootName(objectName string) string {
	if len(objectName) > 58 {
		objectName = objectName[:58]
	}
	return strings.TrimRight(objectName, "-") + "-root"
}

// RestConfig loads a kubeconfig path, or in-cluster configuration when empty.
func RestConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("in-cluster configuration: %w (set NEBULA_KUBECONFIG outside a cluster)", err)
		}
		return cfg, nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig %s: %w", kubeconfig, err)
	}
	return cfg, nil
}

// NewClient builds a clientset.
func NewClient(cfg *rest.Config, userAgent string) (kubernetes.Interface, error) {
	c := rest.CopyConfig(cfg)
	c.UserAgent = userAgent
	// The controller is one client; defaults of 5 QPS / 10 burst make a resync of a
	// few hundred deployments take minutes.
	c.QPS, c.Burst = 50, 100
	return kubernetes.NewForConfig(c)
}

// CPUMilli normalizes a CPU quantity to millicores.
func CPUMilli(q resource.Quantity) int64 { return q.MilliValue() }

// MemoryMiB normalizes a memory quantity to MiB, rounding down.
func MemoryMiB(q resource.Quantity) int64 { return q.Value() / (1 << 20) }
