package api

import "net/http"

// DeploymentLifecycleForTest exposes the lifecycle handler to the package's external
// test, which needs to call it without the authentication middleware (and therefore
// without a database). Only in _test.go, so it is not part of the API surface.
func (a *API) DeploymentLifecycleForTest(w http.ResponseWriter, r *http.Request) error {
	return a.deploymentLifecycle(w, r)
}
